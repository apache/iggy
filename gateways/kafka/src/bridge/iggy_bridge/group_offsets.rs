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

use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::stream::{self, Stream, StreamExt};
use iggy::prelude::{
    Client, Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient, IggyError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{debug, error, info, warn};

use super::group_index::Scanned;
use super::{
    IggyBridge, KafkaTopicMetadata, LazyClient, OffsetGroups, REQUEST_TIMEOUT, SLOT_LIMIT,
    ScannedGroups, TopicTarget, permit_by, resolve_blocker, write_with_recovery,
};
use crate::bridge::error::BridgeError;
use crate::group::is_valid_group_id;
use crate::protocol::api::{ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_UNKNOWN_TOPIC_OR_PARTITION};

/// Prefix of the Iggy consumer group that holds a Kafka group's offsets. It keeps a Kafka group
/// apart from a native Iggy group of the same name.
pub const OFFSET_GROUP_PREFIX: &str = "kafka.cg.";

/// How long one scan for the groups that hold offsets may take. It keeps running after the request
/// that started it stops waiting, on a client of its own, so no offset call waits behind it. A scan
/// that reaches it keeps the groups it found.
const GROUP_SCAN_LIMIT: Duration = Duration::from_secs(30);

/// Topics whose Iggy groups a listing asks for at once. The SDK sends them on one connection.
const LISTING_CALLS: usize = 16;

/// The first pause, and the longest, between two checks of whether a topic still lists a group
/// that Iggy deleted.
const DELETE_CHECK_FIRST: Duration = Duration::from_millis(5);
const DELETE_CHECK_MAX: Duration = Duration::from_millis(100);

/// The first pause, and the longest, between two tries to settle a write that ended in doubt.
const SETTLE_PAUSE_FIRST: Duration = Duration::from_millis(50);
const SETTLE_PAUSE_MAX: Duration = Duration::from_secs(1);

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
    /// Kept by each write until no Iggy call of it can land. See [`Self::keeping`].
    kept: Option<Arc<dyn Any + Send + Sync>>,
}

impl OffsetCalls {
    /// Calls that give up at `deadline`. A call that has not started by then never starts.
    #[must_use]
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            queued: std::array::from_fn(|_| Arc::new(Semaphore::new(1))),
            kept: None,
        }
    }

    /// These calls, with `kept` held by each commit and each group deletion until no Iggy call of
    /// it can land. A call given up on at the deadline goes on and can still land, and so can one
    /// that ended in doubt. So the coordinator's count of the commit or the deletion goes here.
    #[must_use]
    pub fn keeping(mut self, kept: impl Any + Send + Sync) -> Self {
        self.kept = Some(Arc::new(kept));
        self
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
    /// to [`Self::offset_groups`] at once. Each topic that a commit stored to, or may have, counts
    /// for [`Self::delete_offset_groups`] once no commit of the request can land, with the numeric
    /// id of the Iggy group when a commit created it.
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
        let storing = commits
            .iter()
            .any(|&(_, _, offset)| offset >= 0)
            .then(|| Arc::new(self.group_index.storing(group)));
        // `join_all` polls each call once, in order, before any again, so calls on one slot
        // queue for it in order.
        let results = join_all(commits.iter().map(|&(topic, partition, offset)| {
            let slot = slot_of(topic, partition);
            let (names, topic) = (Arc::clone(&names), topic.clone());
            let storing = if offset >= 0 { storing.clone() } else { None };
            let held = storing.clone();
            let commit = move |client: Arc<IggyClient>| async move {
                let mut created = None;
                let written = commit_on(&client, &names, &topic, partition, offset, |id| {
                    created = Some(id);
                })
                .await;
                if let Some(storing) = storing
                    && may_have_stored(&written, created)
                {
                    storing.stored(&topic, created);
                }
                written
            };
            self.commit_in_slot(slot, calls, commit, held)
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
    /// Lists the topics, then the Iggy groups of each topic, on the scan client, so the listing
    /// neither waits behind `OffsetFetch` nor costs it its client. That costs one call per topic,
    /// and at least two per deletion. The scan client can read a replica that missed a recent
    /// commit, so the topics that commits through this gateway stored to lately come too, with
    /// the numeric id of the Iggy group when a commit created it. Each deletion runs on the commit
    /// lane of partition 0 of its topic (see `delete_offset_group_on`), and keeps what `calls`
    /// keep until it can no longer land, also past their deadline. A topic deleted since the
    /// listing holds none. A group whose deletions all ended leaves [`Self::offset_groups`] at
    /// once.
    ///
    /// A deletion can end after its request answered. The retry of that request then finds no
    /// topic that holds the group, and still counts it as held, once, for up to a minute.
    ///
    /// # Errors
    ///
    /// The outer error holds for every group, and nothing was deleted: the listing failed, or did
    /// not end by the deadline of `calls`, or [`IggyError::Unauthorized`] when the bridge user may
    /// not list the groups of a Kafka topic, or may not read an override topic, which the listing
    /// then leaves out. A group can hold offsets there. Per group, as
    /// [`Self::commit_group_offsets`]: a deletion that does not end by the deadline of `calls` is
    /// [`BridgeError::Timeout`], and can still land.
    pub async fn delete_offset_groups(
        &self,
        groups: &[&str],
        calls: &OffsetCalls,
    ) -> Result<Vec<Result<bool, BridgeError>>, BridgeError> {
        let deletions = timeout_at(calls.deadline, self.offset_group_topics(groups))
            .await
            .map_err(|_elapsed| BridgeError::Timeout)??;
        let deleted = join_all(deletions.iter().map(|deletion| {
            self.delete_offset_group_on(groups[deletion.at], &deletion.topic, deletion.id, calls)
        }))
        .await;
        let mut results: Vec<Result<bool, BridgeError>> =
            groups.iter().map(|_| Ok(false)).collect();
        for (deletion, deleted) in deletions.iter().zip(deleted) {
            let result = &mut results[deletion.at];
            match deleted {
                Ok(found) => {
                    if let Ok(held) = result {
                        *held |= found;
                    }
                }
                Err(error) => *result = Err(error),
            }
        }
        for (group, result) in groups.iter().zip(&mut results) {
            if let Ok(held) = result {
                // Taken also when this request found the group, so no later request answers for
                // this deletion.
                let deleted = self.group_index.deleted().take(group);
                *held |= deleted;
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

    /// The Kafka groups that hold offsets in Iggy, from the last scan, plus the groups that this
    /// gateway stored offsets for since, less those it deleted. The groups of a scan answer at once
    /// for 10 s after it ends. After that a caller starts a new scan, and still gets the older
    /// groups at once for up to 1 min after their scan ended. Otherwise the caller waits for the
    /// new scan, until `deadline`.
    ///
    /// A scan lists the Kafka topics, then the Iggy groups of each topic, so it costs one call per
    /// topic. It runs on a client of its own, a few topics at once, so no offset call waits behind
    /// it. A scan keeps running after its caller stops waiting, for up to 30 s, and serves the next
    /// caller.
    pub async fn offset_groups(self: &Arc<Self>, deadline: Instant) -> ScannedGroups {
        let bridge = Arc::clone(self);
        let scan = move || async move { bridge.scan_offset_groups().await };
        self.group_index.groups(deadline, scan).await
    }

    /// The groups of the last scan, if one found them, without a scan.
    #[must_use]
    pub fn last_offset_groups(&self) -> Option<Arc<OffsetGroups>> {
        self.group_index.last()
    }

    /// Every Kafka group with offsets on a Kafka topic. A topic deleted since the listing holds
    /// none. A topic whose groups the bridge user may not list is left out, so that one such
    /// topic does not hide the groups of every other. A scan that reaches `GROUP_SCAN_LIMIT` keeps
    /// what it found.
    async fn scan_offset_groups(&self) -> Result<Scanned, BridgeError> {
        let deadline = Instant::now() + GROUP_SCAN_LIMIT;
        let topics = async {
            let client = self.scan_client.connected(&self.config).await?;
            let topics = self.list_kafka_topics_on(&client).await?;
            Ok::<_, BridgeError>((client, topics))
        };
        let (client, topics) = timeout_at(deadline, topics)
            .await
            .map_err(|_elapsed| BridgeError::Timeout)??;
        let mut groups = BTreeSet::new();
        let mut listings = pin!(self.kafka_groups_by_topic(&client, &topics));
        loop {
            let (topic, listed) = match timeout_at(deadline, listings.next()).await {
                Ok(Some(listing)) => listing,
                Ok(None) => {
                    return Ok(Scanned {
                        groups,
                        whole: true,
                    });
                }
                Err(_elapsed) => {
                    warn!(
                        topics = topics.len(),
                        "the scan for the Kafka groups that hold offsets did not list every topic \
                         in {GROUP_SCAN_LIMIT:?}; keeping the groups it found"
                    );
                    return Ok(Scanned {
                        groups,
                        whole: false,
                    });
                }
            };
            match listed {
                Ok((_, found)) => groups.extend(found.into_iter().map(|listed| listed.group)),
                Err(error)
                    if topic_deleted(&error)
                        || matches!(error, BridgeError::Iggy(IggyError::Unauthorized)) =>
                {
                    debug!(topic, %error, "skipping a topic in the scan for groups");
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Each Iggy group that holds offsets of a group of `groups`: those that the scan client
    /// lists, then those on the topics that commits through this gateway stored to lately.
    async fn offset_group_topics(
        &self,
        groups: &[&str],
    ) -> Result<Vec<GroupToDelete>, BridgeError> {
        let client = self.scan_client.connected(&self.config).await?;
        let listing = self.kafka_topic_listing_on(&client).await?;
        let mut unexamined = listing.unreadable;
        // The handler answers a repeated group id once, so each id has one index.
        let requested: HashMap<&str, usize> = groups
            .iter()
            .enumerate()
            .map(|(at, group)| (*group, at))
            .collect();
        let mut deletions = Vec::new();
        let mut listings = pin!(self.kafka_groups_by_topic(&client, &listing.topics));
        while let Some((topic, listed)) = listings.next().await {
            let (target, found) = match listed {
                Ok(listed) => listed,
                Err(error) if topic_deleted(&error) => continue,
                Err(BridgeError::Iggy(IggyError::Unauthorized)) => {
                    unexamined.push(topic.to_owned());
                    continue;
                }
                Err(error) => return Err(error),
            };
            for listed in found {
                if let Some(&at) = requested.get(listed.group.as_str()) {
                    deletions.push(GroupToDelete {
                        at,
                        topic: target.clone(),
                        id: OffsetGroupId::Listed(listed.id),
                    });
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
        let stored = groups
            .iter()
            .enumerate()
            .flat_map(|(at, group)| {
                self.group_index
                    .stored_topics(group)
                    .into_iter()
                    .map(move |(topic, created)| GroupToDelete {
                        at,
                        topic,
                        id: created.map_or(OffsetGroupId::Unknown, OffsetGroupId::Created),
                    })
            })
            .collect();
        Ok(with_stored(deletions, stored))
    }

    /// The Kafka groups with offsets on each topic of `topics`, listed on `client`,
    /// `LISTING_CALLS` topics at once. Each item is a Kafka topic, and its Iggy topic with the
    /// listing, in the order the listings end.
    fn kafka_groups_by_topic<'a>(
        &'a self,
        client: &'a IggyClient,
        topics: &'a [KafkaTopicMetadata],
    ) -> impl Stream<Item = TopicListing<'a>> + 'a {
        stream::iter(topics)
            .map(move |topic| async move {
                let listed = match self.topic_target(&topic.kafka_topic) {
                    Ok(target) => kafka_groups_on(client, &target)
                        .await
                        .map(|found| (target, found)),
                    Err(error) => Err(error),
                };
                (topic.kafka_topic.as_str(), listed)
            })
            .buffer_unordered(LISTING_CALLS)
    }

    /// Deletes the Iggy group of Kafka group `group` on `topic`, on the commit lane of partition 0
    /// of the topic, so the delete follows the commits of that partition. `false` when the topic
    /// holds no such group.
    ///
    /// Deletes by numeric id (see `numeric_id`). So a copy of the delete that lands late cannot
    /// hit a newer group of the same name. Iggy never reuses a group id within a topic, but a
    /// topic deleted and created again numbers its groups from the start. So where the id names
    /// another group, the group of this id went with the older topic.
    ///
    /// The lane frees once Iggy answers the delete. The rest runs off it, and keeps what `calls`
    /// keep, also past their deadline: a delete that ended in doubt settles first (see
    /// `group_deleted`). Iggy then deletes the group once every partition of the topic dropped it,
    /// and in a cluster another replica can list it a little longer. A commit could still land in
    /// it until then. So the gateway waits until the topic no longer lists the id. It checks on
    /// the client that deleted the group, which reads what it wrote.
    async fn delete_offset_group_on(
        &self,
        group: &str,
        topic: &TopicTarget,
        id: OffsetGroupId,
        calls: &OffsetCalls,
    ) -> Result<bool, BridgeError> {
        let names = Arc::new(OffsetNames::new(group).map_err(BridgeError::Iggy)?);
        let slot = slot_of(topic, 0);
        let topic = Arc::new(topic.clone());
        let delete = {
            let (topic, names) = (Arc::clone(&topic), Arc::clone(&names));
            move |client: Arc<IggyClient>| async move {
                let Some(id) = numeric_id(&client, &topic, &names, id).await? else {
                    return Ok(None);
                };
                let id = Identifier::numeric(id).map_err(BridgeError::Iggy)?;
                let reply = delete_group(&client, &topic, &id).await;
                Ok(Some(SentDelete { id, reply }))
            }
        };
        let (deleted, group) = (self.group_index.deleted().clone(), group.to_owned());
        self.write_in_slot(
            slot,
            calls,
            delete,
            move |client, sent, answer| async move {
                let gone = group_gone(&client, &topic, &names.group, sent).await;
                if matches!(gone, Ok(true)) {
                    deleted.record(&group);
                }
                let _ = answer.send(gone);
            },
        )
        .await
    }

    /// [`Self::write_in_slot`] for a commit, which answers once it ends. A commit that ended in
    /// doubt keeps what `calls` keep after that, and `held`, until a replay proves its outcome or
    /// the SDK forgets it. The SDK forgets a write once a replay proved it, or once a new session
    /// replaced the one that sent it, which can then no longer apply it. `held` goes first.
    async fn commit_in_slot<F>(
        &self,
        slot: usize,
        calls: &OffsetCalls,
        commit: impl FnOnce(Arc<IggyClient>) -> F,
        held: impl Send + 'static,
    ) -> Result<(), BridgeError>
    where
        F: Future<Output = Result<(), BridgeError>> + Send + 'static,
    {
        self.write_in_slot(slot, calls, commit, |client, written, answer| async move {
            let in_doubt = match &written {
                Err(BridgeError::Iggy(IggyError::OperationUncertain(operation))) => {
                    Some(*operation)
                }
                _ => None,
            };
            let _ = answer.send(written);
            if let Some(operation) = in_doubt {
                let proved = replayed(&client, operation).await;
                debug!(
                    operation,
                    ?proved,
                    "settled an offset write that ended in doubt"
                );
            }
            drop(held);
        })
        .await
    }

    /// Runs `write` with the commit client of `slot`, then `then` with that client, what `write`
    /// returned, and the sender of the answer, which the caller waits for until the deadline of
    /// `calls`. A write that has not started by then never starts.
    ///
    /// Both go on in a task of their own after the caller stops waiting, and keep what `calls`
    /// keep until `then` ends. The lane frees when `write` ends, or once it ran for `SLOT_LIMIT`:
    /// the SDK cannot take back a request it sent, so the write then goes on off the lane.
    async fn write_in_slot<T, U, W, A>(
        &self,
        slot: usize,
        calls: &OffsetCalls,
        write: impl FnOnce(Arc<IggyClient>) -> W,
        then: impl FnOnce(Arc<IggyClient>, Result<T, BridgeError>, Answer<U>) -> A + Send + 'static,
    ) -> Result<U, BridgeError>
    where
        T: Send + 'static,
        U: Send + 'static,
        W: Future<Output = Result<T, BridgeError>> + Send + 'static,
        A: Future<Output = ()> + Send + 'static,
    {
        let lane = &self.offset_pool.slots[slot].commits;
        let Some((_queued, turn)) = calls.take(slot, lane).await else {
            return Err(BridgeError::Timeout);
        };
        let client = match timeout_at(calls.deadline, lane.client.connected(&self.config)).await {
            Ok(connected) => connected?,
            Err(_elapsed) => return Err(BridgeError::Timeout),
        };
        let (write, kept) = (write(Arc::clone(&client)), calls.kept.clone());
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            // `write` and `then` go before `kept`, with what they hold. A deletion that waits for
            // `kept` must find the topics that a commit stored to (`Storing`).
            {
                let mut write = pin!(write);
                let in_time = timeout(SLOT_LIMIT, &mut write).await;
                drop(turn);
                let written = match in_time {
                    Ok(written) => written,
                    Err(_elapsed) => write.await,
                };
                then(client, written, sender).await;
            }
            drop(kept);
        });
        timeout_at(calls.deadline, receiver)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(Err(BridgeError::Timeout))
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
        lane.client.until(&self.config, calls.deadline, call).await
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

/// A Kafka topic, and its Iggy topic with the Kafka groups that hold offsets there.
type TopicListing<'a> = (
    &'a str,
    Result<(TopicTarget, Vec<ListedGroup>), BridgeError>,
);

/// A Kafka group with offsets on a topic.
struct ListedGroup {
    group: String,
    /// The numeric id of the Iggy group that holds them.
    id: u32,
}

/// One Iggy group for `IggyBridge::delete_offset_groups` to delete.
struct GroupToDelete {
    /// The index of its Kafka group in the request.
    at: usize,
    topic: TopicTarget,
    id: OffsetGroupId,
}

/// Where the numeric id of an Iggy group to delete comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OffsetGroupId {
    /// The listing of the request found it.
    Listed(u32),
    /// A commit through this gateway created the group with it, up to a minute before. The topic
    /// can have been deleted and created again since.
    Created(u32),
    /// The client that deletes the group looks it up by name.
    Unknown,
}

/// The delete of one Iggy group by its numeric id, and Iggy's reply.
struct SentDelete {
    id: Identifier,
    reply: Result<(), IggyError>,
}

/// Where a write on a commit lane sends its answer. The caller may have stopped waiting.
type Answer<U> = oneshot::Sender<Result<U, BridgeError>>;

/// The Kafka groups with offsets on `topic`, listed on `client`.
async fn kafka_groups_on(
    client: &IggyClient,
    topic: &TopicTarget,
) -> Result<Vec<ListedGroup>, BridgeError> {
    let groups = client
        .get_consumer_groups(&topic.stream_id, &topic.topic_id)
        .await
        .map_err(BridgeError::Iggy)?;
    Ok(groups
        .into_iter()
        .filter_map(|group| {
            kafka_group_of(&group.name).map(|name| ListedGroup {
                group: name,
                id: group.id,
            })
        })
        .collect())
}

/// `listed`, the Iggy groups that a listing found, and each of `stored` that it missed. A stored
/// topic without an id counts as found when the listing found the Kafka group on it.
fn with_stored(mut listed: Vec<GroupToDelete>, stored: Vec<GroupToDelete>) -> Vec<GroupToDelete> {
    let missed: Vec<GroupToDelete> = {
        let ids: HashSet<(usize, &TopicTarget, u32)> = listed
            .iter()
            .filter_map(|deletion| match deletion.id {
                OffsetGroupId::Listed(id) | OffsetGroupId::Created(id) => {
                    Some((deletion.at, &deletion.topic, id))
                }
                OffsetGroupId::Unknown => None,
            })
            .collect();
        let topics: HashSet<(usize, &TopicTarget)> = listed
            .iter()
            .map(|deletion| (deletion.at, &deletion.topic))
            .collect();
        stored
            .into_iter()
            .filter(|deletion| match deletion.id {
                OffsetGroupId::Listed(id) | OffsetGroupId::Created(id) => {
                    !ids.contains(&(deletion.at, &deletion.topic, id))
                }
                OffsetGroupId::Unknown => !topics.contains(&(deletion.at, &deletion.topic)),
            })
            .collect()
    };
    listed.extend(missed);
    listed
}

/// The numeric id of the Iggy group of `names` on `topic`, as `id` gives it or as `client` finds
/// it. `None` when the topic holds no such group. A created id that now names another group is
/// from an older topic of the same name, so the group is looked up by name instead.
async fn numeric_id(
    client: &IggyClient,
    topic: &TopicTarget,
    names: &OffsetNames,
    id: OffsetGroupId,
) -> Result<Option<u32>, BridgeError> {
    match id {
        OffsetGroupId::Listed(id) => Ok(Some(id)),
        OffsetGroupId::Created(id) => {
            let numeric = Identifier::numeric(id).map_err(BridgeError::Iggy)?;
            match offset_group_name(client, topic, &numeric).await? {
                Some(name) if name != names.group => {
                    offset_group_id(client, topic, &names.consumer.id).await
                }
                _ => Ok(Some(id)),
            }
        }
        OffsetGroupId::Unknown => offset_group_id(client, topic, &names.consumer.id).await,
    }
}

/// The name of the Iggy group `id` on `topic`, read on `client`. `None` when the topic, or the
/// group, is not there.
async fn offset_group_name(
    client: &IggyClient,
    topic: &TopicTarget,
    id: &Identifier,
) -> Result<Option<String>, BridgeError> {
    match client
        .get_consumer_group(&topic.stream_id, &topic.topic_id, id)
        .await
        .map_err(BridgeError::Iggy)
    {
        Ok(found) => Ok(found.map(|group| group.name)),
        Err(BridgeError::Iggy(IggyError::ConsumerGroupIdNotFound(..))) => Ok(None),
        Err(error) if topic_deleted(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// The numeric id of the Iggy group `name` on `topic`, looked up on `client`. `None` when the
/// topic, or the group, is not there.
async fn offset_group_id(
    client: &IggyClient,
    topic: &TopicTarget,
    name: &Identifier,
) -> Result<Option<u32>, BridgeError> {
    match client
        .get_consumer_group(&topic.stream_id, &topic.topic_id, name)
        .await
        .map_err(BridgeError::Iggy)
    {
        Ok(found) => Ok(found.map(|group| group.id)),
        Err(error) if topic_deleted(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// One delete of the Iggy group `id` on `topic`. An earlier write in doubt on the client's lane
/// replays first. `LaneBlocked` when it stays in doubt: then the delete was not sent.
///
/// Not [`write_with_recovery`], which then returns the earlier write's `OperationUncertain`. A
/// replay of it would prove that write, not this delete.
async fn delete_group(
    client: &IggyClient,
    topic: &TopicTarget,
    id: &Identifier,
) -> Result<(), IggyError> {
    let deadline = (Instant::now() + SLOT_LIMIT).into_std();
    loop {
        match client
            .delete_consumer_group(&topic.stream_id, &topic.topic_id, id)
            .await
        {
            Err(IggyError::LaneBlocked(blocker)) => {
                if resolve_blocker(client, blocker, deadline).await.is_err() {
                    return Err(IggyError::LaneBlocked(blocker));
                }
            }
            reply => return reply,
        }
    }
}

/// Whether `topic` held the Iggy group `name` of a delete that `sent` holds. Once it did, this
/// waits until the delete can no longer land and the topic no longer lists the group.
async fn group_gone(
    client: &IggyClient,
    topic: &TopicTarget,
    name: &str,
    sent: Result<Option<SentDelete>, BridgeError>,
) -> Result<bool, BridgeError> {
    let Some(SentDelete { id, reply }) = sent? else {
        return Ok(false);
    };
    group_deleted(client, topic, &id, name, reply).await?;
    until_unlisted(client, topic, &id, name).await?;
    Ok(true)
}

/// Waits until the delete of the Iggy group `id`, `name`, which Iggy answered with `reply`, can
/// no longer land. `Ok` once Iggy took it, or the group or its topic is gone.
///
/// A delete that ended in doubt replays until Iggy proves its outcome. When the SDK no longer
/// holds it, the outcome stays unknown, so the group is deleted again until Iggy answers.
async fn group_deleted(
    client: &IggyClient,
    topic: &TopicTarget,
    id: &Identifier,
    name: &str,
    reply: Result<(), IggyError>,
) -> Result<(), BridgeError> {
    let reply = match reply {
        Err(IggyError::OperationUncertain(operation)) => match replayed(client, operation).await {
            Err(IggyError::OperationNotRetained(_)) => deleted_again(client, topic, id, name).await,
            proved => proved,
        },
        reply => reply,
    };
    match reply {
        Ok(()) | Err(IggyError::RequestAlreadyApplied | IggyError::ConsumerGroupIdNotFound(..)) => {
            Ok(())
        }
        Err(error) => match BridgeError::Iggy(error) {
            error if topic_deleted(&error) => Ok(()),
            error => Err(error),
        },
    }
}

/// Deletes the Iggy group `id`, `name`, until Iggy answers. A failure that a retry can fix goes
/// on: the first delete, which Iggy may still run, could be what Iggy refuses it for. Once `id`
/// names another group, this one went with an older topic of the same name.
async fn deleted_again(
    client: &IggyClient,
    topic: &TopicTarget,
    id: &Identifier,
    name: &str,
) -> Result<(), IggyError> {
    let mut pause = SETTLE_PAUSE_FIRST;
    loop {
        if matches!(offset_group_name(client, topic, id).await, Ok(Some(other)) if other != name) {
            return Ok(());
        }
        let answer = match delete_group(client, topic, id).await {
            Err(IggyError::OperationUncertain(operation)) => replayed(client, operation).await,
            answer => answer,
        };
        let again = match &answer {
            Ok(())
            | Err(IggyError::RequestAlreadyApplied | IggyError::ConsumerGroupIdNotFound(..)) => {
                false
            }
            Err(IggyError::OperationNotRetained(_)) => true,
            Err(error) => retries(error),
        };
        if !again {
            return answer;
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(SETTLE_PAUSE_MAX);
    }
}

/// Waits until `topic` no longer lists the Iggy group `id`, `name`, checking on `client`. A topic
/// that is gone lists no group, and an `id` of another name belongs to a newer topic of the same
/// name. A failure that a retry can fix waits on: a commit could land in the group until it is
/// gone.
async fn until_unlisted(
    client: &IggyClient,
    topic: &TopicTarget,
    id: &Identifier,
    name: &str,
) -> Result<(), BridgeError> {
    let mut pause = DELETE_CHECK_FIRST;
    loop {
        match client
            .get_consumer_group(&topic.stream_id, &topic.topic_id, id)
            .await
        {
            Ok(Some(group)) if group.name == name => {}
            Ok(_) | Err(IggyError::ConsumerGroupIdNotFound(..)) => return Ok(()),
            Err(error) if retries(&error) => {}
            Err(error) => {
                let error = BridgeError::Iggy(error);
                return if topic_deleted(&error) {
                    Ok(())
                } else {
                    Err(error)
                };
            }
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(DELETE_CHECK_MAX);
    }
}

/// Replays `operation`, a write that ended in doubt, until Iggy proves its outcome: `Ok`, or the
/// error that Iggy committed for it. Each replay is the same request, so a copy that lands late
/// changes nothing. `OperationNotRetained` once the SDK no longer holds it.
async fn replayed(client: &IggyClient, operation: u64) -> Result<(), IggyError> {
    let mut pause = SETTLE_PAUSE_FIRST;
    loop {
        let deadline = (Instant::now() + REQUEST_TIMEOUT).into_std();
        match client.resolve_operation(operation, deadline).await {
            Err(IggyError::OperationUncertain(_)) => {}
            proved => return proved,
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(SETTLE_PAUSE_MAX);
    }
}

/// Whether a later try can fix `error`. Nothing can, on a client that shut down.
fn retries(error: &IggyError) -> bool {
    !matches!(error, IggyError::ClientShutdown)
        && BridgeError::Iggy(error.clone()).to_offset_error_code()
            == ERROR_COORDINATOR_LOAD_IN_PROGRESS
}

/// The Kafka group whose offsets the Iggy group `name` holds. `None` for any other Iggy group.
fn kafka_group_of(name: &str) -> Option<String> {
    name.strip_prefix(OFFSET_GROUP_PREFIX)
        .filter(|group| is_valid_group_id(group))
        .map(str::to_owned)
}

/// One commit on `client`. A store that finds no Iggy group creates it, passes its numeric id to
/// `created`, and stores again.
async fn commit_on(
    client: &IggyClient,
    names: &OffsetNames,
    topic: &TopicTarget,
    partition: u32,
    offset: i64,
    created: impl FnOnce(u32),
) -> Result<(), BridgeError> {
    let deadline = (Instant::now() + SLOT_LIMIT).into_std();
    let (consumer, stream_id, topic_id) = (&names.consumer, &topic.stream_id, &topic.topic_id);
    let Ok(offset) = u64::try_from(offset) else {
        return match write_with_recovery(client, deadline, || {
            client.delete_consumer_offset(consumer, stream_id, topic_id, Some(partition))
        })
        .await
        {
            Err(
                IggyError::ConsumerOffsetNotFound(_) | IggyError::ConsumerGroupNameNotFound(..),
            ) => Ok(()),
            deleted => landed(deleted),
        };
    };
    let store = || {
        write_with_recovery(client, deadline, || {
            client.store_consumer_offset(consumer, stream_id, topic_id, Some(partition), offset)
        })
    };
    match store().await {
        Err(IggyError::ConsumerGroupNameNotFound(..)) => {
            if let Some(id) = create_offset_group(client, stream_id, topic_id, &names.group).await?
            {
                created(id);
            }
            landed(store().await)
        }
        stored => landed(stored),
    }
}

/// Whether a commit that ended with `written` can have stored an offset, or created the Iggy
/// group: it did, it ended in doubt, or `created` holds the id of the group it created. Only such
/// a commit counts for `DeleteGroups`, so a commit that Iggy refused costs no memory there.
const fn may_have_stored(written: &Result<(), BridgeError>, created: Option<u32>) -> bool {
    created.is_some()
        || matches!(
            written,
            Ok(()) | Err(BridgeError::Iggy(IggyError::OperationUncertain(_)))
        )
}

/// A write whose replay Iggy refuses because the first try landed is done.
fn landed(result: Result<(), IggyError>) -> Result<(), BridgeError> {
    match result {
        Ok(()) | Err(IggyError::RequestAlreadyApplied) => Ok(()),
        Err(error) => Err(BridgeError::Iggy(error)),
    }
}

/// Creates the Iggy group `name` on the topic. Its numeric id when this call created it. A group
/// that exists counts as created, since another commit or another gateway can create it first.
async fn create_offset_group(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    name: &str,
) -> Result<Option<u32>, BridgeError> {
    let deadline = (Instant::now() + SLOT_LIMIT).into_std();
    match write_with_recovery(client, deadline, || {
        client.create_consumer_group(stream_id, topic_id, name)
    })
    .await
    {
        Ok(group) => {
            // `{name:?}`: a client picks the group id, and a newline in it would forge a line.
            info!("created Iggy consumer group {name:?} for Kafka group offsets");
            Ok(Some(group.id))
        }
        Err(IggyError::ConsumerGroupNameAlreadyExists(..) | IggyError::RequestAlreadyApplied) => {
            Ok(None)
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
            .commit_in_slot(
                slot,
                &OffsetCalls::new(soon()),
                |_client| async move {
                    let _ = released.await;
                    Ok(())
                },
                (),
            )
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
            .commit_in_slot(
                0,
                &OffsetCalls::new(soon()),
                |_client| async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                },
                (),
            )
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
            .commit_in_slot(
                0,
                &OffsetCalls::new(later()),
                |_client| async { Ok(()) },
                (),
            )
            .await;
        assert!(after.is_ok(), "the slot frees when the stuck commit ends");
    }

    /// A commit given up on at the deadline can still land. So what its calls keep, the count of a
    /// running commit or deletion, must last until the commit ends, not until its caller stops
    /// waiting.
    #[tokio::test]
    async fn given_a_commit_given_up_on_should_keep_what_its_calls_keep_until_it_ends() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let kept = Arc::new(());
        let calls = OffsetCalls::new(soon()).keeping(Arc::clone(&kept));
        let (release, released) = oneshot::channel::<()>();

        let commit = bridge
            .commit_in_slot(
                0,
                &calls,
                |_client| async move {
                    let _ = released.await;
                    Ok(())
                },
                (),
            )
            .await;
        drop(calls);

        assert!(
            matches!(commit, Err(BridgeError::Timeout)),
            "the caller gives up"
        );
        assert_eq!(Arc::strong_count(&kept), 2, "the commit still runs");
        release
            .send(())
            .expect("the stuck commit waits for its release");
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&kept) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the commit lets go of it once it ends");
    }

    /// What follows a write runs off the lane, so the next write goes ahead. What the calls keep
    /// stays kept until it ends.
    #[tokio::test]
    async fn given_a_write_that_ended_should_free_the_lane_while_what_follows_it_runs() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let kept = Arc::new(());
        let calls = OffsetCalls::new(soon()).keeping(Arc::clone(&kept));
        let (release, released) = oneshot::channel::<()>();
        let then = |_client: Arc<IggyClient>,
                    written: Result<(), BridgeError>,
                    answer: Answer<()>| async move {
            let _ = released.await;
            let _ = answer.send(written);
        };

        let ended = bridge
            .write_in_slot(0, &calls, |_client| async { Ok(()) }, then)
            .await;
        drop(calls);
        assert!(
            matches!(ended, Err(BridgeError::Timeout)),
            "the caller gives up"
        );

        let next = bridge
            .commit_in_slot(
                0,
                &OffsetCalls::new(later()),
                |_client| async { Ok(()) },
                (),
            )
            .await;
        assert!(next.is_ok(), "the lane freed when the write ended");
        assert_eq!(Arc::strong_count(&kept), 2, "what follows still runs");
        release
            .send(())
            .expect("what follows waits for its release");
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&kept) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("it lets go once it ends");
    }

    /// The SDK cannot take back a sent request. A write past `SLOT_LIMIT` frees the lane, and goes
    /// on with what its calls keep.
    #[tokio::test(start_paused = true)]
    async fn given_a_write_past_the_slot_limit_should_free_the_lane_and_keep_what_its_calls_keep() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let kept = Arc::new(());
        let calls = OffsetCalls::new(soon()).keeping(Arc::clone(&kept));
        let (release, released) = oneshot::channel::<()>();

        let stuck = bridge
            .commit_in_slot(
                0,
                &calls,
                |_client| async move {
                    let _ = released.await;
                    Ok(())
                },
                (),
            )
            .await;
        drop(calls);
        assert!(
            matches!(stuck, Err(BridgeError::Timeout)),
            "the caller gives up"
        );

        tokio::time::sleep(SLOT_LIMIT).await;
        let next = bridge
            .commit_in_slot(
                0,
                &OffsetCalls::new(later()),
                |_client| async { Ok(()) },
                (),
            )
            .await;
        assert!(next.is_ok(), "the lane frees at the limit");
        assert_eq!(Arc::strong_count(&kept), 2, "the write still runs");
        release.send(()).expect("the write waits for its release");
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&kept) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the write lets go of it once it ends");
    }

    /// A commit answers once it ends, also in doubt, and settles after that. The SDK of this
    /// client holds no such write, so it can no longer land, and what its calls keep goes at once.
    #[tokio::test]
    async fn given_a_commit_in_doubt_should_answer_at_once_and_settle_after() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let kept = Arc::new(());
        let calls = OffsetCalls::new(later()).keeping(Arc::clone(&kept));

        let written = bridge
            .commit_in_slot(
                0,
                &calls,
                |_client| async { Err(BridgeError::Iggy(IggyError::OperationUncertain(7))) },
                (),
            )
            .await;
        drop(calls);

        assert!(matches!(
            written,
            Err(BridgeError::Iggy(IggyError::OperationUncertain(7)))
        ));
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&kept) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("it settles at once");
    }

    /// Notes its name in the shared order when it drops.
    struct Dropped(&'static str, Arc<Mutex<Vec<&'static str>>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.1.lock().unwrap().push(self.0);
        }
    }

    /// A deletion waits until what the calls of a commit keep goes, then reads the topics that
    /// the commit stored to. So what the commit holds, its `Storing`, lasts as long as the commit,
    /// also after its caller gave up on it, and goes first.
    #[tokio::test]
    async fn given_a_commit_given_up_on_what_it_holds_should_go_before_what_its_calls_keep() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let order = Arc::new(Mutex::new(Vec::new()));
        let calls = OffsetCalls::new(soon()).keeping(Dropped("kept", Arc::clone(&order)));
        let in_write = Dropped("held by the write", Arc::clone(&order));
        let (release, released) = oneshot::channel::<()>();

        let commit = bridge
            .commit_in_slot(
                0,
                &calls,
                |_client| async move {
                    let _ = released.await;
                    let _ = &in_write;
                    Ok(())
                },
                Dropped("held", Arc::clone(&order)),
            )
            .await;
        drop(calls);
        assert!(
            matches!(commit, Err(BridgeError::Timeout)),
            "the caller gives up"
        );
        assert!(order.lock().unwrap().is_empty(), "the commit still runs");

        release.send(()).expect("the commit waits for its release");
        timeout(Duration::from_secs(1), async {
            while order.lock().unwrap().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("each goes once the commit ends");
        assert_eq!(order.lock().unwrap().last(), Some(&"kept"));
    }

    /// The listing can miss a group that a commit through this gateway just created. A stored
    /// topic that it missed is deleted too. A stored id that differs from the listed one is
    /// another Iggy group of the same name, and goes too.
    #[test]
    fn given_stored_topics_should_add_those_the_listing_missed() {
        use OffsetGroupId::{Created, Listed, Unknown};
        let deletion = |at, topic: &str, id| GroupToDelete {
            at,
            topic: target(topic),
            id,
        };
        let listed = vec![
            deletion(0, "orders", Listed(3)),
            deletion(1, "orders", Listed(4)),
            deletion(2, "orders", Listed(6)),
        ];
        let stored = vec![
            deletion(0, "orders", Unknown),
            deletion(0, "payments", Unknown),
            deletion(1, "orders", Created(4)),
            deletion(1, "audit", Created(9)),
            deletion(2, "orders", Created(5)),
        ];

        let deletions: Vec<(usize, TopicTarget, OffsetGroupId)> = with_stored(listed, stored)
            .into_iter()
            .map(|deletion| (deletion.at, deletion.topic, deletion.id))
            .collect();

        assert_eq!(
            deletions,
            [
                (0, target("orders"), Listed(3)),
                (1, target("orders"), Listed(4)),
                (2, target("orders"), Listed(6)),
                (0, target("payments"), Unknown),
                (1, target("audit"), Created(9)),
                (2, target("orders"), Created(5)),
            ]
        );
    }

    /// Only a commit that can have stored an offset counts for `DeleteGroups`, so commits that
    /// Iggy refuses cannot fill the gateway's memory.
    #[test]
    fn given_a_commit_iggy_refused_should_count_only_if_it_created_the_group() {
        let refused = Err(BridgeError::Iggy(IggyError::Unauthorized));
        let in_doubt = Err(BridgeError::Iggy(IggyError::OperationUncertain(7)));
        let blocked = Err(BridgeError::Iggy(IggyError::OperationNotRetained(7)));

        assert!(may_have_stored(&Ok(()), None));
        assert!(may_have_stored(&in_doubt, None));
        assert!(!may_have_stored(&refused, None));
        assert!(!may_have_stored(&blocked, None));
        assert!(
            may_have_stored(&refused, Some(3)),
            "the group it created is there"
        );
    }

    /// A commit that Iggy refuses stores nothing, so it adds no topic to delete on.
    #[tokio::test]
    async fn given_a_refused_commit_should_add_no_topic_to_delete_on() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let kept = Arc::new(());
        let calls = OffsetCalls::new(later()).keeping(Arc::clone(&kept));

        let results = bridge
            .commit_group_offsets("g", &[(&target("orders"), 0, 5)], &calls)
            .await;
        drop(calls);
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&kept) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the commit ends");

        assert!(matches!(results[0], Err(BridgeError::Iggy(_))));
        assert_eq!(bridge.group_index.stored_topics("g"), []);
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
            matches!(scanned, Ok(ScannedGroups::Failed)),
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
