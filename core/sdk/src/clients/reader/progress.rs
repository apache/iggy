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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use dashmap::DashMap;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    AutoCommit, Checkpoint, Client, CommitEvery, CommitPolicy, Consumer, ConsumerOffsetClient,
    Identifier, IggyError, PolledMessages, PollingKind, PollingStrategy, ReaderStamp,
};
use tokio::sync::Notify;
use tracing::error;

use super::ORDERING;
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::driver::operation_deadline;

/// Reader ids start at 1, so no reader shares an id within the process.
static NEXT_READER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
enum OffsetMutation {
    Store { partition_id: u32, offset: u64 },
    StorePosition(Checkpoint),
    Delete { partition_id: Option<u32> },
    DeletePosition(Checkpoint),
}

/// What a reader delivered, counted and committed, its blocked partitions and the generations
/// of its partitions. The fetch loop, the reader handle and `IggyConsumer` share it.
pub(crate) struct Progress {
    client: IggyRwLock<ClientWrapper>,
    consumer: Arc<Consumer>,
    stream: Arc<Identifier>,
    topic: Arc<Identifier>,
    reader: u64,
    /// Set for `CommitPolicy::Auto`.
    auto: Option<AutoCommit>,
    /// The last message that the application took from each partition.
    delivered: DashMap<u32, Checkpoint>,
    /// With `CommitPolicy::Auto`: the latest counted message of each partition.
    counted: DashMap<u32, Counted>,
    /// Counted positions of partitions that a rebalance takes away, stored before the counted
    /// ones of the next flush.
    retiring: Mutex<Vec<Checkpoint>>,
    /// Wakes the store task of the reader when a count makes a store due.
    due: Notify,
    /// The offset that this reader believes the server holds for each partition.
    committed: DashMap<u32, Checkpoint>,
    /// The operation of an unknown outcome that blocks the progress of a partition until the
    /// caller abandons it.
    blocked: DashMap<u32, u64>,
    /// With `OnRead`: the first offset of a committed section that did not decode.
    undecoded: DashMap<u32, Checkpoint>,
    /// One lock, so a seek and its generation appear together.
    generations: Mutex<Generations>,
    /// Bumped under the lock before each change of a generation.
    generation_changes: AtomicU64,
}

#[derive(Clone, Copy)]
struct Counted {
    checkpoint: Checkpoint,
    /// The messages counted since the last store of the partition.
    since_store: u32,
}

#[derive(Default)]
struct Generations {
    last: u64,
    current: BTreeMap<u32, u64>,
    seeks: BTreeMap<u32, PollingStrategy>,
    /// The generation that the last seek of each partition started.
    sought: BTreeMap<u32, u64>,
}

impl Progress {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        commit: CommitPolicy,
    ) -> Self {
        Self {
            client,
            consumer,
            stream,
            topic,
            reader: NEXT_READER.fetch_add(1, Ordering::Relaxed),
            auto: match commit {
                CommitPolicy::Auto(auto) => Some(auto),
                CommitPolicy::Manual | CommitPolicy::OnRead => None,
            },
            delivered: DashMap::new(),
            counted: DashMap::new(),
            retiring: Mutex::new(Vec::new()),
            due: Notify::new(),
            committed: DashMap::new(),
            blocked: DashMap::new(),
            undecoded: DashMap::new(),
            generations: Mutex::new(Generations::default()),
            generation_changes: AtomicU64::new(0),
        }
    }

    pub(crate) fn stamp(&self, generation: u64) -> ReaderStamp {
        ReaderStamp {
            reader: self.reader,
            generation,
        }
    }

    pub(crate) fn generation(&self, partition: u32) -> Option<u64> {
        self.generations().current.get(&partition).copied()
    }

    /// Whether a read of `generation` still counts. With `OnRead`, the server stored the offset
    /// of a read, so a read of a revoked or reassigned partition still counts. Only a seek drops
    /// what was read before it.
    pub(crate) fn keeps(&self, partition: u32, generation: u64, on_read: bool) -> bool {
        let generations = self.generations();
        if generations.current.get(&partition) == Some(&generation) {
            return true;
        }
        on_read
            && generations
                .sought
                .get(&partition)
                .is_none_or(|sought| generation >= *sought)
    }

    /// Rises before any answer of [`Self::keeps`] changes, so a caller that sees the same count
    /// can keep its earlier answer.
    pub(crate) fn generation_changes(&self) -> u64 {
        self.generation_changes.load(ORDERING)
    }

    /// Starts a new generation for each partition of `assigned` outside `staying`, and forgets
    /// the other partitions with their pending seeks. Returns the generations of `assigned`.
    /// Both lists are sorted.
    pub(crate) fn assign(&self, assigned: &[u32], staying: &[u32]) -> BTreeMap<u32, u64> {
        let mut guard = self.generations_mut();
        let generations = &mut *guard;
        generations
            .current
            .retain(|partition, _| assigned.binary_search(partition).is_ok());
        generations
            .seeks
            .retain(|partition, _| staying.binary_search(partition).is_ok());
        for partition in assigned {
            if staying.binary_search(partition).is_err()
                || !generations.current.contains_key(partition)
            {
                generations.last += 1;
                generations.current.insert(*partition, generations.last);
            }
        }
        generations.current.clone()
    }

    /// Starts a new generation of `partition` that reads from `start`, and returns it.
    pub(crate) fn seek(&self, partition: u32, start: PollingStrategy) -> u64 {
        let mut generations = self.generations_mut();
        generations.last += 1;
        let generation = generations.last;
        generations.current.insert(partition, generation);
        generations.seeks.insert(partition, start);
        generations.sought.insert(partition, generation);
        generation
    }

    /// With `OnRead`: the application stored the offset of `stored`, so the reader reads the
    /// partition on after it in a new generation, and the close keeps that offset as the last
    /// delivered message. A partition that the reader does not read only keeps the store.
    pub(crate) fn read_after(&self, stored: Checkpoint) {
        let partition = stored.partition_id();
        let Some(next) = stored.offset().checked_add(1) else {
            return;
        };
        if self.generation(partition).is_none() {
            return;
        }
        let context = stored.context();
        let start = if context == PartitionContext::default() {
            PollingStrategy::offset(next)
        } else {
            PollingStrategy::continuation(partition, PollingKind::Offset, next, context)
        };
        self.resume_after(stored, start);
    }

    /// With `OnRead`: the server holds the offset of `stored`, so the reader reads its partition
    /// from `start` in a new generation, and `stored` is the last delivered message of that
    /// generation and its commit record, which the close keeps.
    pub(crate) fn resume_after(&self, stored: Checkpoint, start: PollingStrategy) {
        let partition = stored.partition_id();
        let generation = self.seek(partition, start);
        let stored = Checkpoint::stamped(
            partition,
            stored.offset(),
            stored.context(),
            Some(self.stamp(generation)),
        );
        self.committed.insert(partition, stored);
        self.deliver(&stored);
    }

    /// The seeks that the fetch loop did not apply yet, each with its generation.
    pub(crate) fn take_seeks(&self) -> Vec<(u32, PollingStrategy, u64)> {
        let mut guard = self.generations();
        let generations = &mut *guard;
        std::mem::take(&mut generations.seeks)
            .into_iter()
            .filter_map(|(partition, start)| {
                let generation = generations.current.get(&partition).copied()?;
                Some((partition, start, generation))
            })
            .collect()
    }

    pub(crate) fn deliver(&self, checkpoint: &Checkpoint) {
        self.delivered
            .insert(checkpoint.partition_id(), *checkpoint);
    }

    pub(crate) fn last_delivered(&self, partition: u32) -> Option<Checkpoint> {
        self.delivered.get(&partition).map(|entry| *entry.value())
    }

    /// Counts `messages` up to `checkpoint` under `CommitPolicy::Auto`, and wakes the store task
    /// when its event makes a store due. A count only moves forward within a generation, and a
    /// newer generation replaces it. `ends_batch` marks the last message of a batch.
    pub(crate) fn count(&self, checkpoint: Checkpoint, messages: u32, ends_batch: bool) {
        let Some(auto) = self.auto else {
            return;
        };
        let generation = |checkpoint: &Checkpoint| checkpoint.stamp().map(|stamp| stamp.generation);
        let since_store = {
            let mut counted = self
                .counted
                .entry(checkpoint.partition_id())
                .or_insert(Counted {
                    checkpoint,
                    since_store: 0,
                });
            let previous = counted.checkpoint;
            if generation(&checkpoint) > generation(&previous) {
                counted.checkpoint = checkpoint;
                counted.since_store = 0;
            } else if generation(&checkpoint) == generation(&previous)
                && (!same_incarnation_and_owner(previous.context(), checkpoint.context())
                    || checkpoint.offset() > previous.offset())
            {
                counted.checkpoint = checkpoint;
            } else if generation(&checkpoint) < generation(&previous) {
                return;
            }
            counted.since_store = counted.since_store.saturating_add(messages);
            counted.since_store
        };
        let due = match auto.every {
            None => false,
            Some(CommitEvery::Message) => true,
            Some(CommitEvery::Messages(every)) => since_store >= every.get(),
            Some(CommitEvery::Batch) => ends_batch,
        };
        if due {
            self.due.notify_one();
        }
    }

    /// Counts a checkpoint that the application processed, under `CommitPolicy::Auto`.
    pub(crate) fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        if self.auto.is_none()
            || checkpoint
                .stamp()
                .is_none_or(|stamp| stamp.reader != self.reader)
        {
            return Err(IggyError::InvalidCommand);
        }
        self.count(checkpoint, 1, false);
        Ok(())
    }

    /// The latest counted message of `partition`.
    #[cfg(test)]
    pub(crate) fn last_counted(&self, partition: u32) -> Option<Checkpoint> {
        self.counted
            .get(&partition)
            .map(|counted| counted.checkpoint)
    }

    /// Resolves once a count made a store due since the last call.
    pub(crate) async fn store_due(&self) {
        self.due.notified().await;
    }

    /// Takes the counted positions of `partitions` out of the record, so the next flush stores
    /// them before a rebalance starts their new generations.
    pub(crate) fn retire_counted(&self, partitions: &[u32]) {
        let retired: Vec<Checkpoint> = partitions
            .iter()
            .filter_map(|partition| self.counted.remove(partition))
            .map(|(_, counted)| counted.checkpoint)
            .filter(|checkpoint| self.is_current(checkpoint))
            .collect();
        if retired.is_empty() {
            return;
        }
        self.retiring_guard().extend(retired);
        self.due.notify_one();
    }

    /// Stores each counted position that is ahead of the last store of its generation, the
    /// retired ones first. A position of an older generation is dropped. Every partition is
    /// tried, and the first failure is the error.
    pub(crate) async fn flush_counted(&self) -> Result<(), IggyError> {
        let mut failure = None;
        let retiring = std::mem::take(&mut *self.retiring_guard());
        for checkpoint in retiring {
            if self.commit_covers(&checkpoint) {
                continue;
            }
            if let Err(error) = self.store(checkpoint).await {
                error!(
                    "Failed to store the counted offset of a partition that a rebalance took away: {checkpoint:?}. {error}"
                );
                failure.get_or_insert(error);
            }
        }
        self.counted
            .retain(|_, counted| self.is_current(&counted.checkpoint));
        let counted: Vec<Counted> = self.counted.iter().map(|entry| *entry.value()).collect();
        for Counted {
            checkpoint,
            since_store,
        } in counted
        {
            if self.commit_covers(&checkpoint) {
                continue;
            }
            match self.store(checkpoint).await {
                Ok(()) => {
                    if let Some(mut counted) = self.counted.get_mut(&checkpoint.partition_id()) {
                        counted.since_store = counted.since_store.saturating_sub(since_store);
                    }
                }
                Err(error) => {
                    error!("Failed to store the counted offset: {checkpoint:?}. {error}");
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// The last delivered message of `partition` that a replay filter skips up to. A seek reads
    /// again on purpose, so a message delivered before the last seek of its partition is none.
    pub(crate) fn replay_floor(&self, partition: u32) -> Option<Checkpoint> {
        let delivered = self.last_delivered(partition)?;
        let sought = self.generations().sought.get(&partition).copied();
        sought
            .is_none_or(|sought| {
                delivered
                    .stamp()
                    .is_some_and(|stamp| stamp.generation >= sought)
            })
            .then_some(delivered)
    }

    pub(crate) fn last_committed(&self, partition: u32) -> Option<Checkpoint> {
        self.committed.get(&partition).map(|entry| *entry.value())
    }

    /// With `OnRead`, the server stored the offset of a batch that the reader hands over.
    pub(crate) fn record_commit(&self, checkpoint: Checkpoint) {
        self.committed.insert(checkpoint.partition_id(), checkpoint);
    }

    /// Stores the offset of a checkpoint that this reader delivered. A checkpoint of an older
    /// generation, or at or below the last commit of its generation, stores nothing.
    pub(crate) async fn commit(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        let Some(stamp) = checkpoint
            .stamp()
            .filter(|stamp| stamp.reader == self.reader)
        else {
            return Err(IggyError::InvalidCommand);
        };
        if self.generation(checkpoint.partition_id()) != Some(stamp.generation)
            || self.commit_covers(&checkpoint)
        {
            return Ok(());
        }
        self.store(checkpoint).await
    }

    pub(crate) fn block(&self, partition: u32, operation: u64) {
        self.blocked.insert(partition, operation);
    }

    pub(crate) fn blocked(&self, partition: u32) -> Option<u64> {
        self.blocked.get(&partition).map(|entry| *entry.value())
    }

    pub(crate) fn blocks(&self) -> Vec<(u32, u64)> {
        // The fetch loop asks on every turn, and `DashMap::iter` allocates for each shard, also
        // for an empty map.
        if self.blocked.is_empty() {
            return Vec::new();
        }
        self.blocked
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Lifts the block of `partition` only while `operation` still holds it.
    pub(crate) fn unblock(&self, partition: u32, operation: u64) {
        self.blocked
            .remove_if(&partition, |_, blocking| *blocking == operation);
    }

    /// Lifts each block whose operation the client no longer retains, as after
    /// `abandon_operation`, which wakes no reader.
    pub(crate) async fn unblock_abandoned(&self) {
        let blocks = self.blocks();
        if blocks.is_empty() {
            return;
        }
        let client = self.client.read().await;
        for (partition, operation) in blocks {
            if !client.retains_operation(operation).await {
                self.unblock(partition, operation);
            }
        }
    }

    /// Keeps the lowest first offset of the undecoded sections of one history.
    pub(crate) fn remember_undecoded(&self, start: Checkpoint) {
        self.undecoded
            .entry(start.partition_id())
            .and_modify(|previous| {
                if !same_incarnation_and_owner(previous.context(), start.context())
                    || start.offset() < previous.offset()
                {
                    *previous = start;
                }
            })
            .or_insert(start);
    }

    #[cfg(test)]
    pub(crate) fn undecoded(&self, partition: u32) -> Option<Checkpoint> {
        self.undecoded.get(&partition).map(|start| *start)
    }

    pub(crate) fn forget_undecoded(&self, partition: u32) {
        self.undecoded.remove(&partition);
    }

    /// Forgets an undecoded start of `partition` in another history than `context`.
    pub(crate) fn forget_undecoded_history(&self, partition: u32, context: PartitionContext) {
        self.undecoded.remove_if(&partition, |_, start| {
            !same_incarnation_and_owner(start.context(), context)
        });
    }

    /// A delivered batch covers the undecoded start of its partition, or replaces its history.
    pub(crate) fn forget_undecoded_covered(&self, batch: &PolledMessages) {
        self.undecoded.remove_if(&batch.partition_id, |_, start| {
            !same_incarnation_and_owner(start.context(), batch.context())
                || batch
                    .messages
                    .first()
                    .zip(batch.messages.last())
                    .is_some_and(|(first, last)| {
                        first.header.offset <= start.offset()
                            && last.header.offset >= start.offset()
                    })
        });
    }

    pub(crate) fn retain_undecoded(&self, partitions: &[u32]) {
        self.undecoded
            .retain(|partition, _| partitions.binary_search(partition).is_ok());
    }

    /// The `OnRead` close: the server stored the offsets of `undelivered`, so the offset of
    /// each partition moves back to its last delivered message. A partition that delivered
    /// nothing in the history of a batch moves before that batch when the reader resumes at
    /// `Next`. `owned` limits a group member to the partitions that it still owns. Every
    /// partition is tried, and the first failure is the error.
    pub(crate) async fn rewind(
        &self,
        undelivered: Vec<PolledMessages>,
        owned: Option<&[u32]>,
        resumes_next: bool,
    ) -> Result<(), IggyError> {
        let mut failure = None;
        let mut rewound = BTreeSet::new();
        for polled in undelivered {
            let partition_id = polled.partition_id;
            if self.blocked.contains_key(&partition_id)
                || owned.is_some_and(|owned| !owned.contains(&partition_id))
            {
                continue;
            }
            let (Some(first), Some(last)) = (polled.checkpoint_at(0), polled.checkpoint()) else {
                continue;
            };
            // The server refuses the final store of a message delivered in
            // another history, so that store cannot rewind this batch.
            if self.last_delivered(partition_id).is_some_and(|delivered| {
                same_incarnation_and_owner(delivered.context(), polled.context())
            }) {
                self.committed.insert(partition_id, last);
                continue;
            }
            // The batches of one history arrive in offset order, so the first
            // one holds the oldest undelivered message of that history.
            let history = (
                partition_id,
                polled.context().incarnation,
                polled.context().owner_generation,
            );
            if !resumes_next || !rewound.insert(history) {
                continue;
            }
            if let Err(error) = self.rewind_before(first).await {
                error!(
                    "Failed to rewind the offset before undelivered messages in partition: {partition_id}. {error}"
                );
                failure.get_or_insert(error);
            }
        }
        if resumes_next {
            let undecoded: Vec<_> = self.undecoded.iter().map(|entry| *entry.value()).collect();
            for start in undecoded {
                let partition_id = start.partition_id();
                if self.blocked.contains_key(&partition_id)
                    || owned.is_some_and(|owned| !owned.contains(&partition_id))
                    || self.last_delivered(partition_id).is_some_and(|delivered| {
                        same_incarnation_and_owner(delivered.context(), start.context())
                    })
                    || self.last_committed(partition_id).is_some_and(|committed| {
                        same_incarnation_and_owner(committed.context(), start.context())
                            && committed.offset() >= start.offset()
                    })
                {
                    continue;
                }
                match self.rewind_before(start).await {
                    Ok(()) => self.forget_undecoded(partition_id),
                    Err(error) => {
                        error!(?start, %error, "Failed to rewind an undecoded committed fetch section");
                        failure.get_or_insert(error);
                    }
                }
            }
        }
        for checkpoint in self.unstored_delivered(owned) {
            let partition_id = checkpoint.partition_id();
            // A rewind under a newer history replaced the commit record. The server refuses a
            // position of the older history, which means nothing in the newer one.
            if self.blocked.contains_key(&partition_id)
                || self.last_committed(partition_id).is_some_and(|committed| {
                    !same_incarnation_and_owner(committed.context(), checkpoint.context())
                })
            {
                continue;
            }
            if let Err(error) = self.store(checkpoint).await {
                error!("Failed to store delivered position: {checkpoint:?}. {error}");
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Stores the offset of `checkpoint` without the generation rule, so it can move the
    /// server offset back.
    pub(crate) async fn store(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.mutate_offset(OffsetMutation::StorePosition(checkpoint))
            .await?;
        self.committed.insert(checkpoint.partition_id(), checkpoint);
        self.undecoded
            .remove_if(&checkpoint.partition_id(), |_, start| {
                same_incarnation_and_owner(start.context(), checkpoint.context())
                    && checkpoint.offset() >= start.offset()
            });
        Ok(())
    }

    /// Stores `offset` without a context. The record proves no coverage in any history, so a
    /// later commit of the same position still goes out.
    pub(crate) async fn store_offset(
        &self,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.mutate_offset(OffsetMutation::Store {
            partition_id,
            offset,
        })
        .await?;
        self.committed.insert(
            partition_id,
            Checkpoint::new(partition_id, offset, PartitionContext::default()),
        );
        Ok(())
    }

    /// Deletes the stored offset of `partition_id`. For `None` the server resolves the
    /// partition, so every commit record goes.
    pub(crate) async fn delete_offset(&self, partition_id: Option<u32>) -> Result<(), IggyError> {
        // Cancellation can hide a committed delete, so a later store must reach the server.
        match partition_id {
            Some(partition_id) => {
                self.committed.remove(&partition_id);
            }
            None => self.committed.clear(),
        }
        self.mutate_offset(OffsetMutation::Delete { partition_id })
            .await
    }

    fn is_current(&self, checkpoint: &Checkpoint) -> bool {
        checkpoint.stamp().is_some_and(|stamp| {
            self.generation(checkpoint.partition_id()) == Some(stamp.generation)
        })
    }

    fn retiring_guard(&self) -> MutexGuard<'_, Vec<Checkpoint>> {
        self.retiring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn generations(&self) -> MutexGuard<'_, Generations> {
        self.generations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A caller that sees the new count then waits for this lock, so it never keeps an answer
    /// that the change made stale.
    fn generations_mut(&self) -> MutexGuard<'_, Generations> {
        let generations = self.generations();
        self.generation_changes.fetch_add(1, ORDERING);
        generations
    }

    fn commit_covers(&self, checkpoint: &Checkpoint) -> bool {
        self.last_committed(checkpoint.partition_id())
            .is_some_and(|committed| {
                committed.stamp() == checkpoint.stamp()
                    && same_incarnation_and_owner(committed.context(), checkpoint.context())
                    && checkpoint.offset() <= committed.offset()
            })
    }

    async fn rewind_before(&self, start: Checkpoint) -> Result<(), IggyError> {
        match start.offset().checked_sub(1) {
            Some(offset) => {
                let before = Checkpoint::new(start.partition_id(), offset, start.context());
                self.store(before).await
            }
            None => {
                self.committed.remove(&start.partition_id());
                self.mutate_offset(OffsetMutation::DeletePosition(start))
                    .await
            }
        }
    }

    /// The last delivered message of each partition where it differs from the commit record.
    /// `owned` limits a group member to the partitions that it owns.
    pub(crate) fn unstored_delivered(&self, owned: Option<&[u32]>) -> Vec<Checkpoint> {
        self.delivered
            .iter()
            .map(|entry| *entry.value())
            .filter(|delivered| {
                owned.is_none_or(|owned| owned.contains(&delivered.partition_id()))
                    && self
                        .last_committed(delivered.partition_id())
                        .is_none_or(|committed| committed != *delivered)
            })
            .collect()
    }

    async fn mutate_offset(&self, mutation: OffsetMutation) -> Result<(), IggyError> {
        crate::clients::with_client!(self, client => {
            let deadline = operation_deadline();
            match self.send_offset_mutation(&client, mutation).await {
                Err(IggyError::PartitionLaneBlocked(partition, operation)) => {
                    self.block(partition, operation);
                    Err(IggyError::PartitionLaneBlocked(partition, operation))
                }
                Err(IggyError::LaneBlocked(op)) => {
                    if matches!(
                        client.resolve_operation(op, deadline.into_std()).await,
                        Err(IggyError::OperationUncertain(_))
                    ) {
                        return Err(IggyError::LaneBlocked(op));
                    }
                    // The blocker has its own outcome. Re-entering the lane either
                    // admits this new mutation or reports that it remains blocked.
                    self.send_offset_mutation(&client, mutation).await
                }
                result => result,
            }
        })
    }

    async fn send_offset_mutation(
        &self,
        client: &ClientWrapper,
        mutation: OffsetMutation,
    ) -> Result<(), IggyError> {
        match mutation {
            OffsetMutation::Store {
                partition_id,
                offset,
            } => {
                client
                    .store_consumer_offset(
                        &self.consumer,
                        &self.stream,
                        &self.topic,
                        Some(partition_id),
                        offset,
                    )
                    .await
            }
            OffsetMutation::Delete { partition_id } => {
                client
                    .delete_consumer_offset(&self.consumer, &self.stream, &self.topic, partition_id)
                    .await
            }
            OffsetMutation::StorePosition(checkpoint) => {
                client
                    .store_consumer_position(&self.consumer, &self.stream, &self.topic, &checkpoint)
                    .await
            }
            OffsetMutation::DeletePosition(checkpoint) => {
                client
                    .delete_consumer_position(
                        &self.consumer,
                        &self.stream,
                        &self.topic,
                        &checkpoint,
                    )
                    .await
            }
        }
    }
}

/// The server can store the offsets of a committing read whose reply never reaches the
/// application. Dropping this forgets the commit record of each partition that it holds, where
/// the record equals the delivered message, so the `OnRead` close stores that message again,
/// which moves the server offset back to it.
pub(crate) struct PendingCommit {
    partitions: Vec<u32>,
    progress: Arc<Progress>,
}

impl PendingCommit {
    pub(crate) fn new(progress: Arc<Progress>, partitions: Vec<u32>) -> Self {
        Self {
            partitions,
            progress,
        }
    }

    /// Every committed batch reaches the application, except the ones of `lost`.
    pub(crate) fn settle(mut self, lost: Vec<u32>) {
        self.partitions = lost;
    }
}

impl Drop for PendingCommit {
    fn drop(&mut self) {
        for partition in &self.partitions {
            let delivered = self.progress.last_delivered(*partition);
            self.progress
                .committed
                .remove_if(partition, |_, committed| delivered == Some(*committed));
        }
    }
}

/// Offsets are comparable only within one partition incarnation and owner. The metadata frontier
/// is left out: another group's ownership change can advance it between two polls of the same
/// partition.
pub(crate) fn same_incarnation_and_owner(left: PartitionContext, right: PartitionContext) -> bool {
    left.incarnation == right.incarnation && left.owner_generation == right.owner_generation
}
