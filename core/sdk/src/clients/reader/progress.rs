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
    Checkpoint, Client, Consumer, ConsumerOffsetClient, Identifier, IggyError, PolledMessages,
    PollingStrategy, ReaderStamp,
};
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

/// What a reader delivered and committed, its blocked partitions and the generations of its
/// partitions. The fetch loop, the reader handle and `IggyConsumer` share it.
pub(crate) struct Progress {
    client: IggyRwLock<ClientWrapper>,
    consumer: Arc<Consumer>,
    stream: Arc<Identifier>,
    topic: Arc<Identifier>,
    reader: u64,
    /// The last message that the application took from each partition.
    delivered: DashMap<u32, Checkpoint>,
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

#[derive(Default)]
struct Generations {
    last: u64,
    current: BTreeMap<u32, u64>,
    seeks: BTreeMap<u32, PollingStrategy>,
}

impl Progress {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
    ) -> Self {
        Self {
            client,
            consumer,
            stream,
            topic,
            reader: NEXT_READER.fetch_add(1, Ordering::Relaxed),
            delivered: DashMap::new(),
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
    /// of a read, so a read of a revoked partition still counts.
    pub(crate) fn keeps(&self, partition: u32, generation: u64, on_read: bool) -> bool {
        match self.generations().current.get(&partition) {
            Some(current) => *current == generation,
            None => on_read,
        }
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

    /// Starts a new generation of `partition` that reads from `start`.
    pub(crate) fn seek(&self, partition: u32, start: PollingStrategy) {
        let mut generations = self.generations_mut();
        generations.last += 1;
        let generation = generations.last;
        generations.current.insert(partition, generation);
        generations.seeks.insert(partition, start);
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
