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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use dashmap::DashMap;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    AutoCommit, Checkpoint, Client, CommitEvery, CommitPolicy, Consumer, ConsumerOffsetClient,
    Identifier, IggyError, PollOptions, PolledMessages, PollingKind, PollingStrategy, ReaderStamp,
    binary_deadline_in_force,
};
use tokio::sync::{Notify, oneshot, watch};
use tokio::time::{Instant, sleep_until};
use tracing::{error, warn};

use super::ORDERING;
use super::worker::transient_refusal;
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::driver::{operation_deadline, with_deadline};

/// Reader ids start at 1, so no reader shares an id within the process.
static NEXT_READER: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static CALLER: Caller;
}

/// The caller of a write that is not the application, which ends the retries of its read for
/// the owner. The application ends them by dropping the write, or with a deadline in force.
#[derive(Clone, Copy)]
enum Caller {
    /// The reader itself: a seek after its caller returned, and an `Auto` store. The close
    /// start ends the retries.
    Reader,
    /// The close, whose budget ends at this instant. Then no read for the owner starts, so
    /// each write of the close that did not go yet still goes, under its own request timeout.
    Close(Instant),
}

/// Runs `write` as a write of the reader itself, so the close ends its retries.
pub(crate) fn in_background<F: Future>(write: F) -> impl Future<Output = F::Output> {
    CALLER.scope(Caller::Reader, write)
}

/// Runs the writes of a close, so the end of `budget` ends the retries of their reads for the
/// owner.
pub(crate) fn in_close<F: Future>(budget: Instant, writes: F) -> impl Future<Output = F::Output> {
    CALLER.scope(Caller::Close(budget), writes)
}

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
    /// The owner generations that the reads of each partition named.
    owners: DashMap<u32, Owners>,
    /// One lock, so a seek and its generation appear together.
    generations: Mutex<Generations>,
    /// Bumped under the lock before each change of a generation.
    generation_changes: AtomicU64,
    /// Wakes a seek when the fetch loop answers the hold of its partition, when a hold ends, and
    /// when the fetch loop ends.
    quiet: Notify,
    /// Wakes the fetch loop of the reader.
    pub(super) wake: Arc<Notify>,
    /// Set by a write refused as not owned, so the fetch loop refreshes the assignment at once.
    refresh_requested: AtomicBool,
    /// The pause before a read again after a transient refusal.
    retry_interval: Duration,
    /// True once the reader closes or drops, which ends the retries of its own writes.
    stopping: watch::Sender<bool>,
}

#[derive(Clone, Copy)]
struct Counted {
    checkpoint: Checkpoint,
    /// The messages counted since the last store of the partition.
    since_store: u32,
}

/// The owners that the reads of a partition named. A session change installs the owner again
/// under a higher generation, so the owners up to `stale` belong to earlier sessions.
#[derive(Clone, Copy, Default)]
struct Owners {
    seen: u64,
    stale: u64,
}

#[derive(Default)]
struct Generations {
    last: u64,
    /// Rises at each session change.
    session: u64,
    current: BTreeMap<u32, u64>,
    /// The generation that started the current assignment of each partition. A partition that
    /// leaves and comes back starts a new assignment, so a lower generation belongs to an
    /// earlier one.
    joined: BTreeMap<u32, u64>,
    seeks: BTreeMap<u32, PollingStrategy>,
    /// The generation that the last seek of each partition started.
    sought: BTreeMap<u32, u64>,
    /// With `OnRead`: the partitions whose reads a seek holds while it writes their stored
    /// offset. True once the fetch loop holds them and no read of them that can commit is in
    /// flight.
    held: BTreeMap<u32, bool>,
    /// The held partitions whose position named no offset when the fetch loop answered the hold.
    unnamed: BTreeSet<u32>,
    /// The close waited for the seeks, so a later seek would write beside its rewind.
    closed: bool,
    /// A fetch loop runs, which answers the holds.
    reading: bool,
    lost_writes: Vec<LostWrite>,
    /// The stored offset from before the first seek write with an unknown outcome of each
    /// partition, `None` inside when the server held none. The close puts it back while no
    /// batch and no delivered message of the partition decide its rewind, and the partition did
    /// not leave the assignment since.
    restores: BTreeMap<u32, Option<u64>>,
}

/// A seek write with an unknown outcome that the fetch loop did not take yet.
pub(crate) struct LostWrite {
    pub(crate) partition: u32,
    /// 0 when the error names none, as over HTTP.
    pub(crate) operation: u64,
    /// The generation of the partition at the write.
    pub(crate) generation: u64,
    /// For a seek that looked it up: the stored offset from before the write, `None` inside
    /// when the server held none.
    pub(crate) before: Option<Option<u64>>,
}

impl Progress {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        commit: CommitPolicy,
        retry_interval: Duration,
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
            owners: DashMap::new(),
            generations: Mutex::new(Generations::default()),
            generation_changes: AtomicU64::new(0),
            quiet: Notify::new(),
            wake: Arc::new(Notify::new()),
            refresh_requested: AtomicBool::new(false),
            retry_interval,
            stopping: watch::Sender::new(false),
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
    /// the other partitions with their pending seeks and restore points. Returns the generations
    /// of `assigned`. Both lists are sorted.
    pub(crate) fn assign(&self, assigned: &[u32], staying: &[u32]) -> BTreeMap<u32, u64> {
        let mut guard = self.generations_mut();
        let generations = &mut *guard;
        generations
            .current
            .retain(|partition, _| assigned.binary_search(partition).is_ok());
        generations
            .joined
            .retain(|partition, _| assigned.binary_search(partition).is_ok());
        generations
            .seeks
            .retain(|partition, _| staying.binary_search(partition).is_ok());
        // Another member can have read a partition that left, so its return must not move the
        // offset back over that progress.
        generations
            .restores
            .retain(|partition, _| staying.binary_search(partition).is_ok());
        for partition in assigned {
            if staying.binary_search(partition).is_err()
                || !generations.current.contains_key(partition)
            {
                generations.last += 1;
                generations.current.insert(*partition, generations.last);
                generations.joined.insert(*partition, generations.last);
            }
        }
        generations.current.clone()
    }

    /// The generation that started the current assignment of `partition`, if `stamp` belongs to
    /// that assignment. No stamp counts as one of the current assignment.
    fn assignment(&self, partition: u32, stamp: Option<ReaderStamp>) -> Option<u64> {
        let joined = self.generations().joined.get(&partition).copied()?;
        stamp
            .is_none_or(|stamp| stamp.generation >= joined)
            .then_some(joined)
    }

    /// Asks the fetch loop to refresh the assignment at once.
    fn request_refresh(&self) {
        self.refresh_requested.store(true, ORDERING);
        self.wake.notify_one();
    }

    /// Whether a write asked for a refresh of the assignment since the last call.
    pub(crate) fn take_refresh(&self) -> bool {
        self.refresh_requested.swap(false, ORDERING)
    }

    /// Ends the retries of the writes that run in the background, so the close does not wait
    /// for them without end. Their refusal stands.
    pub(crate) fn stop_background(&self) {
        self.stopping.send_replace(true);
    }

    /// Resolves once the caller of the write ends the retries of its read for the owner.
    async fn cancelled(&self) {
        match CALLER.try_with(|caller| *caller) {
            Ok(Caller::Reader) => {
                let mut stopping = self.stopping.subscribe();
                // The sender lives as long as the progress.
                let _ = stopping.wait_for(|stopping| *stopping).await;
            }
            // A sleep past its deadline can wait for the next turn of the timer, and a read
            // would start before it.
            Ok(Caller::Close(budget)) if Instant::now() < budget => sleep_until(budget).await,
            Ok(Caller::Close(_)) => {}
            Err(_) => pending().await,
        }
    }

    /// Records the owner generation that a read of `partition` named.
    pub(crate) fn see_owner(&self, partition: u32, context: PartitionContext) {
        let mut owners = self.owners.entry(partition).or_default();
        owners.seen = owners.seen.max(context.owner_generation);
    }

    /// After a session change, the server installs the owner of each partition again under a
    /// higher generation. The owners that the reads named so far are stale, and a seek write in
    /// flight changes nothing.
    pub(crate) fn lose_owners(&self) {
        for mut owners in self.owners.iter_mut() {
            owners.stale = owners.seen;
        }
        self.generations_mut().session += 1;
    }

    /// After a not-owned refusal of a read, the server can have installed the owner of
    /// `partition` again under a higher generation, as after a session change.
    pub(crate) fn lose_owner(&self, partition: u32) {
        if let Some(mut owners) = self.owners.get_mut(&partition) {
            owners.stale = owners.seen;
        }
    }

    /// Whether `context` names an owner from before the last session change or not-owned
    /// refusal of `partition`. A request names owner 0 in its place, which the server binds to
    /// the owner that it installed for the member.
    pub(crate) fn is_stale(&self, partition: u32, context: PartitionContext) -> bool {
        self.owners
            .get(&partition)
            .is_some_and(|owners| owners.stale > 0 && context.owner_generation <= owners.stale)
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
        let start =
            PollingStrategy::continuation(partition, PollingKind::Offset, next, stored.context());
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

    /// With `OnRead`: takes the turn of `partition`, and waits until no read of it that can
    /// commit is in flight. A transport lane alone is not enough: HTTP has none, and a read from
    /// the old position that enters a lane after the writes still commits after them.
    pub(crate) async fn hold(self: &Arc<Self>, partition: u32) -> Result<SeekHold, IggyError> {
        let mut hold = self.take_turn(partition).await?;
        self.wake.notify_one();
        self.quiet(partition).await;
        (hold.generation, hold.session) = {
            let generations = self.generations();
            (
                generations.current.get(&partition).copied(),
                generations.session,
            )
        };
        Ok(hold)
    }

    /// With `OnRead`: waits for the seek of `partition` in progress, then holds the reads of
    /// `partition` until the hold drops, so the seeks of a partition move in their order.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidConfiguration`] once the close waited for the seeks.
    pub(crate) async fn take_turn(self: &Arc<Self>, partition: u32) -> Result<SeekHold, IggyError> {
        loop {
            let released = self.quiet.notified();
            {
                let mut generations = self.generations();
                if generations.closed {
                    return Err(IggyError::InvalidConfiguration);
                }
                if let Entry::Vacant(free) = generations.held.entry(partition) {
                    free.insert(false);
                    break;
                }
            }
            released.await;
        }
        Ok(SeekHold {
            progress: Arc::clone(self),
            partition,
            generation: None,
            session: 0,
            before: None,
        })
    }

    /// A seek that moves publishes its new position first.
    fn release(&self, partition: u32) {
        {
            let mut generations = self.generations();
            generations.held.remove(&partition);
            generations.unnamed.remove(&partition);
        }
        self.quiet.notify_waiters();
        self.wake.notify_one();
    }

    pub(crate) fn held(&self) -> BTreeSet<u32> {
        self.generations().held.keys().copied().collect()
    }

    /// Waits until no seek holds a partition, also a seek that finishes after its caller
    /// dropped it. Every later seek fails, so none writes beside the close.
    pub(crate) async fn close_seeks(&self) {
        loop {
            let released = self.quiet.notified();
            {
                let mut generations = self.generations();
                if generations.held.is_empty() {
                    generations.closed = true;
                    return;
                }
            }
            released.await;
        }
    }

    /// A fetch loop starts, which answers each hold.
    pub(crate) fn attach(&self) {
        self.generations().reading = true;
    }

    /// The fetch loop ended, so no read of it is in flight, and each hold is answered.
    pub(crate) fn detach(&self) {
        self.generations().reading = false;
        self.quiet.notify_waiters();
    }

    /// A seek write with an unknown outcome can have moved the stored offset. The reader
    /// abandons its operation on a client that retains one, so the next write of the partition
    /// gets through. A write that a new generation outlived changes nothing else, because that
    /// generation starts from the progress of the server. Otherwise the reader forgets that the
    /// delivered message is committed, so the close stores it again. Without a delivered
    /// message, the close puts `before` back, because a write can move the offset back or
    /// delete it. And the fetch loop takes the write as a lost read from the position, or from
    /// `before` for a position that named no offset, so its next read does not skip a message.
    async fn lose_write(
        self: &Arc<Self>,
        partition: u32,
        generation: Option<u64>,
        session: u64,
        operation: Option<u64>,
        before: Option<Option<u64>>,
    ) {
        if let Some(operation) = operation {
            // An operation that the client no longer retains is already free.
            let _ = self.client.read().await.abandon_operation(operation).await;
        }
        {
            let mut generations = self.generations();
            let Some(generation) = generation.filter(|generation| {
                generations.session == session
                    && generations.current.get(&partition) == Some(generation)
            }) else {
                return;
            };
            // An earlier lost write can have moved the offset that a later one looked up.
            if let Some(before) = before {
                generations.restores.entry(partition).or_insert(before);
            }
            generations.lost_writes.push(LostWrite {
                partition,
                operation: operation.unwrap_or_default(),
                generation,
                before,
            });
        }
        drop(PendingCommit::new(Arc::clone(self), vec![partition]));
    }

    pub(crate) fn take_lost_writes(&self) -> Vec<LostWrite> {
        std::mem::take(&mut self.generations().lost_writes)
    }

    /// The fetch loop holds the reads of `partition`, and no read of it that can commit is in
    /// flight, so its seek can write. `unnamed` tells that the position names no offset.
    pub(crate) fn set_quiet(&self, partition: u32, unnamed: bool) {
        let mut guard = self.generations();
        let generations = &mut *guard;
        if let Some(quiet) = generations.held.get_mut(&partition)
            && !*quiet
        {
            *quiet = true;
            if unnamed {
                generations.unnamed.insert(partition);
            }
            self.quiet.notify_waiters();
        }
    }

    /// Waits until the fetch loop answers the hold of `partition`. Without a fetch loop, no read
    /// is in flight.
    async fn quiet(&self, partition: u32) {
        loop {
            let answered = self.quiet.notified();
            let waiting = {
                let generations = self.generations();
                generations.reading && generations.held.get(&partition) == Some(&false)
            };
            if !waiting {
                return;
            }
            answered.await;
        }
    }

    pub(crate) fn deliver(&self, checkpoint: &Checkpoint) {
        self.delivered
            .insert(checkpoint.partition_id(), *checkpoint);
    }

    pub(crate) fn last_delivered(&self, partition: u32) -> Option<Checkpoint> {
        self.delivered.get(&partition).map(|entry| *entry.value())
    }

    /// The last delivered message of `partition` in its current assignment. Another member can
    /// have read the partition while it was away, so a message of an earlier assignment decides
    /// nothing at the close.
    fn delivered_here(&self, partition: u32) -> Option<Checkpoint> {
        self.last_delivered(partition)
            .filter(|delivered| self.assignment(partition, delivered.stamp()).is_some())
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
            if let Err(error) = self.abandon_unknown(self.store(checkpoint).await).await {
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
            match self.abandon_unknown(self.store(checkpoint).await).await {
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
        self.abandon_unknown(self.store(checkpoint).await).await
    }

    /// A write of the reader with an unknown outcome stays open on the client, which keeps every
    /// later write of its partition out. The reader or the application writes the partition
    /// again, so the reader abandons it, as the fetch loop does for a lost read. The error stays.
    async fn abandon_unknown(&self, result: Result<(), IggyError>) -> Result<(), IggyError> {
        if let Err(IggyError::OperationUncertain(operation)) = result {
            // An operation that the client no longer retains is already free.
            let _ = self.client.read().await.abandon_operation(operation).await;
        }
        result
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
        self.adopt_undecoded(start.partition_id(), start.context());
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
        self.adopt_undecoded(partition, context);
        self.undecoded.remove_if(&partition, |_, start| {
            !same_incarnation_and_owner(start.context(), context)
        });
    }

    /// A delivered batch covers the undecoded start of its partition, or replaces its history.
    pub(crate) fn forget_undecoded_covered(&self, batch: &PolledMessages) {
        self.adopt_undecoded(batch.partition_id, batch.context());
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

    /// The first read of the current owner takes over the undecoded start of a stale owner in
    /// the same history, so the start keeps deciding the close.
    fn adopt_undecoded(&self, partition: u32, context: PartitionContext) {
        if self.is_stale(partition, context) {
            return;
        }
        if let Some(mut start) = self.undecoded.get_mut(&partition)
            && start.context().incarnation == context.incarnation
            && self.is_stale(partition, start.context())
        {
            *start = Checkpoint::new(partition, start.offset(), context);
        }
    }

    pub(crate) fn retain_undecoded(&self, partitions: &[u32]) {
        self.undecoded
            .retain(|partition, _| partitions.binary_search(partition).is_ok());
    }

    /// The `OnRead` close: the server stored the offsets of `undelivered`, so the offset of
    /// each partition moves back to its last delivered message. A partition that delivered
    /// nothing in the history of a batch moves before that batch, whatever the start of the
    /// reader, because the next reader resumes after the stored offset. `owned` limits a group
    /// member to the partitions that it still owns. Every partition is tried, and the first
    /// failure is the error.
    pub(crate) async fn rewind(
        &self,
        undelivered: Vec<PolledMessages>,
        owned: Option<&[u32]>,
    ) -> Result<(), IggyError> {
        let mut failure = None;
        let mut rewound = BTreeSet::new();
        let mut rewound_partitions = BTreeSet::new();
        for polled in undelivered {
            let partition_id = polled.partition_id;
            // A batch of an earlier assignment decides nothing: another member can have read the
            // partition since, and the server refuses a store under the old owner.
            if self.blocked.contains_key(&partition_id)
                || owned.is_some_and(|owned| !owned.contains(&partition_id))
                || polled.stamp().map(|stamp| stamp.generation) != self.generation(partition_id)
            {
                continue;
            }
            let (Some(first), Some(last)) = (polled.checkpoint_at(0), polled.checkpoint()) else {
                continue;
            };
            // The server refuses the final store of a message delivered in
            // another history, so that store cannot rewind this batch.
            if self.delivered_here(partition_id).is_some_and(|delivered| {
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
            if !rewound.insert(history) {
                continue;
            }
            rewound_partitions.insert(partition_id);
            if let Err(error) = self.abandon_unknown(self.rewind_before(first).await).await {
                error!(
                    "Failed to rewind the offset before undelivered messages in partition: {partition_id}. {error}"
                );
                failure.get_or_insert(error);
            }
        }
        let restores = std::mem::take(&mut self.generations().restores);
        for (&partition_id, &before) in &restores {
            // A known batch or a delivered message names the rewind.
            if self.blocked.contains_key(&partition_id)
                || owned.is_some_and(|owned| !owned.contains(&partition_id))
                || rewound_partitions.contains(&partition_id)
                || self.delivered_here(partition_id).is_some()
            {
                continue;
            }
            // An offset at or past the start of a lost read shows that the read stored, so the
            // offset moves before that start.
            let start = self
                .undecoded
                .get(&partition_id)
                .map(|start| *start)
                .filter(|start| before.is_some_and(|before| before >= start.offset()))
                .unwrap_or_else(|| {
                    let resume = before.map_or(0, |before| before.saturating_add(1));
                    Checkpoint::new(partition_id, resume, PartitionContext::default())
                });
            if let Err(error) = self.abandon_unknown(self.rewind_before(start).await).await {
                error!(partition_id, ?before, %error, "Failed to restore the offset from before a seek write");
                failure.get_or_insert(error);
            }
        }
        let undecoded: Vec<_> = self.undecoded.iter().map(|entry| *entry.value()).collect();
        for start in undecoded {
            let partition_id = start.partition_id();
            let without_history = start.context() == PartitionContext::default();
            // A known batch or a delivered message names the rewind of a start without a history.
            if self.blocked.contains_key(&partition_id)
                || owned.is_some_and(|owned| !owned.contains(&partition_id))
                || restores.contains_key(&partition_id)
                || (without_history && rewound_partitions.contains(&partition_id))
                || self.delivered_here(partition_id).is_some_and(|delivered| {
                    without_history
                        || same_incarnation_and_owner(delivered.context(), start.context())
                })
                || self.last_committed(partition_id).is_some_and(|committed| {
                    same_incarnation_and_owner(committed.context(), start.context())
                        && committed.offset() >= start.offset()
                })
            {
                continue;
            }
            match self
                .abandon_unknown(self.rewind_undecoded(start).await)
                .await
            {
                Ok(()) => self.forget_undecoded(partition_id),
                Err(error) => {
                    error!(?start, %error, "Failed to rewind an undecoded committed fetch section");
                    failure.get_or_insert(error);
                }
            }
        }
        for checkpoint in self.unstored_delivered(owned) {
            let partition_id = checkpoint.partition_id();
            // A message of an earlier assignment decides nothing. A rewind under a newer history
            // replaced the commit record. The server refuses a position of the older history,
            // which means nothing in the newer one.
            if self.blocked.contains_key(&partition_id)
                || self.delivered_here(partition_id).is_none()
                || self.last_committed(partition_id).is_some_and(|committed| {
                    !same_incarnation_and_owner(committed.context(), checkpoint.context())
                })
            {
                continue;
            }
            if let Err(error) = self.abandon_unknown(self.store(checkpoint).await).await {
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

    /// With `OnRead`: deletes the stored offset of `partition_id` under `context` when the
    /// lookup found one in `stored`, so that the next read from offset 0 commits again. The
    /// commit of a read never moves the offset back. It forgets the last delivered message and
    /// the offset from before a lost write, so a close before the next delivery cannot store an
    /// old position again.
    pub(crate) async fn delete_stored(
        &self,
        partition_id: u32,
        stored: Option<u64>,
        context: Option<PartitionContext>,
    ) -> Result<(), IggyError> {
        self.delete_if_stored(partition_id, stored, context).await?;
        self.delivered.remove(&partition_id);
        self.generations().restores.remove(&partition_id);
        Ok(())
    }

    /// The server refuses a delete of a missing offset, so the caller looks it up first.
    async fn delete_if_stored(
        &self,
        partition_id: u32,
        stored: Option<u64>,
        context: Option<PartitionContext>,
    ) -> Result<(), IggyError> {
        // Cancellation can hide a committed delete, so a later store must reach the server.
        let removed = self.committed.remove(&partition_id);
        if stored.is_none() {
            return Ok(());
        }
        let mutation = match context {
            Some(context) => {
                OffsetMutation::DeletePosition(Checkpoint::new(partition_id, 0, context))
            }
            None => OffsetMutation::Delete {
                partition_id: Some(partition_id),
            },
        };
        let result = self.mutate_offset(mutation).await;
        // A refused delete leaves the offset on the server, so its record comes back, unless a
        // newer commit wrote one.
        if let (Err(error), Some((_, record))) = (&result, removed)
            && unknown_outcome(error).is_none()
        {
            self.committed.entry(partition_id).or_insert(record);
        }
        result
    }

    pub(crate) fn is_current(&self, checkpoint: &Checkpoint) -> bool {
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

    /// A start without a history names no known message: a read from past the end of the
    /// partition stores nothing, and the server refuses a store past its last message. So the
    /// offset moves back only when the stored offset shows that the read stored.
    async fn rewind_undecoded(&self, start: Checkpoint) -> Result<(), IggyError> {
        if start.context() == PartitionContext::default() && start.offset() > 0 {
            let stored = self.stored_offset(start.partition_id()).await?;
            if stored.is_none_or(|stored| stored < start.offset()) {
                return Ok(());
            }
        }
        self.rewind_before(start).await
    }

    async fn rewind_before(&self, start: Checkpoint) -> Result<(), IggyError> {
        let partition_id = start.partition_id();
        match start.offset().checked_sub(1) {
            Some(offset) => {
                self.store(Checkpoint::new(partition_id, offset, start.context()))
                    .await
            }
            None => {
                let stored = self.stored_offset(partition_id).await?;
                self.delete_if_stored(partition_id, stored, Some(start.context()))
                    .await
            }
        }
    }

    /// The lookup waits for the committing read in flight of the partition, as a store does.
    pub(crate) async fn stored_offset(&self, partition_id: u32) -> Result<Option<u64>, IggyError> {
        let stored = crate::clients::with_client!(self, client => {
            client
                .get_consumer_offset(&self.consumer, &self.stream, &self.topic, Some(partition_id))
                .await
        })?;
        Ok(stored.map(|stored| stored.stored_offset))
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
        let assignment = self.assignment_of(mutation);
        let (mutation, result) = crate::clients::with_client!(self, client => {
            let mutation = self.renew(mutation, client.metadata_watermark().await);
            Ok::<_, IggyError>((mutation, self.send_in_lane(&client, mutation).await))
        })?;
        if !matches!(result, Err(IggyError::ConsumerGroupPartitionNotOwned(..))) {
            return result;
        }
        // Each request after the refusal has a deadline of its own, as the retries of the read
        // for the installed owner can outlast one.
        match self.for_installed_owner(mutation, assignment).await {
            Some(installed) => crate::clients::with_client!(self, client => {
                self.send_in_lane(&client, installed).await
            }),
            None => result,
        }
    }

    /// The generation that started the assignment of the partition of `mutation`, while the
    /// mutation belongs to it.
    fn assignment_of(&self, mutation: OffsetMutation) -> Option<u64> {
        let (partition, stamp) = match mutation {
            OffsetMutation::Store { partition_id, .. } => (partition_id, None),
            OffsetMutation::Delete { partition_id } => (partition_id?, None),
            OffsetMutation::StorePosition(checkpoint)
            | OffsetMutation::DeletePosition(checkpoint) => {
                (checkpoint.partition_id(), checkpoint.stamp())
            }
        };
        self.assignment(partition, stamp)
    }

    /// Sends `mutation`, and once more after the operation that blocked its lane resolves.
    async fn send_in_lane(
        &self,
        client: &ClientWrapper,
        mutation: OffsetMutation,
    ) -> Result<(), IggyError> {
        let deadline = operation_deadline();
        match self.send_offset_mutation(client, mutation).await {
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
                self.send_offset_mutation(client, mutation).await
            }
            result => result,
        }
    }

    /// A write refused as not owned can name an owner that the server installed again, or owner
    /// 0 under a metadata op from before the installation: a rebalance by another member
    /// installs at the op of that member, which the session watermark does not reach. While the
    /// partition stays in `assignment`, the assignment of the write, its owner counts as lost,
    /// the fetch loop refreshes the assignment, and a read without a commit under owner 0 shows
    /// the installed owner. The write goes once more under the owner that it shows, in its own
    /// history only. Another member can own a partition that left, or can have read it while it
    /// was away, so a write of an assignment that ended keeps its refusal. So does a write whose
    /// caller ends the retries of its read: the close start for a write of the reader itself,
    /// and the end of the close budget for a write of the close.
    async fn for_installed_owner(
        &self,
        mutation: OffsetMutation,
        assignment: Option<u64>,
    ) -> Option<OffsetMutation> {
        let (partition, offset, history) = match mutation {
            OffsetMutation::Store {
                partition_id,
                offset,
            } => (partition_id, offset, None),
            OffsetMutation::Delete { partition_id } => (partition_id?, 0, None),
            // A context without an incarnation names the history of the route, as none does.
            OffsetMutation::StorePosition(checkpoint)
            | OffsetMutation::DeletePosition(checkpoint) => (
                checkpoint.partition_id(),
                checkpoint.offset(),
                Some(checkpoint.context()).filter(|context| context.incarnation != 0),
            ),
        };
        if assignment.is_none() || self.assignment(partition, None) != assignment {
            return None;
        }
        self.lose_owner(partition);
        self.request_refresh();
        let strategy = match history {
            Some(context) => PollingStrategy::continuation(
                partition,
                PollingKind::Offset,
                offset,
                PartitionContext {
                    owner_generation: 0,
                    ..context
                },
            ),
            None => PollingStrategy::offset(offset),
        };
        // A read answered as its caller ends the retries leads to no write either.
        let installed = tokio::select! {
            biased;
            () = self.cancelled() => None,
            installed = self.installed_context(partition, &strategy) => installed,
        }?;
        if history.is_some_and(|context| context.incarnation != installed.incarnation) {
            return None;
        }
        self.see_owner(partition, installed);
        let checkpoint = Checkpoint::new(partition, offset, installed);
        Some(match mutation {
            OffsetMutation::Store { .. } | OffsetMutation::StorePosition(_) => {
                OffsetMutation::StorePosition(checkpoint)
            }
            OffsetMutation::Delete { .. } | OffsetMutation::DeletePosition(_) => {
                OffsetMutation::DeletePosition(checkpoint)
            }
        })
    }

    /// The context of a read of `partition` from `strategy` without a commit, which the server
    /// serves for the owner that it installed. A transient refusal of the read or of its request
    /// reads again after the retry interval, until the deadline of the caller, or without one,
    /// until the caller ends the retries.
    async fn installed_context(
        &self,
        partition: u32,
        strategy: &PollingStrategy,
    ) -> Option<PartitionContext> {
        /// The smallest read: only the context of the reply counts.
        const PROBE_COUNT: u32 = 1;
        let probe = PollOptions {
            count: PROBE_COUNT,
            ..PollOptions::default()
        };
        loop {
            let installed = crate::clients::with_client!(self, client => {
                client
                    .poll_context(
                        &self.stream,
                        &self.topic,
                        partition,
                        &self.consumer,
                        strategy,
                        probe,
                    )
                    .await
            });
            match installed {
                Ok(installed) => return Some(installed),
                Err(error)
                    if transient_refusal(&error) && Instant::now() < operation_deadline() =>
                {
                    sleep_until((Instant::now() + self.retry_interval).min(operation_deadline()))
                        .await;
                }
                Err(_) => return None,
            }
        }
    }

    /// A write of a stale owner names owner 0 in the history of its context. The server binds
    /// owner 0 only under a metadata op at or after the installation of the owner, and after a
    /// session change the reply of the join raised the session watermark past it.
    fn renew(&self, mutation: OffsetMutation, watermark: u64) -> OffsetMutation {
        let (OffsetMutation::StorePosition(checkpoint)
        | OffsetMutation::DeletePosition(checkpoint)) = mutation
        else {
            return mutation;
        };
        let context = checkpoint.context();
        // A fully zero context names no history, so it takes the context of the route, as no
        // context does.
        if context == PartitionContext::default()
            || !self.is_stale(checkpoint.partition_id(), context)
        {
            return mutation;
        }
        let renewed = Checkpoint::new(
            checkpoint.partition_id(),
            checkpoint.offset(),
            PartitionContext {
                owner_generation: 0,
                metadata_op: context.metadata_op.max(watermark),
                ..context
            },
        );
        match mutation {
            OffsetMutation::StorePosition(_) => OffsetMutation::StorePosition(renewed),
            _ => OffsetMutation::DeletePosition(renewed),
        }
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

/// Holds the reads of a partition while its seek writes the stored offset. Dropping it lets them
/// start again.
pub(crate) struct SeekHold {
    progress: Arc<Progress>,
    partition: u32,
    /// The generation of the partition when the fetch loop answered the hold, so at the write.
    generation: Option<u64>,
    /// The session at the write. In a new session another member can have stored progress.
    session: u64,
    /// See [`LostWrite::before`].
    before: Option<Option<u64>>,
}

impl SeekHold {
    /// For a position that names no offset, or a partition without a delivered message, looks
    /// up the stored offset before the writes. After a write with an unknown outcome, a position
    /// that names no offset moves to it, so the next read skips no message, and the close puts
    /// it back while nothing was delivered.
    pub(crate) async fn look_up_before(&mut self) -> Result<(), IggyError> {
        if self.needs_before() {
            self.before = Some(self.progress.stored_offset(self.partition).await?);
        }
        Ok(())
    }

    /// [`Self::look_up_before`] for a seek that looked up `stored` itself.
    pub(crate) fn keep_before(&mut self, stored: Option<u64>) {
        if self.needs_before() {
            self.before = Some(stored);
        }
    }

    /// Without a delivered message of the current assignment, the close can restore no other
    /// offset.
    fn needs_before(&self) -> bool {
        let unnamed = self
            .progress
            .generations()
            .unnamed
            .contains(&self.partition);
        unnamed || self.progress.delivered_here(self.partition).is_none()
    }

    /// Runs the writes of a seek and its move in a task of its own, which keeps the hold until
    /// they end. So a caller that drops the seek once the writes started still lets them finish,
    /// and the move follow on success. A later seek and the close wait for that task, and the
    /// close ends its retries, as does a deadline of the caller. The task logs a failure that no
    /// caller takes.
    pub(crate) async fn finish(
        self,
        write: impl Future<Output = Result<(), IggyError>> + Send + 'static,
    ) -> Result<(), IggyError> {
        // The task runs outside the deadline scope of the caller.
        let deadline = binary_deadline_in_force();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let partition = self.partition;
            let write = in_background(write);
            let result = match deadline {
                Some(deadline) => with_deadline(deadline, write).await,
                None => write.await,
            };
            if let Err(error) = &result
                && let Some(operation) = unknown_outcome(error)
            {
                self.progress
                    .lose_write(
                        partition,
                        self.generation,
                        self.session,
                        operation,
                        self.before,
                    )
                    .await;
            }
            drop(self);
            if let Err(Err(error)) = sender.send(result) {
                warn!(partition, %error, "A seek that its caller dropped failed to write");
            }
        });
        receiver.await.unwrap_or(Err(IggyError::ClientShutdown))
    }
}

impl Drop for SeekHold {
    fn drop(&mut self) {
        self.progress.release(self.partition);
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

/// Whether a write that failed with `error` may have landed, with the operation that the client
/// retains for it. HTTP retains none and reports such a write as
/// [`IggyError::TransientNotCommitted`].
fn unknown_outcome(error: &IggyError) -> Option<Option<u64>> {
    match error {
        IggyError::OperationUncertain(operation) => Some(Some(*operation)),
        IggyError::TransientNotCommitted => Some(None),
        _ => None,
    }
}

/// Offsets are comparable only within one partition incarnation and owner. The metadata frontier
/// is left out: another group's ownership change can advance it between two polls of the same
/// partition.
pub(crate) fn same_incarnation_and_owner(left: PartitionContext, right: PartitionContext) -> bool {
    left.incarnation == right.incarnation && left.owner_generation == right.owner_generation
}
