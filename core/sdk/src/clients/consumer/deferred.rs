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
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    AssignmentVersion, ConsumerPosition, EncryptorKind, IggyError, IggyMessage, MessageClient,
    PolledMessages, PollingKind, PollingStrategy,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{AbortHandle, Id, JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::error;

use super::{IggyConsumer, ORDERING, same_incarnation_and_owner};
use crate::driver::{
    DEFERRED_CONSUMER_CAPACITY, MAX_DEFERRED_REQUESTS_PER_CONSUMER, with_deadline,
};

const ASSIGNMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_PREFETCH_BYTES: u32 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_PREFETCH_MESSAGES: u32 = 16_000;

/// A reservation covers the whole possible reply, including work still in flight.
/// Keep it until the batch is drained: message slices can share its backing allocation.
pub(super) struct FetchCapacity {
    permit: Option<OwnedSemaphorePermit>,
    wake: Arc<Notify>,
}

impl Drop for FetchCapacity {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.wake.notify_one();
    }
}

pub(super) struct PrefetchedBatch {
    pub generation: u64,
    pub assignment: Option<AssignmentVersion>,
    pub result: Result<PolledMessages, IggyError>,
    pub capacity: FetchCapacity,
}

pub(super) struct DeferredPolls {
    receiver: mpsc::Receiver<PrefetchedBatch>,
    wake: Arc<Notify>,
    task: JoinHandle<()>,
}

impl DeferredPolls {
    pub(super) fn start(consumer: &IggyConsumer) -> Self {
        let slots = consumer.prefetch_slots();
        let (sender, receiver) = mpsc::channel(slots);
        let worker = Worker::new(consumer, sender);
        Self {
            receiver,
            wake: Arc::clone(&worker.wake),
            task: tokio::spawn(worker.run()),
        }
    }

    #[cfg(test)]
    pub(super) fn with_error(consumer: &IggyConsumer, error: IggyError) -> Self {
        let (sender, receiver) = mpsc::channel(consumer.prefetch_slots());
        let worker = Worker::new(consumer, sender.clone());
        let capacity = worker.reserve().unwrap();
        let generation = worker.epoch;
        sender
            .try_send(PrefetchedBatch {
                generation,
                assignment: None,
                result: Err(error),
                capacity,
            })
            .unwrap_or_else(|_| panic!("the test queue is empty"));
        Self {
            receiver,
            wake: Arc::clone(&worker.wake),
            task: tokio::spawn(async move {
                worker.wake.notified().await;
                worker.run().await;
            }),
        }
    }

    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<PrefetchedBatch>> {
        self.receiver.poll_recv(cx)
    }

    /// Stops fetching and returns the polled batches that the application did not take.
    ///
    /// The server can commit the batch of a committing fetch in flight. With a `grace`, the
    /// woken worker sees the shutdown flag of the consumer and lets its fetches hand their
    /// batches over first. After the grace, a fetch still in flight is aborted, and its
    /// pending commit forgets the stored offset.
    pub(super) async fn stop(mut self, grace: Option<Duration>) -> Vec<PolledMessages> {
        let ended = match grace {
            Some(grace) => {
                self.wake.notify_one();
                tokio::time::timeout(grace, &mut self.task).await.is_ok()
            }
            None => false,
        };
        if !ended {
            self.task.abort();
        }
        // The channel closes once the worker and every pending commit are gone.
        // The drain then holds every batch, and each aborted fetch already
        // forgot its stored offset.
        let mut undelivered = Vec::new();
        while let Some(batch) = self.receiver.recv().await {
            if let Ok(polled) = batch.result {
                undelivered.push(polled);
            }
        }
        undelivered
    }
}

impl Drop for DeferredPolls {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CompletedPoll {
    /// The fetch state that started the fetch.
    epoch: u64,
    partition: u32,
    /// The position that the fetch read from.
    strategy: PollingStrategy,
    result: Result<PolledMessages, IggyError>,
    capacity: FetchCapacity,
    /// Set for a fetch that commits its batch.
    commit: Option<PendingCommit>,
}

/// The server can commit the batch of a committing fetch whose reply never
/// reaches the application. Unless the fetch settles, dropping this forgets
/// the stored offset of its partition. A shutdown then stores the last
/// delivered message again, which moves the server offset back to it.
struct PendingCommit {
    partition: u32,
    consumed: Arc<DashMap<u32, ConsumerPosition>>,
    stored: Arc<DashMap<u32, ConsumerPosition>>,
    /// An aborted worker leaves the drop of a fetch in flight to the task of that
    /// fetch. Until then, this keeps the batch channel open for `stop` to wait on.
    _batches: mpsc::Sender<PrefetchedBatch>,
    settled: bool,
}

impl PendingCommit {
    fn settle(mut self) {
        self.settled = true;
    }
}

impl Drop for PendingCommit {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // A record that differs from the delivered message makes a shutdown
        // store that message anyway. Such a record can also be an offset that
        // the application stored to skip a message.
        let consumed = self
            .consumed
            .get(&self.partition)
            .map(|entry| *entry.value());
        self.stored
            .remove_if(&self.partition, |_, stored| consumed == Some(*stored));
    }
}

struct CompletedWait {
    partitions: Vec<u32>,
    result: Result<Vec<u32>, IggyError>,
}

/// The assignment that a refresh read.
struct Refreshed {
    /// The fetch state that started the refresh.
    epoch: u64,
    assignment: AssignmentVersion,
    partitions: Vec<u32>,
    /// Set when readiness waits cover the partitions.
    groups: Option<Vec<Vec<u32>>>,
}

/// Readiness state of the fetches that start only once a partition holds a
/// message: every fetch of `AutoCommitWhen::PollingMessages`, where each poll
/// commits its batch on the server, and the fetches of a group member with
/// more partitions than fetch slots.
struct OnDemand {
    /// Set while the application waits for a message and its buffer is empty.
    demand: Arc<AtomicBool>,
    /// One readiness wait per group that has idle partitions.
    waits: JoinSet<CompletedWait>,
    /// The assigned partitions, grouped by primary endpoint and split to fit
    /// one wait request each.
    groups: Vec<Vec<u32>>,
    /// Partitions that hold a message for their next poll.
    ready: BTreeSet<u32>,
    /// The wait that covers each waiting partition.
    waiting: BTreeMap<u32, AbortHandle>,
    /// The offset after the newest message that a `Timestamp` poll skipped.
    floors: BTreeMap<u32, u64>,
}

impl OnDemand {
    fn clear(&mut self) {
        self.waits = JoinSet::new();
        self.groups.clear();
        self.ready.clear();
        self.waiting.clear();
        self.floors.clear();
    }

    /// A wait can end before the restart that replaces it cancels it. Only
    /// the wait that covers the partitions now reports them.
    fn covers(&self, wait: Id, partitions: &[u32]) -> bool {
        partitions.iter().all(|partition| {
            self.waiting
                .get(partition)
                .is_some_and(|running| running.id() == wait)
        })
    }
}

struct Worker {
    client: iggy_common::locking::IggyRwLock<crate::client_wrappers::client_wrapper::ClientWrapper>,
    stream: Arc<iggy_common::Identifier>,
    topic: Arc<iggy_common::Identifier>,
    consumer: Arc<iggy_common::Consumer>,
    name: String,
    standalone_partition: u32,
    is_group: bool,
    auto_join: bool,
    create_group: bool,
    joined: Arc<std::sync::atomic::AtomicBool>,
    uncertain_group_operation: Arc<tokio::sync::Mutex<Option<u64>>>,
    can_poll: Arc<std::sync::atomic::AtomicBool>,
    shutdown: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    wake: Arc<Notify>,
    strategy: PollingStrategy,
    options: iggy_common::DeferredPollOptions,
    count: u32,
    auto_commit: bool,
    encryptor: Option<Arc<iggy_common::EncryptorKind>>,
    retry_interval: Duration,
    sender: mpsc::Sender<PrefetchedBatch>,
    capacity: Arc<Semaphore>,
    request_capacity: Arc<Semaphore>,
    /// The permits of `capacity`.
    slots: usize,
    tasks: JoinSet<CompletedPoll>,
    /// At most one refresh, apart from the fetches, so a slow one never holds
    /// their replies back.
    refreshing: JoinSet<Result<Refreshed, IggyError>>,
    active: BTreeSet<u32>,
    /// Partitions whose next record exceeds `max_bytes`.
    oversized: BTreeSet<u32>,
    positions: BTreeMap<u32, PollingStrategy>,
    /// The last message that the application took from each partition.
    consumed: Arc<DashMap<u32, ConsumerPosition>>,
    /// The last offset that the application stored for each partition.
    stored: Arc<DashMap<u32, ConsumerPosition>>,
    retry_at: BTreeMap<u32, Instant>,
    partitions: Vec<u32>,
    assignment: Option<AssignmentVersion>,
    pending_error: Option<IggyError>,
    epoch: u64,
    cursor: Option<u32>,
    on_demand: OnDemand,
}

impl Worker {
    fn new(consumer: &IggyConsumer, sender: mpsc::Sender<PrefetchedBatch>) -> Self {
        let slots = consumer.prefetch_slots();
        Self {
            client: consumer.client.clone(),
            stream: Arc::clone(&consumer.stream_id),
            topic: Arc::clone(&consumer.topic_id),
            consumer: Arc::clone(&consumer.consumer),
            name: consumer.consumer_name.clone(),
            standalone_partition: consumer.partition_id.unwrap_or_default(),
            is_group: consumer.is_consumer_group,
            auto_join: consumer.auto_join_consumer_group,
            create_group: consumer.create_consumer_group_if_not_exists,
            joined: Arc::clone(&consumer.joined_consumer_group),
            uncertain_group_operation: Arc::clone(&consumer.uncertain_consumer_group_operation),
            can_poll: Arc::clone(&consumer.can_poll),
            shutdown: Arc::clone(&consumer.shutdown),
            generation: Arc::clone(&consumer.fetch_generation),
            wake: Arc::clone(&consumer.fetch_notify),
            strategy: consumer.polling_strategy,
            options: consumer.poll_options,
            count: consumer.batch_length,
            auto_commit: consumer.auto_commit_after_polling,
            encryptor: consumer.encryptor.clone(),
            retry_interval: consumer.reconnection_retry_interval.get_duration(),
            sender,
            capacity: Arc::new(Semaphore::new(slots)),
            request_capacity: Arc::new(Semaphore::new(MAX_DEFERRED_REQUESTS_PER_CONSUMER)),
            slots,
            tasks: JoinSet::new(),
            refreshing: JoinSet::new(),
            active: BTreeSet::new(),
            oversized: BTreeSet::new(),
            positions: BTreeMap::new(),
            consumed: Arc::clone(&consumer.state.last_consumed_offsets),
            stored: Arc::clone(&consumer.state.last_stored_offsets),
            retry_at: BTreeMap::new(),
            partitions: Vec::new(),
            assignment: None,
            pending_error: None,
            epoch: consumer.fetch_generation.load(ORDERING),
            cursor: None,
            on_demand: OnDemand {
                demand: Arc::clone(&consumer.fetch_demand),
                waits: JoinSet::new(),
                groups: Vec::new(),
                ready: BTreeSet::new(),
                waiting: BTreeMap::new(),
                floors: BTreeMap::new(),
            },
        }
    }

    async fn run(mut self) {
        let mut refresh = tokio::time::interval(ASSIGNMENT_REFRESH_INTERVAL);
        refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if self.sender.is_closed() {
                return;
            }
            if self.shutdown.load(ORDERING) {
                self.finish_fetches().await;
                return;
            }
            if self.epoch != self.generation.load(ORDERING) {
                self.rewind();
                self.epoch = self.generation.load(ORDERING);
                refresh.reset_immediately();
            }
            let enabled =
                self.can_poll.load(ORDERING) || !self.client.read().await.can_recover().await;
            self.flush_error();
            if enabled {
                self.schedule();
            }
            let retry_at = self
                .retry_at
                .values()
                .min()
                .copied()
                .unwrap_or_else(|| Instant::now() + ASSIGNMENT_REFRESH_INTERVAL);
            tokio::select! {
                _ = self.sender.closed() => return,
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep_until(retry_at), if !self.retry_at.is_empty() => {
                    self.retry_at.retain(|_, deadline| *deadline > Instant::now());
                },
                _ = refresh.tick() => {
                    if enabled && self.refreshing.is_empty() {
                        let deadline = Instant::now() + self.options.request_timeout.get_duration();
                        self.refreshing.spawn(with_deadline(deadline, self.read_assignment()));
                    }
                },
                Some(refreshed) = self.refreshing.join_next(), if !self.refreshing.is_empty() => {
                    match refreshed {
                        Ok(Ok(refreshed)) => {
                            if self.apply(refreshed) {
                                refresh.reset();
                            } else {
                                refresh.reset_immediately();
                            }
                        }
                        Ok(Err(error)) => {
                            self.report(error);
                            refresh.reset_after(self.retry_interval);
                        }
                        Err(_) => {
                            self.invalidate();
                            self.report(IggyError::ClientShutdown);
                            return;
                        }
                    }
                },
                Some(completed) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match completed {
                        Ok(completed) => {
                            self.complete(completed);
                            if self.partitions.is_empty() {
                                refresh.reset_after(self.retry_interval);
                            }
                        },
                        Err(_) => {
                            self.invalidate();
                            self.report(IggyError::ClientShutdown);
                            return;
                        }
                    }
                }
                Some(joined) = self.on_demand.waits.join_next_with_id(), if !self.on_demand.waits.is_empty() => {
                    match joined {
                        Ok((wait, completed)) => {
                            if self.on_demand.covers(wait, &completed.partitions) {
                                self.complete_wait(completed);
                            }
                        }
                        // A restart cancelled the wait.
                        Err(error) if error.is_cancelled() => {}
                        Err(_) => {
                            self.invalidate();
                            self.report(IggyError::ClientShutdown);
                            return;
                        }
                    }
                    if self.partitions.is_empty() {
                        refresh.reset_after(self.retry_interval);
                    }
                }
            }
        }
    }

    /// The server can commit the batch of a committing fetch in flight, so the
    /// worker ends once its fetches hand their batches over. Nothing new starts.
    async fn finish_fetches(&mut self) {
        while let Some(Ok(completed)) = self.tasks.join_next().await {
            self.complete(completed);
        }
    }

    /// A new fetch generation discards every batch that the application did
    /// not take, so each partition continues after its last delivered message.
    /// The server already committed the batch of a committing fetch, so that
    /// fetch finishes, delivers it and moves its position. Nothing is fetched
    /// until the next refresh confirms the assignment.
    fn rewind(&mut self) {
        if !self.auto_commit {
            self.tasks = JoinSet::new();
            self.active.clear();
            let consumed = &self.consumed;
            // A partition without a delivered message in the incarnation of
            // its position restarts from the initial strategy.
            self.positions.retain(|partition, position| {
                let Some(consumed) = consumed.get(partition).map(|entry| *entry.value()) else {
                    return false;
                };
                let same_incarnation = position
                    .context
                    .is_some_and(|context| same_incarnation_and_owner(context, consumed.context));
                match consumed.offset.checked_add(1) {
                    Some(next) if same_incarnation => {
                        *position = PollingStrategy::offset(next).with_context(consumed.context);
                        true
                    }
                    _ => false,
                }
            });
        }
        self.on_demand.clear();
        self.oversized.clear();
        self.retry_at.clear();
        self.partitions.clear();
        self.pending_error = None;
    }

    fn invalidate(&mut self) {
        self.epoch = self.generation.fetch_add(1, ORDERING) + 1;
        self.rewind();
    }

    fn read_assignment(
        &self,
    ) -> impl Future<Output = Result<Refreshed, IggyError>> + Send + 'static {
        let client = self.client.clone();
        let stream = Arc::clone(&self.stream);
        let topic = Arc::clone(&self.topic);
        let consumer = Arc::clone(&self.consumer);
        let name = self.name.clone();
        let joined = Arc::clone(&self.joined);
        let uncertain_group_operation = Arc::clone(&self.uncertain_group_operation);
        let join = self.is_group && self.auto_join;
        let create_group = self.create_group;
        let is_group = self.is_group;
        let standalone_partition = self.standalone_partition;
        let (auto_commit, slots) = (self.auto_commit, self.slots);
        let epoch = self.epoch;
        async move {
            if join && !joined.load(ORDERING) {
                IggyConsumer::initialize_consumer_group(
                    client.clone(),
                    create_group,
                    Arc::clone(&stream),
                    Arc::clone(&topic),
                    &name,
                    joined,
                    uncertain_group_operation,
                )
                .await?;
            }
            let (assignment, mut partitions) = if is_group {
                client
                    .read()
                    .await
                    .deferred_poll_partitions(&stream, &topic, &consumer.id)
                    .await?
            } else {
                (AssignmentVersion::default(), vec![standalone_partition])
            };
            partitions.sort_unstable();
            let groups = if waits_for_readiness(is_group, auto_commit, partitions.len(), slots)
                && !partitions.is_empty()
            {
                Some(
                    client
                        .read()
                        .await
                        .readiness_groups(&stream, &topic, &partitions)
                        .await?,
                )
            } else {
                None
            };
            Ok(Refreshed {
                epoch,
                assignment,
                partitions,
                groups,
            })
        }
    }

    /// Returns false for a refresh that started before the current fetch
    /// state, which only a new refresh replaces.
    fn apply(&mut self, refreshed: Refreshed) -> bool {
        if refreshed.epoch != self.epoch || self.epoch != self.generation.load(ORDERING) {
            return false;
        }
        if self.assignment != Some(refreshed.assignment) {
            self.invalidate();
            // The server keeps the owner of a partition that stays with its
            // member, so that partition continues from its position. Every
            // partition has a new owner in a new session. A partition that
            // moved away and back between two refreshes has one too: the
            // server refuses a poll from its old position, and the worker
            // then drops that position.
            if self
                .assignment
                .is_some_and(|assignment| assignment.session == refreshed.assignment.session)
            {
                self.positions
                    .retain(|partition, _| refreshed.partitions.binary_search(partition).is_ok());
            } else {
                self.positions.clear();
            }
            self.assignment = Some(refreshed.assignment);
        }
        self.partitions = refreshed.partitions;
        if let Some(groups) = refreshed.groups
            && groups != self.on_demand.groups
        {
            // A restart cancels the wait of its own group only, so no wait
            // may cover partitions of two groups.
            self.on_demand.waits = JoinSet::new();
            self.on_demand.waiting.clear();
            self.on_demand.groups = groups;
        }
        true
    }

    fn reserve(&self) -> Option<FetchCapacity> {
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .ok()
            .map(|permit| FetchCapacity {
                permit: Some(permit),
                wake: Arc::clone(&self.wake),
            })
    }

    fn report(&mut self, error: IggyError) {
        if matches!(error, IggyError::ConsumerGroupMemberNotFound(..)) {
            self.joined.store(false, ORDERING);
            if self.auto_join {
                return;
            }
        }
        if matches!(
            error,
            IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient
        ) {
            self.joined.store(false, ORDERING);
        }
        self.pending_error = Some(error);
        self.flush_error();
    }

    fn flush_error(&mut self) {
        let Some(error) = self.pending_error.take() else {
            return;
        };
        let Some(capacity) = self.reserve() else {
            self.pending_error = Some(error);
            return;
        };
        self.deliver(PrefetchedBatch {
            generation: self.epoch,
            assignment: self.assignment,
            result: Err(error),
            capacity,
        });
    }

    /// The application takes one item per demand and asks again for the next.
    fn deliver(&self, batch: PrefetchedBatch) {
        self.on_demand.demand.store(false, ORDERING);
        let _ = self.sender.try_send(batch);
    }

    fn schedule(&mut self) {
        if self.partitions.is_empty() {
            return;
        }
        if self.auto_commit {
            self.schedule_on_demand();
            return;
        }
        if self.waits_for_readiness() {
            self.schedule_ready();
            return;
        }
        let start = self.cursor.map_or(0, |cursor| {
            self.partitions.partition_point(|id| *id <= cursor)
        });
        for index in 0..self.partitions.len() {
            if self.tasks.len() >= MAX_DEFERRED_REQUESTS_PER_CONSUMER {
                break;
            }
            let partition = self.partitions[(start + index) % self.partitions.len()];
            if self.active.contains(&partition) || self.retry_at.contains_key(&partition) {
                continue;
            }
            let Some(capacity) = self.reserve() else {
                break;
            };
            self.spawn_fetch(partition, capacity);
        }
    }

    fn waits_for_readiness(&self) -> bool {
        waits_for_readiness(
            self.is_group,
            self.auto_commit,
            self.partitions.len(),
            self.slots,
        )
    }

    fn schedule_ready(&mut self) {
        while let Some(partition) = self.next_ready() {
            let Some(capacity) = self.reserve() else {
                break;
            };
            self.spawn_fetch(partition, capacity);
        }
        self.start_waits();
    }

    /// Every poll of this mode commits its batch. The worker fetches one batch
    /// per demand, so it never holds a committed batch that the application
    /// did not ask for. A wait commits nothing, so the waits run without
    /// demand.
    fn schedule_on_demand(&mut self) {
        if self.on_demand.demand.load(ORDERING) && self.tasks.is_empty() {
            let partition = if self.is_group {
                self.next_ready()
            } else {
                self.partitions.first().copied()
            };
            if let Some(partition) =
                partition.filter(|partition| !self.retry_at.contains_key(partition))
                && let Some(capacity) = self.reserve()
            {
                self.spawn_fetch(partition, capacity);
            }
        }
        self.start_waits();
    }

    fn next_ready(&self) -> Option<u32> {
        let after = self.cursor.map_or(Unbounded, Excluded);
        self.on_demand
            .ready
            .range((after, Unbounded))
            .chain(&self.on_demand.ready)
            .copied()
            .find(|partition| {
                !self.retry_at.contains_key(partition) && !self.active.contains(partition)
            })
    }

    /// One readiness wait per group covers the idle partitions of the group:
    /// those that are not ready, not fetched and not retrying. A wait takes
    /// no partitions after it starts, so a partition that becomes idle
    /// restarts the wait of its group. A second wait would hold one more of
    /// the deferred requests that the fetches of the consumer share.
    fn start_waits(&mut self) {
        let idle = |partition: &&u32| {
            !self.on_demand.ready.contains(*partition)
                && !self.retry_at.contains_key(*partition)
                && !self.active.contains(*partition)
        };
        for group in &self.on_demand.groups {
            if group
                .iter()
                .filter(idle)
                .all(|partition| self.on_demand.waiting.contains_key(partition))
            {
                continue;
            }
            for partition in group {
                if let Some(wait) = self.on_demand.waiting.remove(partition) {
                    wait.abort();
                }
            }
            let targets: Vec<_> = group
                .iter()
                .filter(idle)
                .map(|partition| (*partition, self.wait_target(*partition)))
                .collect();
            let partitions: Vec<_> = targets.iter().map(|(partition, _)| *partition).collect();
            let client = self.client.clone();
            let stream = Arc::clone(&self.stream);
            let topic = Arc::clone(&self.topic);
            let consumer = Arc::clone(&self.consumer);
            let options = self.options;
            let wait = self.on_demand.waits.spawn(DEFERRED_CONSUMER_CAPACITY.scope(
                Arc::clone(&self.request_capacity),
                async move {
                    let result =
                        tokio::time::timeout(options.request_timeout.get_duration(), async {
                            let client = client.read().await;
                            client
                                .wait_for_messages(&stream, &topic, &targets, &consumer, options)
                                .await
                        })
                        .await
                        .unwrap_or(Err(IggyError::TransientNotAccepted))
                        // A wait changes nothing on the server, so a lost reply only retries.
                        .map_err(|error| match error {
                            IggyError::TransientNotCommitted => IggyError::TransientNotAccepted,
                            error => error,
                        });
                    CompletedWait {
                        partitions: targets
                            .into_iter()
                            .map(|(partition, _)| partition)
                            .collect(),
                        result,
                    }
                },
            ));
            self.on_demand.waiting.extend(
                partitions
                    .into_iter()
                    .map(|partition| (partition, wait.clone())),
            );
        }
    }

    /// A stored offset at or after the position moves it past that offset, so
    /// the application can skip a message that never reads, such as one that
    /// does not decrypt.
    fn position(&self, partition: u32) -> PollingStrategy {
        let position = self
            .positions
            .get(&partition)
            .copied()
            .unwrap_or(self.strategy);
        let stored = self.stored.get(&partition).map(|entry| *entry.value());
        match (position.kind, position.context, stored) {
            (PollingKind::Offset, Some(context), Some(stored))
                if same_incarnation_and_owner(context, stored.context)
                    && stored.offset >= position.value =>
            {
                PollingStrategy::offset(stored.offset.saturating_add(1)).with_context(context)
            }
            _ => position,
        }
    }

    /// A readiness wait takes an offset or `Next`. Before the first batch, a
    /// `First`, `Last` or `Timestamp` start waits for any message after the
    /// ones that a timestamp skipped.
    fn wait_target(&self, partition: u32) -> PollingStrategy {
        let strategy = self.position(partition);
        match strategy.kind {
            PollingKind::Next | PollingKind::Offset => strategy,
            _ => PollingStrategy::offset(
                self.on_demand
                    .floors
                    .get(&partition)
                    .copied()
                    .unwrap_or_default(),
            ),
        }
    }

    fn spawn_fetch(&mut self, partition: u32, capacity: FetchCapacity) {
        let client = self.client.clone();
        let stream = Arc::clone(&self.stream);
        let topic = Arc::clone(&self.topic);
        let consumer = Arc::clone(&self.consumer);
        let strategy = self.position(partition);
        // The record alone, without a byte limit, as a plain poll reads it.
        let (count, options) = if self.oversized.contains(&partition) {
            let options = iggy_common::DeferredPollOptions {
                min_count: 1,
                max_bytes: u32::MAX,
                ..self.options
            };
            (1, options)
        } else {
            (self.count, self.options)
        };
        let auto_commit = self.auto_commit;
        let commit = auto_commit.then(|| PendingCommit {
            partition,
            consumed: Arc::clone(&self.consumed),
            stored: Arc::clone(&self.stored),
            _batches: self.sender.clone(),
            settled: false,
        });
        let encryptor = self.encryptor.clone();
        let epoch = self.epoch;
        self.tasks.spawn(DEFERRED_CONSUMER_CAPACITY.scope(
            Arc::clone(&self.request_capacity),
            async move {
            let started = Instant::now();
            let result = tokio::time::timeout(options.request_timeout.get_duration(), async {
                let client = client.read().await;
                let mut result = client
                    .poll_messages_deferred(
                        &stream,
                        &topic,
                        Some(partition),
                        &consumer,
                        &strategy,
                        count,
                        auto_commit,
                        options.remaining(started.elapsed())?,
                    )
                    .await?;
                if result.count > count || result.messages.len() > count as usize {
                    return Err(IggyError::InvalidMessagesCount);
                }
                if let Some(encryptor) = encryptor {
                    for message in &mut result.messages {
                        let offset = message.header.offset;
                        decrypt(&encryptor, message).inspect_err(|error| {
                            error!(
                                "Failed to decrypt the message at offset: {offset}, partition ID: {partition}. Storing its offset skips it. {error}"
                            );
                        })?;
                    }
                }
                Ok(result)
            })
            .await
            // A fetch that commits nothing leaves nothing behind its timeout.
            .unwrap_or(Err(if auto_commit {
                IggyError::TransientNotCommitted
            } else {
                IggyError::TransientNotAccepted
            }));
            CompletedPoll {
                epoch,
                partition,
                strategy,
                result,
                capacity,
                commit,
            }
            },
        ));
        self.active.insert(partition);
        self.cursor = Some(partition);
    }

    fn complete(&mut self, mut completed: CompletedPoll) {
        self.active.remove(&completed.partition);
        // A reply that never arrived can commit a batch that never reaches the
        // application. So can a reply that the worker drops for holding more
        // messages than it asked for.
        if !matches!(
            completed.result,
            Err(IggyError::TransientNotCommitted | IggyError::InvalidMessagesCount)
        ) && let Some(commit) = completed.commit.take()
        {
            commit.settle();
        }
        // A record above `max_bytes` would stall its partition, so the next
        // fetch reads it alone. Only a failure of that fetch is reported.
        if !self.oversized.remove(&completed.partition)
            && matches!(completed.result, Err(IggyError::InvalidSizeBytes))
            && completed.epoch == self.epoch
            && self.epoch == self.generation.load(ORDERING)
        {
            self.oversized.insert(completed.partition);
            return;
        }
        if self.auto_commit {
            self.complete_on_demand(completed);
            return;
        }
        if self.epoch != self.generation.load(ORDERING) {
            return;
        }
        self.positions
            .insert(completed.partition, completed.strategy);
        if let Ok(result) = &completed.result {
            if result.partition_id == iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL {
                self.invalidate();
                return;
            }
            self.track_readiness(completed.partition, completed.strategy, result);
            if let Some(last) = result.messages.last() {
                match last.header.offset.checked_add(1) {
                    Some(next) => {
                        self.positions.insert(
                            completed.partition,
                            PollingStrategy::offset(next).with_context(result.context),
                        );
                    }
                    None => {
                        completed.result = Err(IggyError::InvalidOffset(last.header.offset));
                    }
                }
            } else {
                return;
            }
        }
        if let Err(error) = completed.result {
            if matches!(
                error,
                IggyError::HistoryUnavailable | IggyError::ConsumerGroupPartitionNotOwned(..)
            ) {
                self.positions.remove(&completed.partition);
            }
            if matches!(
                error,
                IggyError::ConsumerGroupMemberNotFound(..)
                    | IggyError::Disconnected
                    | IggyError::Unauthenticated
                    | IggyError::StaleClient
            ) {
                if self.is_group {
                    self.invalidate();
                }
                self.retry_at
                    .insert(completed.partition, Instant::now() + self.retry_interval);
                self.report(error);
                return;
            }
            self.retry_at
                .insert(completed.partition, Instant::now() + self.retry_interval);
            completed.result = Err(error);
        }
        self.deliver(PrefetchedBatch {
            generation: self.epoch,
            assignment: self.assignment,
            result: completed.result,
            capacity: completed.capacity,
        });
    }

    /// The server committed a batch that holds messages, so the batch reaches
    /// the application even after the fetch state was cleared.
    fn complete_on_demand(&mut self, completed: CompletedPoll) {
        let current = completed.epoch == self.epoch && self.epoch == self.generation.load(ORDERING);
        let partition = completed.partition;
        let polled = match completed.result {
            Ok(polled) => polled,
            Err(error) => {
                if current {
                    if matches!(
                        error,
                        IggyError::HistoryUnavailable
                            | IggyError::ConsumerGroupPartitionNotOwned(..)
                    ) {
                        self.positions.remove(&partition);
                    }
                    self.fail_on_demand(&[partition], error);
                }
                return;
            }
        };
        let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
            if current {
                if polled.partition_id == iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL {
                    self.invalidate();
                } else {
                    self.track_readiness(partition, completed.strategy, &polled);
                }
            }
            return;
        };
        let mut overflow = None;
        // A new assignment replaces the position that the fetch started from.
        // `Next` continues from an explicit offset too, because the interval
        // commit can move the server offset back to the delivered message.
        if completed.strategy == self.position(partition) {
            match last.checked_add(1) {
                Some(next) => {
                    self.positions.insert(
                        partition,
                        PollingStrategy::offset(next).with_context(polled.context),
                    );
                }
                None => overflow = Some(IggyError::InvalidOffset(last)),
            }
        }
        if current {
            self.track_readiness(partition, completed.strategy, &polled);
        }
        self.deliver(PrefetchedBatch {
            generation: self.epoch,
            assignment: self.assignment,
            result: Ok(polled),
            capacity: completed.capacity,
        });
        if let Some(error) = overflow {
            self.fail_on_demand(&[partition], error);
        }
    }

    /// The reply carries the commit offset, so a partition that holds more
    /// messages stays ready without another wait.
    fn track_readiness(
        &mut self,
        partition: u32,
        strategy: PollingStrategy,
        polled: &PolledMessages,
    ) {
        match polled.messages.last() {
            Some(last) => {
                self.on_demand.floors.remove(&partition);
                if last.header.offset >= polled.current_offset {
                    self.on_demand.ready.remove(&partition);
                }
            }
            None => {
                self.on_demand.ready.remove(&partition);
                if strategy.kind == PollingKind::Timestamp {
                    self.on_demand
                        .floors
                        .insert(partition, polled.current_offset.saturating_add(1));
                }
            }
        }
    }

    fn complete_wait(&mut self, completed: CompletedWait) {
        for partition in &completed.partitions {
            self.on_demand.waiting.remove(partition);
        }
        match completed.result {
            Ok(ready) => self.on_demand.ready.extend(
                ready
                    .into_iter()
                    .filter(|partition| completed.partitions.contains(partition)),
            ),
            // A wait does not name the partition whose history is gone, so
            // each partition fetches alone and its own poll tells.
            Err(IggyError::HistoryUnavailable) => {
                self.on_demand.ready.extend(&completed.partitions);
            }
            Err(error) => self.fail_on_demand(&completed.partitions, error),
        }
    }

    /// Membership and connection errors clear the fetch state. Other errors
    /// make the partitions retry later, and the application sees them.
    fn fail_on_demand(&mut self, partitions: &[u32], error: IggyError) {
        match error {
            IggyError::ConsumerGroupPartitionNotOwned(..) => self.invalidate(),
            IggyError::ConsumerGroupMemberNotFound(..)
            | IggyError::Disconnected
            | IggyError::Unauthenticated
            | IggyError::StaleClient => {
                self.invalidate();
                self.report(error);
            }
            error => {
                let retry_at = Instant::now() + self.retry_interval;
                for partition in partitions {
                    self.retry_at.insert(*partition, retry_at);
                }
                self.report(error);
            }
        }
    }
}

fn decrypt(encryptor: &EncryptorKind, message: &mut IggyMessage) -> Result<(), IggyError> {
    message.payload = Bytes::from(encryptor.decrypt(&message.payload)?);
    message.header.payload_length = message.payload.len() as u32;
    if let Some(headers) = &message.user_headers {
        let headers = encryptor.decrypt(headers)?;
        message.header.user_headers_length = headers.len() as u32;
        message.user_headers = Some(Bytes::from(headers));
    }
    Ok(())
}

/// A partition without a known message waits in a readiness wait when every
/// poll commits, or when idle partitions could hold every fetch slot for
/// `max_wait`.
fn waits_for_readiness(is_group: bool, auto_commit: bool, partitions: usize, slots: usize) -> bool {
    is_group && (auto_commit || partitions > slots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::consumer::tests::{builder_for, message_at};
    use crate::clients::consumer::{AutoCommit, AutoCommitWhen};
    use crate::clients::consumer_builder::IggyConsumerBuilder;
    use crate::tcp::tcp_client::TcpClient;
    use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
    use iggy_common::{Consumer, DeferredPollOptions, Identifier, IggyTimestamp, PartitionContext};

    #[tokio::test(start_paused = true)]
    async fn byte_and_message_reservations_cover_inflight_and_queued_replies() {
        const BATCH_COUNT: u32 = 10;
        const REPLY_BYTES: u32 = 1024;
        for (bytes, messages) in [
            (REPLY_BYTES * 2, BATCH_COUNT * 10),
            (REPLY_BYTES * 10, BATCH_COUNT * 2),
        ] {
            let consumer = IggyConsumerBuilder::new(
                IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
                "reader".into(),
                Consumer::default(),
                Identifier::numeric(1).unwrap(),
                Identifier::numeric(1).unwrap(),
                None,
                None,
            )
            .batch_length(BATCH_COUNT)
            .poll_options(DeferredPollOptions {
                max_bytes: REPLY_BYTES,
                ..Default::default()
            })
            .prefetch_bytes(bytes)
            .prefetch_messages(messages)
            .build();
            let client = consumer.client.clone();
            let _blocked_connection = client.write().await;
            let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
            let mut worker = Worker::new(&consumer, sender);
            worker.partitions = vec![0, 1, 2, 3];
            worker.schedule();
            assert_eq!(worker.tasks.len(), 2, "reserve before sending");
            assert_eq!(worker.capacity.available_permits(), 0);
            while let Some(completed) = worker.tasks.join_next().await {
                let mut completed = completed.unwrap();
                assert!(
                    matches!(completed.result, Err(IggyError::TransientNotAccepted)),
                    "a timed-out fetch that commits nothing was not accepted"
                );
                completed.result = Err(IggyError::CannotReadMessage);
                worker.complete(completed);
            }
            assert_eq!(
                receiver.len(),
                2,
                "completed failures retain their bounded queue reservations"
            );
            worker.schedule();
            assert!(
                worker.tasks.is_empty(),
                "a stalled application stops new fetches"
            );
            drop(receiver.recv().await.unwrap());
            worker.retry_at.clear();
            worker.schedule();
            assert_eq!(
                worker.tasks.len(),
                1,
                "draining releases exactly one full-batch reservation"
            );
            while let Some(completed) = worker.tasks.join_next().await {
                let mut completed = completed.unwrap();
                completed.result = Err(IggyError::CannotReadMessage);
                worker.complete(completed);
            }
            worker.invalidate();
            worker.report(IggyError::Disconnected);
            assert!(
                worker.pending_error.is_some(),
                "retain control errors under backpressure"
            );
            drop(receiver.recv().await.unwrap());
            worker.flush_error();
            assert_ne!(receiver.recv().await.unwrap().generation, worker.epoch);
            let error = receiver.recv().await.unwrap();
            assert_eq!(error.generation, worker.epoch);
            assert!(matches!(error.result, Err(IggyError::Disconnected)));
        }
    }

    #[tokio::test]
    async fn refused_polls_reach_the_application_and_retry_later() {
        let consumer = IggyConsumerBuilder::new(
            IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
            "reader".into(),
            Consumer::default(),
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
            None,
            None,
        )
        .build();
        let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
        let mut worker = Worker::new(&consumer, sender);
        for error in [
            IggyError::TransientNotAccepted,
            IggyError::DeferredPollCapacityExceeded,
        ] {
            let capacity = worker.reserve().unwrap();
            worker.complete(CompletedPoll {
                epoch: worker.epoch,
                partition: 0,
                strategy: worker.strategy,
                result: Err(error.clone()),
                capacity,
                commit: None,
            });
            assert!(worker.retry_at.contains_key(&0), "the partition retries");
            assert!(
                matches!(receiver.try_recv().unwrap().result, Err(ref fetched) if *fetched == error),
                "a refused fetch reaches the application"
            );
            worker.report(error.clone());
            assert!(
                matches!(receiver.try_recv().unwrap().result, Err(ref reported) if *reported == error),
                "a refused wait or refresh reaches the application"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn committing_fetch_waits_for_demand_and_survives_invalidation() {
        let (mut worker, mut receiver) =
            on_demand_worker(Consumer::default(), PollingStrategy::next());
        let client = worker.client.clone();
        let _blocked_connection = client.write().await;
        worker.partitions = vec![0];
        worker.schedule();
        assert!(
            worker.tasks.is_empty(),
            "no fetch before the application asks"
        );
        worker.on_demand.demand.store(true, ORDERING);
        worker.schedule();
        worker.schedule();
        assert_eq!(worker.tasks.len(), 1, "one committing fetch at a time");
        let started = worker.epoch;
        worker.invalidate();
        let completed = worker.tasks.join_next().await.unwrap().unwrap();
        assert_eq!(completed.epoch, started, "the fetch survives invalidation");
        assert!(matches!(
            completed.result,
            Err(IggyError::TransientNotCommitted)
        ));
        worker.complete(completed);
        assert!(receiver.is_empty(), "an invalidated fetch drops its error");
        assert!(worker.retry_at.is_empty());

        worker.complete(fetched(
            &worker,
            started,
            PollingStrategy::next(),
            0,
            [3, 4],
            9,
        ));
        let batch = receiver.try_recv().unwrap();
        assert_eq!(
            batch.result.unwrap().messages.len(),
            2,
            "the server committed the batch, so the application gets it"
        );
        assert!(
            !worker.on_demand.demand.load(ORDERING),
            "the batch answers the demand"
        );

        let position = worker.position(0);
        assert_eq!(
            position,
            PollingStrategy::offset(5).with_context(Default::default()),
            "an interval commit that moves the server offset back is not read again"
        );
        worker.complete(fetched(&worker, worker.epoch, position, 0, [5], 9));
        assert!(receiver.try_recv().is_ok());
        assert_eq!(
            worker.position(0),
            PollingStrategy::offset(6).with_context(Default::default())
        );
    }

    #[tokio::test]
    async fn readiness_follows_the_commit_offset_and_the_timestamp_floor() {
        let start = PollingStrategy::timestamp(IggyTimestamp::from(1));
        let (mut worker, _receiver) =
            on_demand_worker(Consumer::group(Identifier::numeric(1).unwrap()), start);
        worker.partitions = vec![0, 1];
        worker.on_demand.ready.extend([0, 1]);
        let epoch = worker.epoch;

        worker.complete(fetched(&worker, epoch, start, 0, [5, 6], 9));
        assert_eq!(
            worker.positions[&0],
            PollingStrategy::offset(7).with_context(Default::default())
        );
        assert!(
            worker.on_demand.ready.contains(&0),
            "offsets 7 to 9 are committed and not read yet"
        );
        worker.complete(fetched(&worker, epoch, worker.position(0), 0, [7, 8, 9], 9));
        assert!(!worker.on_demand.ready.contains(&0));
        assert_eq!(
            worker.wait_target(0),
            PollingStrategy::offset(10).with_context(Default::default())
        );

        worker.complete(fetched(&worker, epoch, start, 1, [], 4));
        assert!(!worker.on_demand.ready.contains(&1));
        assert_eq!(
            worker.wait_target(1),
            PollingStrategy::offset(5),
            "wait for a message after the ones that the timestamp skipped"
        );
        worker.complete(fetched(&worker, epoch, start, 1, [5], 5));
        assert!(worker.on_demand.floors.is_empty());
        assert_eq!(
            worker.wait_target(1),
            PollingStrategy::offset(6).with_context(Default::default())
        );

        let mut fenced = fetched(&worker, epoch, PollingStrategy::offset(6), 1, [], 5);
        fenced.result.as_mut().unwrap().partition_id =
            iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL;
        worker.complete(fenced);
        assert_ne!(worker.epoch, epoch, "a fenced assignment refreshes");
        assert!(
            worker.partitions.is_empty(),
            "nothing is fetched before the refresh confirms the assignment"
        );
    }

    #[tokio::test]
    async fn oversized_record_is_fetched_alone_before_its_error_is_reported() {
        for auto_commit in [
            AutoCommit::Disabled,
            AutoCommit::When(AutoCommitWhen::PollingMessages),
        ] {
            let consumer = builder_for(Consumer::default())
                .auto_commit(auto_commit)
                .build();
            let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
            let mut worker = Worker::new(&consumer, sender);
            for reported in [false, true] {
                worker.complete(CompletedPoll {
                    epoch: worker.epoch,
                    partition: 0,
                    strategy: worker.strategy,
                    result: Err(IggyError::InvalidSizeBytes),
                    capacity: worker.reserve().unwrap(),
                    commit: None,
                });
                assert_eq!(worker.oversized.contains(&0), !reported);
                assert_eq!(worker.retry_at.contains_key(&0), reported);
                assert_eq!(
                    receiver.try_recv().is_ok(),
                    reported,
                    "only the fetch without a byte limit reports the error"
                );
            }
        }
    }

    #[tokio::test]
    async fn new_generation_resumes_after_the_last_delivered_message() {
        let consumer = builder_for(Consumer::default())
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .build();
        let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
        let mut worker = Worker::new(&consumer, sender);
        assert!(worker.apply(worker.read_assignment().await.unwrap()));
        let partition = worker.partitions[0];
        for (delivered, resumed) in [
            (None, PollingStrategy::first()),
            (
                Some(0),
                PollingStrategy::offset(1).with_context(Default::default()),
            ),
        ] {
            worker.complete(fetched(
                &worker,
                worker.epoch,
                PollingStrategy::first(),
                partition,
                [0, 1, 2],
                9,
            ));
            assert!(receiver.try_recv().is_ok());
            if let Some(offset) = delivered {
                consumer.state.last_consumed_offsets.insert(
                    partition,
                    ConsumerPosition {
                        partition_id: partition,
                        offset,
                        context: Default::default(),
                    },
                );
            }
            let stale = worker.read_assignment().await.unwrap();
            worker.invalidate();
            assert!(
                !worker.apply(stale),
                "a refresh from before the new generation is read again"
            );
            assert!(worker.partitions.is_empty());
            assert!(worker.apply(worker.read_assignment().await.unwrap()));
            assert_eq!(worker.partitions, [partition]);
            assert_eq!(
                worker.position(partition),
                resumed,
                "the batch that the application did not take is fetched again"
            );
        }
    }

    #[tokio::test]
    async fn rebalance_keeps_the_position_of_a_partition_that_stays_with_the_member() {
        let consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .build();
        let (sender, _receiver) = mpsc::channel(consumer.prefetch_slots());
        let mut worker = Worker::new(&consumer, sender);
        assert!(worker.apply(assigned(&worker, 1, 1, [0, 1, 2])));
        for partition in [0, 2] {
            worker.complete(fetched(
                &worker,
                worker.epoch,
                PollingStrategy::first(),
                partition,
                [0, 1, 2],
                9,
            ));
            consumer.state.last_consumed_offsets.insert(
                partition,
                ConsumerPosition {
                    partition_id: partition,
                    offset: 0,
                    context: Default::default(),
                },
            );
        }

        assert!(worker.apply(assigned(&worker, 1, 2, [0, 1])));
        assert_eq!(
            worker.position(0),
            PollingStrategy::offset(1).with_context(Default::default()),
            "the partition continues after its last delivered message"
        );
        assert!(
            !worker.positions.contains_key(&2),
            "a partition that left starts over when it comes back"
        );

        assert!(worker.apply(assigned(&worker, 2, 2, [0, 1])));
        assert_eq!(
            worker.position(0),
            PollingStrategy::first(),
            "a new session owns every partition anew"
        );
    }

    #[tokio::test]
    async fn committing_member_retries_a_lost_reply_until_the_partition_changes_owner() {
        let (mut worker, _receiver) = on_demand_worker(
            Consumer::group(Identifier::numeric(1).unwrap()),
            PollingStrategy::next(),
        );
        assert!(worker.apply(assigned(&worker, 1, 1, [0, 1])));
        worker.complete(fetched(
            &worker,
            worker.epoch,
            PollingStrategy::next(),
            0,
            [3, 4],
            9,
        ));
        let retried = worker.position(0);
        worker.complete(CompletedPoll {
            epoch: worker.epoch,
            partition: 0,
            strategy: retried,
            result: Err(IggyError::TransientNotCommitted),
            capacity: worker.reserve().unwrap(),
            commit: None,
        });
        assert!(worker.retry_at.contains_key(&0));

        assert!(worker.apply(assigned(&worker, 1, 2, [0])));
        assert_eq!(
            worker.position(0),
            retried,
            "`Next` would start after the batch that the server may have committed"
        );

        worker.complete(CompletedPoll {
            epoch: worker.epoch,
            partition: 0,
            strategy: retried,
            result: Err(IggyError::ConsumerGroupPartitionNotOwned(1, 0)),
            capacity: worker.reserve().unwrap(),
            commit: None,
        });
        assert!(worker.apply(assigned(&worker, 1, 2, [0])));
        assert_eq!(
            worker.position(0),
            PollingStrategy::next(),
            "a partition that moved away and back resolves its position again"
        );
    }

    #[tokio::test]
    async fn stored_offset_moves_the_fetch_position_past_a_message_that_never_reads() {
        let consumer = builder_for(Consumer::default())
            .auto_commit(AutoCommit::Disabled)
            .build();
        let (sender, _receiver) = mpsc::channel(consumer.prefetch_slots());
        let mut worker = Worker::new(&consumer, sender);
        let context = PartitionContext {
            incarnation: 7,
            ..Default::default()
        };
        let stuck = PollingStrategy::offset(5).with_context(context);
        worker.positions.insert(0, stuck);
        let store = |offset, context| {
            consumer.state.last_stored_offsets.insert(
                0,
                ConsumerPosition {
                    partition_id: 0,
                    offset,
                    context,
                },
            );
        };

        store(4, context);
        assert_eq!(worker.position(0), stuck, "a commit behind the position");
        store(
            9,
            PartitionContext {
                incarnation: 8,
                ..Default::default()
            },
        );
        assert_eq!(worker.position(0), stuck, "a commit in another incarnation");
        store(5, context);
        assert_eq!(
            worker.position(0),
            PollingStrategy::offset(6).with_context(context)
        );
    }

    #[tokio::test]
    async fn readiness_waits_cover_each_endpoint_once_and_retry_after_errors() {
        let (mut worker, mut receiver) = on_demand_worker(
            Consumer::group(Identifier::numeric(1).unwrap()),
            PollingStrategy::next(),
        );
        let client = worker.client.clone();
        let _blocked_connection = client.write().await;
        worker.partitions = vec![0, 1, 2];
        worker.on_demand.groups = vec![vec![0, 2], vec![1]];
        worker.on_demand.demand.store(true, ORDERING);
        worker.schedule();
        worker.schedule();
        assert_eq!(
            worker.on_demand.waits.len(),
            2,
            "one wait per primary endpoint"
        );
        assert!(worker.tasks.is_empty(), "no partition is known to be ready");

        worker.complete_wait(CompletedWait {
            partitions: vec![0, 2],
            result: Ok(vec![2, 5]),
        });
        assert_eq!(
            worker.on_demand.ready,
            BTreeSet::from([2]),
            "a reply marks only the partitions that the wait named"
        );
        worker.complete_wait(CompletedWait {
            partitions: vec![1],
            result: Err(IggyError::TransientNotAccepted),
        });
        assert!(worker.retry_at.contains_key(&1), "the partition retries");
        assert!(worker.pending_error.is_none());
        assert!(
            matches!(
                receiver.try_recv().unwrap().result,
                Err(IggyError::TransientNotAccepted)
            ),
            "the application sees a refused wait"
        );
        // The application asks again after it took the error.
        worker.on_demand.demand.store(true, ORDERING);

        worker.schedule();
        assert!(
            worker.active.contains(&2),
            "a ready partition is fetched without another wait"
        );

        worker.complete_wait(CompletedWait {
            partitions: vec![0],
            result: Err(IggyError::InvalidCommand),
        });
        assert!(matches!(
            receiver.try_recv().unwrap().result,
            Err(IggyError::InvalidCommand)
        ));

        let epoch = worker.epoch;
        worker.complete_wait(CompletedWait {
            partitions: vec![0, 2],
            result: Err(IggyError::ConsumerGroupPartitionNotOwned(1, 0)),
        });
        assert_ne!(
            worker.epoch, epoch,
            "a moved partition refreshes the assignment"
        );
        assert!(worker.on_demand.groups.is_empty());
        assert!(worker.on_demand.waits.is_empty());
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn committing_member_watches_idle_partitions_while_it_fetches_a_backlog() {
        let (mut worker, _receiver) = on_demand_worker(
            Consumer::group(Identifier::numeric(1).unwrap()),
            PollingStrategy::next(),
        );
        let client = worker.client.clone();
        let _blocked_connection = client.write().await;
        worker.partitions = vec![0, 1];
        worker.on_demand.groups = vec![vec![0, 1]];
        worker.on_demand.ready.insert(0);
        worker.on_demand.demand.store(true, ORDERING);
        worker.schedule();
        assert!(worker.active.contains(&0));
        let watching = worker.on_demand.waiting.get(&1).map(AbortHandle::id);
        assert!(
            watching.is_some(),
            "a backlog on one partition does not hide new messages on another"
        );

        let epoch = worker.epoch;
        worker.complete(fetched(
            &worker,
            epoch,
            PollingStrategy::next(),
            0,
            [5, 6],
            9,
        ));
        worker.schedule();
        assert_eq!(
            worker.on_demand.waiting.get(&1).map(AbortHandle::id),
            watching,
            "the wait runs on while no partition becomes idle"
        );

        worker.complete(fetched(&worker, epoch, worker.position(0), 0, [7, 8, 9], 9));
        worker.schedule();
        let restarted = worker.on_demand.waiting.get(&0).map(AbortHandle::id);
        assert!(
            restarted.is_some_and(|wait| worker.on_demand.covers(wait, &[0, 1])),
            "a drained partition restarts the wait of its group before the application asks"
        );
        let replaced = worker.on_demand.waits.join_next_with_id().await.unwrap();
        assert!(
            replaced.is_err_and(|error| error.is_cancelled() && Some(error.id()) == watching),
            "the restart cancels the wait that it replaces"
        );
        assert_eq!(worker.on_demand.waits.len(), 1, "one wait per group");
        assert!(
            watching.is_some_and(|wait| !worker.on_demand.covers(wait, &[1])),
            "a reply of the replaced wait is stale"
        );
    }

    #[tokio::test]
    async fn changed_readiness_groups_restart_every_wait() {
        let (mut worker, _receiver) = on_demand_worker(
            Consumer::group(Identifier::numeric(1).unwrap()),
            PollingStrategy::next(),
        );
        let client = worker.client.clone();
        let _blocked_connection = client.write().await;
        let (epoch, assignment) = (worker.epoch, AssignmentVersion::default());
        worker.assignment = Some(assignment);
        let refreshed = |groups| Refreshed {
            epoch,
            assignment,
            partitions: vec![0, 1],
            groups: Some(groups),
        };
        assert!(worker.apply(refreshed(vec![vec![0, 1]])));
        worker.schedule();
        let watching = worker.on_demand.waiting.get(&0).map(AbortHandle::id);
        assert!(worker.apply(refreshed(vec![vec![0, 1]])));
        worker.schedule();
        assert!(
            watching.is_some_and(|wait| worker.on_demand.covers(wait, &[0, 1])),
            "a refresh that keeps the groups keeps the waits"
        );

        assert!(worker.apply(refreshed(vec![vec![0], vec![1]])));
        worker.schedule();
        let waits: BTreeSet<_> = worker
            .on_demand
            .waiting
            .values()
            .map(AbortHandle::id)
            .collect();
        assert_eq!(
            (worker.on_demand.waiting.len(), waits.len()),
            (2, 2),
            "each wait covers one group, so a restart of one group leaves the other watched"
        );
    }

    #[tokio::test]
    async fn more_partitions_than_fetch_slots_fetch_only_partitions_that_hold_messages() {
        let consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let (sender, _receiver) = mpsc::channel(consumer.prefetch_slots());
        let mut worker = Worker::new(&consumer, sender);
        let client = worker.client.clone();
        let _blocked_connection = client.write().await;
        let partitions: Vec<u32> = (0..=u32::try_from(worker.slots).unwrap()).collect();
        worker.partitions = partitions.clone();
        worker.on_demand.groups = vec![partitions.clone()];
        worker.schedule();
        assert!(worker.tasks.is_empty(), "an idle partition holds no slot");
        assert_eq!(worker.on_demand.waits.len(), 1);

        worker.complete_wait(CompletedWait {
            partitions: partitions.clone(),
            result: Ok(vec![3]),
        });
        worker.schedule();
        assert_eq!(worker.active, BTreeSet::from([3]));
        assert_eq!(
            worker.on_demand.waiting.len(),
            partitions.len() - 1,
            "the next wait covers every other partition"
        );

        let epoch = worker.epoch;
        worker.complete(fetched(
            &worker,
            epoch,
            PollingStrategy::next(),
            3,
            [5, 6],
            9,
        ));
        worker.schedule();
        assert!(
            worker.active.contains(&3),
            "a partition that holds more messages is fetched again without a wait"
        );
        worker.complete(fetched(&worker, epoch, worker.position(3), 3, [7, 8, 9], 9));
        assert!(!worker.on_demand.ready.contains(&3));
        worker.schedule();
        let restarted = worker.on_demand.waiting.get(&3).map(AbortHandle::id);
        assert!(
            restarted.is_some_and(|wait| worker.on_demand.covers(wait, &partitions)),
            "a drained partition restarts the wait of its group at once"
        );
    }

    #[tokio::test]
    async fn group_member_should_drop_only_the_continuation_of_the_refused_partition() {
        for error in [
            IggyError::HistoryUnavailable,
            IggyError::ConsumerGroupPartitionNotOwned(1, 3),
        ] {
            let consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
                .auto_commit(AutoCommit::Disabled)
                .build();
            let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
            let mut worker = Worker::new(&consumer, sender);
            worker.positions.insert(3, PollingStrategy::offset(11));
            worker.positions.insert(4, PollingStrategy::offset(21));
            worker.complete(CompletedPoll {
                epoch: worker.epoch,
                partition: 3,
                strategy: PollingStrategy::offset(11),
                result: Err(error.clone()),
                capacity: worker.reserve().unwrap(),
                commit: None,
            });
            assert_eq!(receiver.recv().await.unwrap().result.unwrap_err(), error);
            assert!(!worker.positions.contains_key(&3));
            assert_eq!(worker.positions.get(&4), Some(&PollingStrategy::offset(21)));
        }
    }

    #[tokio::test]
    async fn committing_worker_should_drop_only_the_continuation_of_a_partition_without_history() {
        let (mut worker, mut receiver) = on_demand_worker(
            Consumer::new(Identifier::numeric(1).unwrap()),
            PollingStrategy::first(),
        );
        worker.positions.insert(3, PollingStrategy::offset(11));
        worker.positions.insert(4, PollingStrategy::offset(21));
        worker.complete(CompletedPoll {
            epoch: worker.epoch,
            partition: 3,
            strategy: PollingStrategy::offset(11),
            result: Err(IggyError::HistoryUnavailable),
            capacity: worker.reserve().unwrap(),
            commit: None,
        });
        assert!(matches!(
            receiver.recv().await.unwrap().result,
            Err(IggyError::HistoryUnavailable)
        ));
        assert!(!worker.positions.contains_key(&3));
        assert!(worker.retry_at.contains_key(&3));

        worker.complete_wait(CompletedWait {
            partitions: vec![4, 5],
            result: Err(IggyError::HistoryUnavailable),
        });
        assert_eq!(
            worker.on_demand.ready,
            BTreeSet::from([4, 5]),
            "each partition of the wait fetches alone"
        );
        assert!(receiver.is_empty(), "only a fetch reports the lost history");
        assert_eq!(worker.positions.get(&4), Some(&PollingStrategy::offset(21)));
    }

    fn on_demand_worker(
        consumer: Consumer,
        strategy: PollingStrategy,
    ) -> (Worker, mpsc::Receiver<PrefetchedBatch>) {
        let consumer = builder_for(consumer)
            .polling_strategy(strategy)
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .build();
        let (sender, receiver) = mpsc::channel(consumer.prefetch_slots());
        (Worker::new(&consumer, sender), receiver)
    }

    /// A refresh in the current fetch state that read `partitions`.
    fn assigned(
        worker: &Worker,
        session: u64,
        generation: u64,
        partitions: impl Into<Vec<u32>>,
    ) -> Refreshed {
        Refreshed {
            epoch: worker.epoch,
            assignment: AssignmentVersion {
                session,
                generation,
            },
            partitions: partitions.into(),
            groups: None,
        }
    }

    /// A fetch of `partition` that the worker started in `epoch`.
    fn fetched(
        worker: &Worker,
        epoch: u64,
        strategy: PollingStrategy,
        partition: u32,
        offsets: impl IntoIterator<Item = u64>,
        current_offset: u64,
    ) -> CompletedPoll {
        let messages: Vec<_> = offsets.into_iter().map(message_at).collect();
        CompletedPoll {
            epoch,
            partition,
            strategy,
            result: Ok(PolledMessages {
                partition_id: partition,
                context: Default::default(),
                current_offset,
                count: messages.len() as u32,
                messages,
            }),
            capacity: worker.reserve().unwrap(),
            commit: None,
        }
    }
}
