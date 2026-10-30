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
use std::sync::Arc;
use std::time::Duration;

use async_broadcast::Receiver;
use futures_util::StreamExt;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::responses::messages::FetchMessagesResponse;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    AssignmentVersion, Checkpoint, Client, CommitPolicy, DecodeError, DeferredPollOptions,
    DiagnosticEvent, EncryptorKind, FetchedSection, IggyDuration, IggyError,
    MIN_DEFERRED_POLL_MAX_BYTES, MessageClient, PollOptions, PolledMessages, PollingKind,
    PollingStrategy, UndecodedRecord,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, timeout_at};
use tracing::{error, warn};

use super::progress::{PendingCommit, same_incarnation_and_owner};
use super::{Config, FetchCapacity, ORDERING, ReadError, Refusal, Reply, Shared, first_as_offset};
use crate::driver::{
    DEFERRED_CONSUMER_CAPACITY, MAX_DEFERRED_REQUESTS_PER_CONSUMER, with_deadline,
};
use crate::links::CANCEL_COMMITTING;

const ASSIGNMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// The message limit of a probe, and the size of a section whose failure names its message.
const SINGLE_RECORD: u32 = 1;
/// A partition refused this many times in a row for a reason that does not pass by itself stops
/// until a seek.
pub(super) const MAX_LASTING_REFUSALS: u32 = 6;
/// The wait after a lasting refusal doubles at most this many times.
const MAX_REFUSAL_BACKOFF_STEPS: u32 = 5;

enum Completed {
    Poll(CompletedPoll),
    Fetch(CompletedFetch),
}

/// A poll of one partition.
struct CompletedPoll {
    /// The fetch state that started the poll.
    epoch: u64,
    kind: FetchKind,
    partition: u32,
    /// The position that the poll read from.
    strategy: PollingStrategy,
    /// The generation of the position.
    generation: u64,
    result: Result<PolledMessages, DecodeError>,
    capacity: Arc<FetchCapacity>,
    /// Set for a poll that commits its batch.
    commit: Option<PendingCommit>,
}

/// A fetch of partitions of one group.
struct CompletedFetch {
    /// The fetch state that started the fetch.
    epoch: u64,
    /// See [`Active::fetch`].
    fetch: u64,
    /// The partitions that the fetch read, each with the position that it read from and the
    /// generation of that position.
    targets: Vec<(u32, PollingStrategy, u64)>,
    /// The sections, whose records the worker decodes once the positions moved.
    result: Result<Vec<FetchedSection>, IggyError>,
    capacity: Arc<FetchCapacity>,
    /// Set for a fetch that commits its batches.
    commit: Option<PendingCommit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FetchKind {
    /// A poll of one partition.
    Alone,
    /// A poll of one partition that neither commits nor delivers. It resolves a
    /// start other than `Offset` or `Next` before a fetch that commits reads from
    /// it, or it probes a target of a fetch that the server refused for a reason
    /// that names no target.
    Check,
    /// A fetch of the idle partitions of one group.
    Main,
    /// A fetch of the partitions of one group that became idle while its main
    /// fetch parked. It commits nothing, and each new one restarts it.
    Second,
    /// A fetch of backlogged partitions of one group that commits its batches,
    /// beside the main fetch. Its reads return at once, so several may run, and
    /// it tells them from the one fetch of the group that may park, which never
    /// restarts.
    Backlog,
}

/// The poll or the fetch that reads a partition.
struct Active {
    task: AbortHandle,
    kind: FetchKind,
    /// A restart can cancel a fetch that already ended. Its reply is then stale,
    /// and the number tells it apart from the fetch that replaced it.
    fetch: u64,
}

/// The assignment that a refresh read.
struct Refreshed {
    /// The fetch state that started the refresh.
    epoch: u64,
    assignment: AssignmentVersion,
    partitions: Vec<u32>,
    /// The readiness groups. `None` on a transport without them, where each partition polls
    /// alone.
    groups: Option<Vec<Vec<u32>>>,
}

/// The fetch loop of a reader. Its reads run in `tasks`, and one refresh of the assignment and
/// the readiness groups runs apart from them.
struct RetainedBatch {
    batch: PolledMessages,
    capacity: Arc<FetchCapacity>,
}

pub(super) struct Worker {
    config: Arc<Config>,
    shared: Arc<Shared>,
    sender: mpsc::UnboundedSender<Reply>,
    events: Option<Receiver<DiagnosticEvent>>,
    capacity: Arc<Semaphore>,
    recovery_capacity: Arc<Semaphore>,
    retained_batches: BTreeMap<u32, RetainedBatch>,
    /// With `OnRead`: the operation of a committing read of each partition whose outcome is
    /// unknown, 0 when its error names none. Only while one is held does a `Next` read that
    /// starts past the expected offset read the skipped messages again. A later committing read
    /// that returns messages from the expected offset ends it: its store fences out the lost
    /// read on the server.
    lost_operations: BTreeMap<u32, u64>,
    request_capacity: Arc<Semaphore>,
    /// Turns true in the close, so each committing read in flight cancels and takes its one
    /// reply.
    cancel_committing: watch::Sender<bool>,
    tasks: JoinSet<Completed>,
    /// At most one refresh, apart from the fetches, so a slow one never holds
    /// their replies back.
    refreshing: JoinSet<Result<Refreshed, IggyError>>,
    /// The poll or the fetch that reads each partition.
    active: BTreeMap<u32, Active>,
    /// Numbers the polls and the fetches.
    fetches: u64,
    /// Partitions whose next read is a poll of one message: a fetch section refused for a first
    /// record above `max_bytes`, which a poll always returns, or a message of a section that did
    /// not decode, which the poll names.
    read_alone: BTreeSet<u32>,
    /// The end of a section that did not decode. Its partition reads alone until its position
    /// passes it, so each message of the section is read once.
    read_alone_until: BTreeMap<u32, u64>,
    /// With `OnRead`: the last offset that a lost read committed past the position of a
    /// partition. The partition reads up to it again by offset without a commit, then reads
    /// `Next` again.
    catch_up: BTreeMap<u32, u64>,
    /// With `OnRead`: the operations of lost replies, which the worker abandons because the next
    /// `Next` read finds what they committed.
    abandoning: Vec<(u64, Vec<u32>)>,
    /// Partitions that probe alone after the server refused a fetch of theirs.
    probing: BTreeSet<u32>,
    /// Partitions whose last section of their position ended before the head, so their next
    /// read returns at once.
    backlogged: BTreeSet<u32>,
    /// Partitions that read nothing until a seek.
    stopped: BTreeSet<u32>,
    /// The lasting refusals in a row of each partition.
    lasting_refusals: BTreeMap<u32, u32>,
    /// The lasting refusals in a row of the assignment refresh.
    refresh_refusals: u32,
    positions: BTreeMap<u32, PollingStrategy>,
    /// The generation of each position.
    generations: BTreeMap<u32, u64>,
    /// The position that the generation of a partition started at: that of a seek, or the
    /// first message that a `Last` or `Timestamp` start resolved to.
    seek_starts: BTreeMap<u32, PollingStrategy>,
    retry_at: BTreeMap<u32, Instant>,
    partitions: Vec<u32>,
    assignment: Option<AssignmentVersion>,
    /// The latest error of each partition, and of the whole reader, that waits for a slot.
    pending_errors: BTreeMap<Option<u32>, Refusal>,
    epoch: u64,
    cursor: Option<u32>,
    /// The group that the next group fetch starts the walk from, so every group gets a turn
    /// when there are fewer slots than groups.
    group_cursor: usize,
    /// The assigned partitions, grouped by primary endpoint and split to fit one
    /// fetch each. Empty while every partition polls alone.
    groups: Vec<Vec<u32>>,
    /// The fetch reply in which each partition last delivered messages. A fetch
    /// reads its targets in order, so the ones served last go to its end.
    served: BTreeMap<u32, u64>,
    replies: u64,
    /// Set when a primary moved or the membership ended, so the next turn refreshes.
    refresh_now: bool,
}

impl Worker {
    pub(super) fn new(
        config: Arc<Config>,
        shared: Arc<Shared>,
        sender: mpsc::UnboundedSender<Reply>,
        events: Option<Receiver<DiagnosticEvent>>,
    ) -> Self {
        // The fixed partitions of a standalone reader start their generations at once, so a
        // seek before the first refresh survives it.
        let generations = if config.is_group() {
            BTreeMap::new()
        } else {
            shared.progress.assign(&config.partitions, &[])
        };
        Self {
            capacity: Arc::new(Semaphore::new(config.slots)),
            recovery_capacity: Arc::new(Semaphore::new(1)),
            retained_batches: BTreeMap::new(),
            lost_operations: BTreeMap::new(),
            request_capacity: Arc::new(Semaphore::new(MAX_DEFERRED_REQUESTS_PER_CONSUMER)),
            cancel_committing: watch::channel(false).0,
            epoch: shared.epoch.load(ORDERING),
            config,
            shared,
            sender,
            events,
            tasks: JoinSet::new(),
            refreshing: JoinSet::new(),
            active: BTreeMap::new(),
            fetches: 0,
            read_alone: BTreeSet::new(),
            read_alone_until: BTreeMap::new(),
            catch_up: BTreeMap::new(),
            abandoning: Vec::new(),
            probing: BTreeSet::new(),
            backlogged: BTreeSet::new(),
            stopped: BTreeSet::new(),
            lasting_refusals: BTreeMap::new(),
            refresh_refusals: 0,
            positions: BTreeMap::new(),
            generations,
            seek_starts: BTreeMap::new(),
            retry_at: BTreeMap::new(),
            partitions: Vec::new(),
            assignment: None,
            pending_errors: BTreeMap::new(),
            cursor: None,
            group_cursor: 0,
            groups: Vec::new(),
            served: BTreeMap::new(),
            replies: 0,
            refresh_now: false,
        }
    }

    pub(super) async fn run(mut self) {
        if self.events.is_none() {
            self.events = Some(
                self.config
                    .client
                    .read()
                    .await
                    .subscribe_events()
                    .await
                    .new_receiver(),
            );
        }
        let mut refresh = tokio::time::interval(ASSIGNMENT_REFRESH_INTERVAL);
        refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if self.sender.is_closed() {
                return;
            }
            if self.shared.closing.load(ORDERING) {
                self.finish_fetches().await;
                // The close then stores the last delivered offset over what they committed.
                self.abandon_lost().await;
                return;
            }
            if self.epoch != self.shared.epoch.load(ORDERING) {
                self.rewind();
                self.epoch = self.shared.epoch.load(ORDERING);
                refresh.reset_immediately();
            }
            self.apply_seeks();
            if std::mem::take(&mut self.refresh_now) {
                refresh.reset_immediately();
            }
            self.abandon_lost().await;
            self.shared.progress.unblock_abandoned().await;
            let enabled = self.shared.can_poll.load(ORDERING)
                || !self.config.client.read().await.can_recover().await;
            self.flush_errors();
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
                _ = self.shared.wake.notified() => {},
                _ = tokio::time::sleep_until(retry_at), if !self.retry_at.is_empty() => {
                    self.retry_at.retain(|_, deadline| *deadline > Instant::now());
                },
                _ = refresh.tick() => {
                    if enabled && self.refreshing.is_empty() {
                        let deadline = Instant::now() + self.request_timeout();
                        self.refreshing.spawn(with_deadline(deadline, self.read_assignment()));
                    }
                },
                Some(refreshed) = self.refreshing.join_next(), if !self.refreshing.is_empty() => {
                    match refreshed {
                        Ok(Ok(refreshed)) => {
                            self.refresh_refusals = 0;
                            if self.apply(refreshed) {
                                refresh.reset();
                            } else {
                                refresh.reset_immediately();
                            }
                        }
                        Ok(Err(error)) => {
                            let delay = if lasting_refusal(&error) {
                                self.refresh_refusals = self.refresh_refusals.saturating_add(1);
                                backoff(self.config.retry_interval, self.refresh_refusals)
                            } else {
                                self.config.retry_interval
                            };
                            self.refuse_refresh(error);
                            refresh.reset_after(delay);
                        }
                        // The handle sees the closed channel and starts a new worker.
                        Err(_) => return,
                    }
                },
                Some(completed) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match completed {
                        Ok(completed) => {
                            self.complete(completed);
                            if self.partitions.is_empty() {
                                refresh.reset_after(self.config.retry_interval);
                            }
                        },
                        // A restart cancelled the fetch.
                        Err(error) if error.is_cancelled() => {}
                        Err(_) => return,
                    }
                },
                event = next_event(&mut self.events) => self.on_event(event).await,
            }
        }
    }

    /// The server can commit the batches of a committing fetch in flight, so the
    /// worker ends once those fetches hand their batches over. Nothing new starts,
    /// and every other fetch ends with the worker. Each committing read cancels,
    /// which ends one that waits on the server at once, and its one reply still
    /// tells what the server stored.
    async fn finish_fetches(&mut self) {
        if !self.config.on_read() {
            return;
        }
        for active in self.active.values() {
            if active.kind == FetchKind::Check {
                active.task.abort();
            }
        }
        self.cancel_committing.send_replace(true);
        while let Some(completed) = self.tasks.join_next().await {
            if let Ok(completed) = completed {
                self.complete(completed);
            }
        }
        drop(PendingCommit::new(
            Arc::clone(&self.shared.progress),
            self.catch_up.keys().copied().collect(),
        ));
    }

    /// Keeps reading in step with the connection, and makes the next refresh join again after
    /// the session that joined is gone.
    async fn on_event(&mut self, event: Option<DiagnosticEvent>) {
        let Some(event) = event else {
            self.events = None;
            return;
        };
        match event {
            DiagnosticEvent::Shutdown => {
                warn!("Reader client has been shut down");
                self.forget_membership();
                self.shared.can_poll.store(false, ORDERING);
                self.shared.epoch.fetch_add(1, ORDERING);
                self.events = None;
                return;
            }
            DiagnosticEvent::SignedIn => {}
            DiagnosticEvent::Disconnected => {
                warn!("Reader client disconnected from the server");
                self.forget_membership();
            }
            DiagnosticEvent::Connected | DiagnosticEvent::SignedOut => self.forget_membership(),
        }
        // Links publish from several tasks, so an event can arrive after one that reports a
        // later state. The state of the client decides instead.
        let authenticated = self.config.client.read().await.is_authenticated().await;
        self.shared.can_poll.store(authenticated, ORDERING);
        self.shared.epoch.fetch_add(1, ORDERING);
    }

    /// A new epoch discards every batch that the application did not take, so each partition
    /// continues after its last delivered message of the generation, or from the start of the
    /// generation. The server already committed the batches of a committing fetch, so that
    /// fetch finishes, delivers them and moves its positions. Nothing is fetched until the next
    /// refresh confirms the assignment.
    fn rewind(&mut self) {
        if !self.config.on_read() {
            self.tasks = JoinSet::new();
            self.active.clear();
            let partitions: Vec<u32> = self.positions.keys().copied().collect();
            for partition in partitions {
                let generation = self.generations.get(&partition).copied();
                let delivered =
                    self.shared
                        .progress
                        .last_delivered(partition)
                        .filter(|delivered| {
                            delivered.stamp().map(|stamp| stamp.generation) == generation
                        });
                let resumed = delivered.and_then(|delivered| {
                    let same_history = self
                        .positions
                        .get(&partition)
                        .and_then(|position| position.context())
                        .is_some_and(|context| {
                            same_incarnation_and_owner(context, delivered.context())
                        });
                    let next = delivered.offset().checked_add(1).filter(|_| same_history)?;
                    Some(continuation_at(partition, next, delivered.context()))
                });
                match resumed.or_else(|| self.seek_starts.get(&partition).copied()) {
                    Some(position) => {
                        self.positions.insert(partition, position);
                    }
                    None => {
                        self.positions.remove(&partition);
                    }
                }
            }
        }
        self.groups.clear();
        self.read_alone.clear();
        self.read_alone_until.clear();
        self.lasting_refusals.clear();
        self.probing.clear();
        self.backlogged.clear();
        self.retry_at.clear();
        self.partitions.clear();
        self.pending_errors
            .retain(|partition, _| partition.is_some());
    }

    fn invalidate(&mut self) {
        self.epoch = self.shared.epoch.fetch_add(1, ORDERING) + 1;
        self.rewind();
    }

    /// A seek takes effect here, between two turns. It aborts nothing: a new read of its
    /// partition starts when the read in flight ends, whose reply the new generation ignores.
    fn apply_seeks(&mut self) {
        for (partition, start, generation) in self.shared.progress.take_seeks() {
            let start = first_as_offset(start);
            self.positions.insert(partition, start);
            self.seek_starts.insert(partition, start);
            self.generations.insert(partition, generation);
            self.retry_at.remove(&partition);
            self.read_alone.remove(&partition);
            self.read_alone_until.remove(&partition);
            self.catch_up.remove(&partition);
            self.retained_batches.remove(&partition);
            self.lost_operations.remove(&partition);
            self.lasting_refusals.remove(&partition);
            self.probing.remove(&partition);
            self.backlogged.remove(&partition);
            self.stopped.remove(&partition);
            self.pending_errors.remove(&Some(partition));
            self.shared.progress.forget_undecoded(partition);
        }
    }

    fn read_assignment(
        &self,
    ) -> impl Future<Output = Result<Refreshed, IggyError>> + Send + 'static {
        let config = Arc::clone(&self.config);
        let shared = Arc::clone(&self.shared);
        let epoch = self.epoch;
        async move {
            if let Some(membership) = &shared.membership {
                let membership = Arc::clone(membership);
                // A dropped reader aborts this task. A join that the server received must still
                // finish, or its retained operation blocks every later metadata call.
                tokio::spawn(async move { membership.join().await })
                    .await
                    .unwrap_or(Err(IggyError::ClientShutdown))?;
            }
            let (assignment, mut partitions) = if config.is_group() {
                config
                    .client
                    .read()
                    .await
                    .deferred_poll_partitions(&config.stream, &config.topic, &config.consumer.id)
                    .await?
            } else {
                (AssignmentVersion::default(), config.partitions.clone())
            };
            partitions.sort_unstable();
            let groups = if partitions.is_empty() {
                None
            } else {
                match config
                    .client
                    .read()
                    .await
                    .readiness_groups(&config.stream, &config.topic, &partitions)
                    .await
                {
                    Ok(groups) => Some(groups),
                    // HTTP has no fetch, so each partition polls alone.
                    Err(IggyError::FeatureUnavailable) => None,
                    Err(error) => return Err(error),
                }
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
        if refreshed.epoch != self.epoch || self.epoch != self.shared.epoch.load(ORDERING) {
            return false;
        }
        if self.assignment != Some(refreshed.assignment) {
            // The server keeps the owner of a partition that stays with its
            // member, so that partition continues from its position. Every
            // partition has a new owner in a new session. A partition that
            // moved away and back between two refreshes has one too: the
            // server refuses a read from its old position, and the worker
            // then drops that position.
            let same_session = self
                .assignment
                .is_some_and(|assignment| assignment.session == refreshed.assignment.session);
            let staying: Vec<u32> = if !self.config.is_group() {
                refreshed.partitions.clone()
            } else if same_session {
                refreshed
                    .partitions
                    .iter()
                    .copied()
                    .filter(|partition| self.generations.contains_key(partition))
                    .collect()
            } else {
                Vec::new()
            };
            if self.config.is_group() {
                // The counted positions of the partitions that leave go out before their
                // generations end.
                let leaving: Vec<u32> = self
                    .generations
                    .keys()
                    .copied()
                    .filter(|partition| staying.binary_search(partition).is_err())
                    .collect();
                self.shared.progress.retire_counted(&leaving);
                for partition in leaving {
                    self.interrupt_catch_up(partition);
                }
            }
            self.invalidate();
            if self.config.is_group() {
                self.generations = self.shared.progress.assign(&refreshed.partitions, &staying);
            }
            let stays = |partition: &u32| staying.binary_search(partition).is_ok();
            self.positions.retain(|partition, _| stays(partition));
            self.seek_starts.retain(|partition, _| stays(partition));
            self.stopped.retain(stays);
            self.pending_errors.retain(|partition, refusal| {
                partition.as_ref().is_none_or(stays)
                    || matches!(refusal.error.error, IggyError::FetchProgressUncertain(_))
            });
            self.assignment = Some(refreshed.assignment);
        }
        self.partitions = refreshed.partitions;
        self.shared.set_partitions(&self.partitions);
        let groups = refreshed.groups.unwrap_or_default();
        if groups != self.groups {
            // A fetch takes no partitions after it starts, so the fetches that
            // commit nothing start over in the new groups. A committing fetch
            // ends on its own, and its partitions wait for it.
            if !self.config.on_read() {
                self.tasks = JoinSet::new();
                self.active.clear();
            }
            self.groups = groups;
        }
        self.served
            .retain(|partition, _| self.partitions.binary_search(partition).is_ok());
        self.backlogged
            .retain(|partition| self.partitions.binary_search(partition).is_ok());
        self.shared.progress.retain_undecoded(&self.partitions);
        true
    }

    fn refuse_refresh(&mut self, error: IggyError) {
        match error {
            // The next refresh joins again.
            IggyError::ConsumerGroupMemberNotFound(..) if self.shared.membership.is_some() => {
                self.forget_membership();
            }
            IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient => {
                self.forget_membership();
                self.report(ReadError::new(None, error));
            }
            // A partition that is not routable yet, or a primary that is moving, settles by
            // itself, and the next refresh runs after the retry interval.
            IggyError::TransientNotAccepted
            | IggyError::TransientNotCommitted
            | IggyError::NotPrimary
            | IggyError::DeferredPollCapacityExceeded => {
                warn!("Reader assignment refresh will be retried after a transient error: {error}");
            }
            error => self.report(ReadError::new(None, error)),
        }
    }

    fn reserve(&self) -> Option<Arc<FetchCapacity>> {
        self.reserve_from(&self.capacity)
    }

    fn reserve_recovery(&self, partition: u32) -> Option<Arc<FetchCapacity>> {
        if self.retained_batches.contains_key(&partition) {
            self.reserve_from(&self.recovery_capacity)
        } else {
            self.reserve()
        }
    }

    fn reserve_from(&self, capacity: &Arc<Semaphore>) -> Option<Arc<FetchCapacity>> {
        Arc::clone(capacity).try_acquire_owned().ok().map(|permit| {
            Arc::new(FetchCapacity {
                permit: Some(permit),
                wake: Arc::clone(&self.shared.wake),
            })
        })
    }

    /// The error waits for a slot. It goes out at the next turn, after the batches of the same
    /// reply, so a `seek` after it does not drop them.
    fn report(&mut self, error: ReadError) {
        let generation = error
            .partition_id
            .map(|partition| self.generation(partition));
        self.pending_errors
            .insert(error.partition_id, Refusal { error, generation });
    }

    /// An error also needs a slot.
    fn flush_errors(&mut self) {
        while !self.pending_errors.is_empty() {
            let Some(capacity) = self.reserve() else {
                return;
            };
            let Some((_, refusal)) = self.pending_errors.pop_first() else {
                return;
            };
            self.deliver(Reply {
                epoch: self.epoch,
                assignment: self.assignment,
                result: Err(refusal),
                capacity,
            });
        }
    }

    /// The application takes one reply per demand and asks again for the next. Each reply
    /// holds a slot, and the channel has one place per slot, so the send never fails.
    fn deliver(&self, reply: Reply) {
        self.shared.demand.store(false, ORDERING);
        let _ = self.sender.send(reply);
    }

    fn schedule(&mut self) {
        if self.partitions.is_empty() {
            return;
        }
        if self.groups.is_empty() {
            if self.config.on_read() {
                self.schedule_on_demand();
            } else {
                self.schedule_alone();
            }
            return;
        }
        // Every fetch of this mode commits its batches, so the reader fetches only when the
        // application waits for one, and then fills every free slot.
        if !self.config.on_read() || self.shared.demand.load(ORDERING) {
            self.schedule_groups();
        }
        self.schedule_alone();
    }

    /// Without readiness groups, each committing poll stores the offset of its batch. The
    /// reader polls on demand, so it holds at most one committed batch per partition that the
    /// application did not ask for.
    fn schedule_on_demand(&mut self) {
        if !self.shared.demand.load(ORDERING) {
            return;
        }
        for index in 0..self.partitions.len() {
            let partition = self.partitions[index];
            if !self.is_idle(partition) {
                continue;
            }
            let Some(capacity) = self.reserve_recovery(partition) else {
                return;
            };
            let kind = if self.needs_start(partition) {
                FetchKind::Check
            } else {
                FetchKind::Alone
            };
            self.spawn_poll(partition, kind, capacity);
        }
    }

    /// A fetch takes no partitions after it starts, so the main fetch of a group
    /// runs until it ends. The quiet partitions that become idle meanwhile share a
    /// second fetch, and each new one restarts only that fetch. This keeps a busy
    /// partition among quiet ones from waiting up to `max_wait` for the next main
    /// fetch. The next main fetch takes over the partitions of the second one, so
    /// the second one stays small. Every group with idle partitions gets a slot,
    /// those without a main fetch first, before any group gets a second one. The
    /// slots left then spread the backlogged partitions of each group over more
    /// fetches. Their reads return at once, so each slot keeps a full read in
    /// flight, as a poll of each partition would, and no more reads park.
    ///
    /// A committing fetch never restarts, so with `OnRead` the quiet partitions
    /// that turn idle wait for the one main fetch of their group to end, and only
    /// the backlogged ones read beside it, in fetches of their own.
    fn schedule_groups(&mut self) {
        let count = self.groups.len();
        let start = self.group_cursor % count;
        let mut plans = Vec::new();
        for kind in [FetchKind::Main, FetchKind::Second] {
            for index in 0..count {
                let group = (start + index) % count;
                let main = self.main_in_flight(group);
                if main != (kind == FetchKind::Second) {
                    continue;
                }
                let targets = self.targets(group);
                if targets.is_empty() {
                    continue;
                }
                let (mut backlogged, mut quiet): (Vec<u32>, Vec<u32>) = targets
                    .into_iter()
                    .partition(|partition| self.backlogged.contains(partition));
                backlogged.sort_by_key(|partition| {
                    self.served.get(partition).copied().unwrap_or_default()
                });
                if main && self.config.on_read() {
                    quiet.clear();
                    if backlogged.is_empty() {
                        continue;
                    }
                }
                plans.push(GroupPlan {
                    group,
                    main,
                    quiet,
                    backlogged,
                    extra: 0,
                });
            }
        }
        let free = self.capacity.available_permits();
        plans.truncate(free);
        if plans.iter().any(|plan| plan.room() > 0) {
            let mut extra = free
                .saturating_sub(plans.len())
                .saturating_sub(self.alone_reserve());
            while extra > 0 {
                let before = extra;
                for plan in plans.iter_mut().filter(|plan| plan.extra < plan.room()) {
                    if extra == 0 {
                        break;
                    }
                    plan.extra += 1;
                    extra -= 1;
                }
                if extra == before {
                    break;
                }
            }
        }
        for plan in plans {
            if !self.spawn_group(plan, count) {
                return;
            }
        }
    }

    /// Starts the fetches of one group: the quiet partitions in one fetch, which takes over the
    /// second fetch in flight, and the backlogged ones dealt in turn over the extra slots.
    /// Without an extra slot they join the quiet ones. Returns false when no slot is left.
    fn spawn_group(&mut self, plan: GroupPlan, count: usize) -> bool {
        let GroupPlan {
            group,
            main,
            quiet,
            backlogged,
            extra,
        } = plan;
        let on_read = self.config.on_read();
        let chunks = extra + usize::from(quiet.is_empty());
        let mut waiting = quiet;
        let mut spread = vec![Vec::new(); chunks];
        for (index, partition) in backlogged.into_iter().enumerate() {
            match index
                .checked_rem(chunks)
                .and_then(|chunk| spread.get_mut(chunk))
            {
                Some(targets) => targets.push(partition),
                None => waiting.push(partition),
            }
        }
        if !waiting.is_empty() {
            let Some(capacity) = self.reserve() else {
                return false;
            };
            if let Some((second, task)) = self.group_fetch(group, FetchKind::Second) {
                task.abort();
                let restarted: Vec<u32> = self
                    .active
                    .iter()
                    .filter(|(_, active)| active.fetch == second)
                    .map(|(partition, _)| *partition)
                    .collect();
                for partition in &restarted {
                    self.active.remove(partition);
                }
                waiting.extend(restarted);
            }
            let kind = if main {
                FetchKind::Second
            } else {
                FetchKind::Main
            };
            self.group_cursor = (group + 1) % count;
            self.spawn_fetch(waiting, kind, capacity);
        }
        let kind = if on_read {
            FetchKind::Backlog
        } else {
            FetchKind::Main
        };
        for targets in spread {
            let Some(capacity) = self.reserve() else {
                return false;
            };
            self.group_cursor = (group + 1) % count;
            self.spawn_fetch(targets, kind, capacity);
        }
        true
    }

    /// Whether the fetch of `group` that may park is in flight. With `OnRead` a check that
    /// resolves a `Next` start counts too, so the partition joins the main fetch after it.
    fn main_in_flight(&self, group: usize) -> bool {
        self.group_fetch(group, FetchKind::Main).is_some()
            || (self.config.on_read()
                && self.groups[group].iter().any(|partition| {
                    self.active
                        .get(partition)
                        .is_some_and(|active| active.kind == FetchKind::Check)
                        && self.position(*partition).kind == PollingKind::Next
                        && self.needs_start(*partition)
                }))
    }

    /// The slots that the polls alone after the group fetches can take: one for each idle
    /// partition that no group fetch reads, within the limit of [`Self::schedule_alone`].
    fn alone_reserve(&self) -> usize {
        let limit = self.config.slots.saturating_sub(self.groups.len()).max(1);
        let running = self
            .active
            .values()
            .filter(|active| matches!(active.kind, FetchKind::Alone | FetchKind::Check))
            .count();
        let waiting = self
            .partitions
            .iter()
            .filter(|partition| self.is_idle(**partition) && !self.fetched_by_group(**partition))
            .count();
        waiting.min(limit.saturating_sub(running))
    }

    /// The fetch of `kind` in flight for `group`.
    fn group_fetch(&self, group: usize, kind: FetchKind) -> Option<(u64, AbortHandle)> {
        self.groups[group].iter().find_map(|partition| {
            self.active
                .get(partition)
                .filter(|active| active.kind == kind)
                .map(|active| (active.fetch, active.task.clone()))
        })
    }

    /// Polls partitions alone, from the one after the last polled one. With
    /// readiness groups, it polls only the partitions that the group fetches
    /// cannot read, and keeps one fetch slot per group free of them, so polls that
    /// wait for a first message never hold every slot. With `OnRead`, a poll that
    /// commits or delivers waits for the application to ask, as the fetches do.
    fn schedule_alone(&mut self) {
        let by_group = !self.groups.is_empty();
        // Every partition is in a group fetch, so the walk below would find nothing.
        if by_group
            && self.probing.is_empty()
            && self.read_alone.is_empty()
            && self.catch_up.is_empty()
            && !(self.config.on_read() && self.config.start.kind == PollingKind::Next)
            && fetched_by_group_kind(self.config.start.kind)
            && self
                .positions
                .values()
                .all(|position| fetched_by_group_kind(position.kind))
        {
            return;
        }
        let limit = if by_group {
            self.config.slots.saturating_sub(self.groups.len()).max(1)
        } else {
            MAX_DEFERRED_REQUESTS_PER_CONSUMER
        };
        let mut running = self
            .active
            .values()
            .filter(|active| matches!(active.kind, FetchKind::Alone | FetchKind::Check))
            .count();
        let start = self.cursor.map_or(0, |cursor| {
            self.partitions.partition_point(|id| *id <= cursor)
        });
        for index in 0..self.partitions.len() {
            if running >= limit {
                break;
            }
            let partition = self.partitions[(start + index) % self.partitions.len()];
            if !self.is_idle(partition) {
                continue;
            }
            let on_read = self.config.on_read();
            let demand = self.shared.demand.load(ORDERING);
            if self.needs_start(partition)
                && self.position(partition).kind == PollingKind::Next
                && !demand
            {
                continue;
            }
            let kind = if !by_group {
                FetchKind::Alone
            } else if self.fetched_by_group(partition) {
                continue;
            } else if on_read
                && (self.read_alone.contains(&partition) || self.catching_up(partition).is_some())
            {
                if !demand {
                    continue;
                }
                FetchKind::Alone
            } else if on_read || self.probing.contains(&partition) {
                FetchKind::Check
            } else {
                FetchKind::Alone
            };
            let capacity = match kind {
                FetchKind::Alone if on_read => self.reserve_recovery(partition),
                FetchKind::Check if on_read => self
                    .reserve()
                    .or_else(|| self.reserve_from(&self.recovery_capacity)),
                _ => self.reserve(),
            };
            let Some(capacity) = capacity else {
                break;
            };
            self.spawn_poll(partition, kind, capacity);
            running += 1;
        }
    }

    /// The partitions of a group that its next fetch reads.
    fn targets(&self, group: usize) -> Vec<u32> {
        self.groups[group]
            .iter()
            .copied()
            .filter(|partition| self.is_idle(*partition) && self.fetched_by_group(*partition))
            .collect()
    }

    /// A fetch of a group reads `Offset` and `Next` positions only.
    fn fetched_by_group(&self, partition: u32) -> bool {
        !self.probing.contains(&partition)
            && !self.needs_start(partition)
            && !self.read_alone.contains(&partition)
            && self.catching_up(partition).is_none()
            && fetched_by_group_kind(self.position(partition).kind)
    }

    fn needs_start(&self, partition: u32) -> bool {
        let position = self.position(partition);
        self.config.on_read()
            && matches!(
                position.kind,
                PollingKind::Next | PollingKind::Last | PollingKind::Timestamp
            )
            && position.context().is_none()
    }

    fn is_idle(&self, partition: u32) -> bool {
        !self.active.contains_key(&partition)
            && !self.retry_at.contains_key(&partition)
            && !self.stopped.contains(&partition)
            && self.shared.progress.blocked(partition).is_none()
    }

    fn position(&self, partition: u32) -> PollingStrategy {
        self.positions
            .get(&partition)
            .copied()
            .unwrap_or(self.config.start)
    }

    /// A partition without a generation reads nothing that counts.
    fn generation(&self, partition: u32) -> u64 {
        self.generations
            .get(&partition)
            .copied()
            .unwrap_or_default()
    }

    fn keeps(&self, partition: u32, generation: u64) -> bool {
        self.shared
            .progress
            .keeps(partition, generation, self.config.on_read())
    }

    fn request_timeout(&self) -> Duration {
        self.config.options.request_timeout().get_duration()
    }

    fn spawn_fetch(&mut self, partitions: Vec<u32>, kind: FetchKind, capacity: Arc<FetchCapacity>) {
        let targets = self.fetch_targets(partitions);
        let wire_targets: Vec<(u32, PollingStrategy)> = targets
            .iter()
            .map(|(partition, position, _)| (*partition, *position))
            .collect();
        let partitions: Vec<u32> = targets.iter().map(|(partition, ..)| *partition).collect();
        let config = Arc::clone(&self.config);
        let on_read = config.on_read();
        let count = config.options.count;
        let options = DeferredPollOptions {
            max_bytes: fetch_max_bytes(config.options.max_bytes, targets.len()),
            ..DeferredPollOptions::from(config.options)
        };
        let commit = on_read
            .then(|| PendingCommit::new(Arc::clone(&self.shared.progress), partitions.clone()));
        let epoch = self.epoch;
        self.fetches += 1;
        let fetch = self.fetches;
        let task = self.tasks.spawn(DEFERRED_CONSUMER_CAPACITY.scope(
            Arc::clone(&self.request_capacity),
            CANCEL_COMMITTING.scope(self.cancel_committing.subscribe(), async move {
                let started = Instant::now();
                let deadline = started + options.request_timeout.get_duration();
                // Only the lock wait has a limit here: the driver ends the fetch by the same
                // deadline, and a committing fetch must never be dropped once sent.
                let result = async {
                    let client = timeout_at(deadline, config.client.read())
                        .await
                        .map_err(|_| IggyError::TransientNotAccepted)?;
                    let sections = client
                        .fetch_messages(
                            &config.stream,
                            &config.topic,
                            &wire_targets,
                            &config.consumer,
                            count,
                            on_read,
                            options.remaining(started.elapsed())?,
                        )
                        .await?;
                    let messages: u64 = sections
                        .iter()
                        .map(|section| u64::from(section.messages_count))
                        .sum();
                    if messages > u64::from(count) {
                        return Err(IggyError::InvalidMessagesCount);
                    }
                    Ok(sections)
                }
                .await;
                Completed::Fetch(CompletedFetch {
                    epoch,
                    fetch,
                    targets,
                    result,
                    capacity,
                    commit,
                })
            }),
        ));
        for partition in partitions {
            self.active.insert(
                partition,
                Active {
                    task: task.clone(),
                    kind,
                    fetch,
                },
            );
        }
    }

    /// A fetch serves its targets in order, so the partitions that delivered
    /// messages last go to its end, behind the ones that never did.
    fn fetch_targets(&self, mut partitions: Vec<u32>) -> Vec<(u32, PollingStrategy, u64)> {
        partitions.sort_by_key(|partition| self.served.get(partition).copied().unwrap_or_default());
        partitions
            .into_iter()
            .map(|partition| {
                (
                    partition,
                    self.position(partition),
                    self.generation(partition),
                )
            })
            .collect()
    }

    fn spawn_poll(&mut self, partition: u32, kind: FetchKind, capacity: Arc<FetchCapacity>) {
        let config = Arc::clone(&self.config);
        let strategy = self.position(partition);
        let generation = self.generation(partition);
        let catching_up = self.catching_up(partition);
        let options = if self.probing.contains(&partition) {
            // Only a refusal answers a probe, so it does not wait.
            PollOptions {
                count: SINGLE_RECORD,
                min_count: SINGLE_RECORD,
                max_wait: IggyDuration::from(Duration::ZERO),
                ..config.options
            }
        } else if self.read_alone.contains(&partition) || self.needs_start(partition) {
            PollOptions {
                count: SINGLE_RECORD,
                min_count: SINGLE_RECORD,
                ..config.options
            }
        } else if let Some(until) = catching_up {
            // The server committed these messages already, so the read takes them and no more.
            let range = until.saturating_sub(strategy.value).saturating_add(1);
            PollOptions {
                count: u32::try_from(range)
                    .unwrap_or(u32::MAX)
                    .min(config.options.count),
                min_count: SINGLE_RECORD,
                max_wait: IggyDuration::from(Duration::ZERO),
                ..config.options
            }
        } else {
            config.options
        };
        // A check delivers nothing, and a catch-up reads what is committed, so neither commits.
        let on_read = config.on_read() && kind == FetchKind::Alone && catching_up.is_none();
        let commit =
            on_read.then(|| PendingCommit::new(Arc::clone(&self.shared.progress), vec![partition]));
        let policy = if on_read {
            CommitPolicy::OnRead
        } else {
            CommitPolicy::Manual
        };
        let epoch = self.epoch;
        self.fetches += 1;
        let fetch = self.fetches;
        let task = self.tasks.spawn(DEFERRED_CONSUMER_CAPACITY.scope(
            Arc::clone(&self.request_capacity),
            CANCEL_COMMITTING.scope(self.cancel_committing.subscribe(), async move {
                let started = Instant::now();
                let deadline = started + options.request_timeout().get_duration();
                // As in `spawn_fetch`, only the lock wait has a limit here.
                let result = async {
                    let client =
                        timeout_at(deadline, config.client.read())
                            .await
                            .map_err(|_| {
                                DecodeError::without_record(IggyError::TransientNotAccepted)
                            })?;
                    let remaining = DeferredPollOptions::from(options)
                        .remaining(started.elapsed())
                        .map_err(DecodeError::without_record)?;
                    let polled = client
                        .poll_messages_raw(
                            &config.stream,
                            &config.topic,
                            partition,
                            &config.consumer,
                            &strategy,
                            PollOptions {
                                max_wait: remaining.max_wait,
                                commit: policy,
                                ..options
                            },
                        )
                        .await?;
                    if polled.count > options.count
                        || polled.messages.len() > options.count as usize
                    {
                        return Err(DecodeError::without_record(IggyError::InvalidMessagesCount));
                    }
                    Ok(polled)
                }
                .await;
                Completed::Poll(CompletedPoll {
                    epoch,
                    kind,
                    partition,
                    strategy,
                    generation,
                    result,
                    capacity,
                    commit,
                })
            }),
        ));
        self.active.insert(partition, Active { task, kind, fetch });
        self.cursor = Some(partition);
    }

    fn complete(&mut self, completed: Completed) {
        match completed {
            Completed::Fetch(completed) => self.complete_fetch(completed),
            Completed::Poll(completed) if completed.kind == FetchKind::Check => {
                self.complete_check(completed);
            }
            Completed::Poll(completed) => {
                let partition = completed.partition;
                self.complete_poll(completed);
                self.read_alone_to_section_end(partition);
            }
        }
    }

    /// Keeps a partition reading alone while its position lies inside a section that did not
    /// decode.
    fn read_alone_to_section_end(&mut self, partition: u32) {
        let Some(&until) = self.read_alone_until.get(&partition) else {
            return;
        };
        let position = self.position(partition);
        if position.kind == PollingKind::Offset && position.value < until {
            self.read_alone.insert(partition);
        } else {
            self.read_alone_until.remove(&partition);
        }
    }

    /// The section table moves the positions, so the next fetch of a group that
    /// commits nothing starts before the worker decodes the records. With an
    /// encryptor, a batch that does not decrypt keeps its position, so the
    /// worker decrypts first.
    fn complete_fetch(&mut self, completed: CompletedFetch) {
        let CompletedFetch {
            epoch,
            fetch,
            mut targets,
            result,
            capacity,
            commit,
        } = completed;
        // Sorted, so each section of a reply finds its target and its hold by binary search.
        targets.sort_unstable_by_key(|(partition, ..)| *partition);
        let held: Vec<u32> = targets
            .iter()
            .map(|(partition, ..)| *partition)
            .filter(|partition| {
                self.active
                    .get(partition)
                    .is_some_and(|active| active.fetch == fetch)
            })
            .collect();
        let is_held = |partition: &u32| held.binary_search(partition).is_ok();
        for partition in &held {
            self.active.remove(partition);
        }
        let committing = commit.is_some();
        let sections = match result {
            Ok(sections) => sections,
            Err(error) => {
                if let Some(commit) = commit {
                    if may_hide_commit(&error) {
                        for (partition, _, generation) in &targets {
                            if self.keeps(*partition, *generation) {
                                self.lose(*partition, &error);
                            }
                        }
                    } else {
                        commit.settle(Vec::new());
                    }
                }
                if self.is_current(epoch) {
                    self.refuse_fetch(&held, error);
                } else if committing {
                    let partitions: Vec<u32> =
                        targets.iter().map(|(partition, ..)| *partition).collect();
                    self.abandon_stale(&partitions, &error);
                }
                return;
            }
        };
        let mut served = Vec::with_capacity(sections.len());
        let mut answered = Vec::with_capacity(sections.len());
        for section in sections {
            let partition = section.partition_id;
            let Ok(target) = targets.binary_search_by_key(&partition, |(target, ..)| *target)
            else {
                continue;
            };
            if section.status.is_err() || section.messages_count > 0 {
                answered.push(partition);
            }
            let (_, sent, generation) = targets[target];
            match &section.status {
                Err(error) => {
                    if self.is_current(epoch)
                        && is_held(&partition)
                        && self.keeps(partition, generation)
                    {
                        self.refuse_partition(partition, error.clone());
                    } else if committing {
                        self.abandon_stale(&[partition], error);
                    }
                }
                Ok(()) => served.push((sent, generation, section)),
            }
        }
        let current = self.is_current(epoch);
        if current {
            answered.sort_unstable();
            let idle: Vec<u32> = held
                .iter()
                .copied()
                .filter(|partition| answered.binary_search(partition).is_err())
                .collect();
            self.pause_idle(&idle);
        }
        // The server committed the batches of a committing fetch, so they reach the
        // application even after the fetch state was cleared, unless a seek replaced the
        // generation of their partition. The close then moves the offset back over them.
        let replaced: Vec<u32> = served
            .iter()
            .filter(|(_, generation, section)| {
                committing
                    && section.messages_count > 0
                    && !self.keeps(section.partition_id, *generation)
            })
            .map(|(_, _, section)| section.partition_id)
            .collect();
        served.retain(|(sent, generation, section)| {
            let partition = section.partition_id;
            self.keeps(partition, *generation)
                && (committing
                    || (current && is_held(&partition) && *sent == self.position(partition)))
        });
        self.replies += 1;
        let mut batches = Vec::with_capacity(served.len());
        let mut lost = replaced;
        let mut undecoded = Vec::with_capacity(served.len());
        let mut kept = Vec::with_capacity(served.len());
        for (sent, generation, section) in served {
            let partition = section.partition_id;
            let current = committing && sent == self.position(partition);
            if current
                && self.skipped_by_lost_read(
                    partition,
                    &sent,
                    section_start(&section),
                    section.context,
                )
            {
                let until = section.next_offset.saturating_sub(1);
                self.start_catch_up(partition, sent.value, until, section.context);
                if section.messages_count > 0 {
                    match section.decode_records() {
                        Ok(mut batch) => {
                            batch.set_stamp(self.shared.progress.stamp(generation));
                            self.retained_batches.insert(
                                partition,
                                RetainedBatch {
                                    batch,
                                    capacity: Arc::clone(&capacity),
                                },
                            );
                        }
                        Err(error) => self.refuse_section(&section, false, true, &mut lost, error),
                    }
                }
                lost.push(partition);
                continue;
            }
            if current && section.messages_count > 0 {
                self.lost_operations.remove(&partition);
            }
            kept.push((sent, generation, section));
        }
        for (sent, generation, section) in kept {
            let Some(encryptor) = self.config.encryptor.clone() else {
                let moved = self.advance(sent, &section);
                undecoded.push((moved, generation, section));
                continue;
            };
            let partition = section.partition_id;
            let mut polled = match section.decode_records() {
                Ok(polled) => polled,
                Err(error) => {
                    let rewind = sent == self.position(partition);
                    self.refuse_section(&section, rewind, committing, &mut lost, error);
                    continue;
                }
            };
            match decrypt_messages(&encryptor, &mut polled) {
                Ok(()) => {
                    self.advance(sent, &section);
                }
                Err((index, failed, error)) => {
                    let rewind = sent == self.position(partition);
                    let record = polled.undecoded_record(index).map(Box::new);
                    self.refuse_undecoded(
                        rewind,
                        partition,
                        failed,
                        section.context,
                        error,
                        record,
                    );
                }
            }
            if !polled.messages.is_empty() {
                polled.set_stamp(self.shared.progress.stamp(generation));
                batches.push(polled);
            }
        }
        if !committing && self.shared.can_poll.load(ORDERING) {
            self.schedule();
        }
        for (moved, generation, section) in undecoded {
            if section.messages_count == 0 {
                continue;
            }
            match section.decode_records() {
                Ok(mut polled) => {
                    polled.set_stamp(self.shared.progress.stamp(generation));
                    batches.push(polled);
                }
                // A committing fetch already moved the server offset past the
                // records, so the worker reads them again from their start. A
                // fetch sent from the moved position then reads stale.
                Err(error) => self.refuse_section(&section, moved, committing, &mut lost, error),
            }
        }
        if let Some(commit) = commit {
            commit.settle(lost);
        }
        for batch in &batches {
            self.shared.progress.forget_undecoded_covered(batch);
        }
        if !batches.is_empty() {
            self.deliver(Reply {
                epoch: self.epoch,
                assignment: self.assignment,
                result: Ok(batches),
                capacity,
            });
        }
    }

    /// A section that does not decode has no message boundary, so it fails at its start. Unless
    /// it holds one message, its partition reads from there one message at a time: each good
    /// message reaches the application, and the read of the bad one names it.
    fn refuse_section(
        &mut self,
        section: &FetchedSection,
        rewind: bool,
        committing: bool,
        lost: &mut Vec<u32>,
        error: DecodeError,
    ) {
        let partition = section.partition_id;
        let start = section_start(section);
        if committing {
            lost.push(partition);
            self.shared.progress.remember_undecoded(Checkpoint::new(
                partition,
                start,
                section.context,
            ));
        }
        if rewind && section.messages_count != SINGLE_RECORD {
            warn!(
                "Failed to decode {count} messages from offset: {start}, partition ID: {partition}. They are read again one at a time. {}",
                error.error,
                count = section.messages_count
            );
            self.positions.insert(
                partition,
                continuation_at(partition, start, section.context),
            );
            self.read_alone.insert(partition);
            self.read_alone_until.insert(partition, section.next_offset);
            return;
        }
        self.refuse_undecoded(
            rewind,
            partition,
            start,
            section.context,
            error.error,
            error.record,
        );
    }

    /// The message at `start` does not decode. Its partition reads it alone again until a seek
    /// to the skip of the error.
    fn refuse_message(&mut self, partition: u32, start: &PollingStrategy, error: DecodeError) {
        self.read_alone.insert(partition);
        let context = start.context().unwrap_or_default();
        self.refuse_undecoded(
            false,
            partition,
            start.value,
            context,
            error.error,
            error.record,
        );
    }

    /// The reader reads the message at `failed` again until a seek to the continuation after
    /// it. With `rewind`, the position moves back to it.
    fn refuse_undecoded(
        &mut self,
        rewind: bool,
        partition: u32,
        failed: u64,
        context: PartitionContext,
        error: IggyError,
        record: Option<Box<UndecodedRecord>>,
    ) {
        if rewind {
            self.positions
                .insert(partition, continuation_at(partition, failed, context));
        }
        self.retry(&[partition]);
        self.report(ReadError {
            partition_id: Some(partition),
            error,
            skip: failed
                .checked_add(1)
                .map(|next| continuation_at(partition, next, context)),
            offset: record.as_ref().and_then(|record| record.offset),
            record: record.map(|record| record.bytes),
        });
    }

    /// Moves the position of the partition of `section` after it, unless a new
    /// assignment or a seek replaced the position that the fetch read from. A
    /// `Next` target continues from an offset too, also without a read.
    fn advance(&mut self, sent: PollingStrategy, section: &FetchedSection) -> bool {
        let partition = section.partition_id;
        self.lasting_refusals.remove(&partition);
        self.shared
            .progress
            .forget_undecoded_history(partition, section.context);
        if sent != self.position(partition) {
            return false;
        }
        let position = self.continue_at(partition, section.next_offset, section.context);
        self.positions.insert(partition, position);
        if ends_before_head(section) {
            self.backlogged.insert(partition);
        } else {
            self.backlogged.remove(&partition);
        }
        if section.messages_count > 0 {
            self.served.insert(partition, self.replies);
        }
        true
    }

    /// A check delivers nothing. Its batch resolves the start of a fetch that
    /// commits, and its refusal names the partition of a refused fetch.
    fn complete_check(&mut self, completed: CompletedPoll) {
        let partition = completed.partition;
        self.active.remove(&partition);
        let probe = self.probing.remove(&partition);
        if !self.is_current(completed.epoch) || !self.keeps(partition, completed.generation) {
            return;
        }
        match completed.result {
            // A probe that passes hands its partition back to the group fetch.
            Ok(polled) if polled.messages.is_empty() && !probe => self.pause_idle(&[partition]),
            Ok(polled) => {
                if (self.needs_start(partition)
                    || !matches!(
                        completed.strategy.kind,
                        PollingKind::Offset | PollingKind::Next
                    ))
                    && completed.strategy == self.position(partition)
                    && let Some(first) = polled.messages.first()
                {
                    let position = if completed.strategy.kind == PollingKind::Next {
                        self.continue_at(partition, first.header.offset, polled.context())
                    } else {
                        continuation_at(partition, first.header.offset, polled.context())
                    };
                    self.positions.insert(partition, position);
                }
            }
            // A poll returns a record above `max_bytes` alone, so its refusal is final.
            Err(DecodeError {
                error: IggyError::LaneBlocked(operation),
                ..
            }) if self.config.on_read() => {
                self.refuse(
                    &[partition],
                    IggyError::PartitionLaneBlocked(partition, operation),
                );
            }
            Err(error) => self.refuse(&[partition], error.error),
        }
    }

    fn complete_poll(&mut self, mut completed: CompletedPoll) {
        let partition = completed.partition;
        if self.config.on_read()
            && completed.commit.is_none()
            && (!self.is_current(completed.epoch)
                || !self.keeps(partition, completed.generation)
                || completed.strategy != self.position(partition))
        {
            return;
        }
        self.active.remove(&partition);
        let commits = completed.commit.is_some();
        if let Err(error) = &completed.result
            && commits
            && may_hide_commit(&error.error)
            && self.keeps(partition, completed.generation)
        {
            self.lose(partition, &error.error);
        }
        // Dropped unsettled, it forgets the commit of its partition, so the close rewinds.
        let commit = completed.commit.take().filter(|_| {
            !completed
                .result
                .as_ref()
                .is_err_and(|error| may_hide_commit(&error.error))
        });
        // A mark holds for one read. Another failure to decode marks the partition again.
        let alone = self.read_alone.remove(&partition);
        if self.config.on_read() {
            let lost = self.complete_on_demand(completed, alone, commits);
            if let Some(commit) = commit {
                commit.settle(lost);
            }
            return;
        }
        if !self.is_current(completed.epoch) || !self.keeps(partition, completed.generation) {
            return;
        }
        let mut polled = match completed.result {
            Ok(polled) => {
                self.lasting_refusals.remove(&partition);
                polled
            }
            Err(error) if names_message(alone, &completed.strategy, &error.error) => {
                self.refuse_message(partition, &completed.strategy, error);
                return;
            }
            Err(error) => {
                self.refuse(&[partition], error.error);
                return;
            }
        };
        let context = polled.context();
        let polled_nothing = polled.messages.is_empty();
        let failure = self
            .config
            .encryptor
            .as_deref()
            .and_then(|encryptor| decrypt_messages(encryptor, &mut polled).err())
            .map(|(index, failed, error)| {
                (failed, error, polled.undecoded_record(index).map(Box::new))
            });
        if let Some(last) = polled.messages.last().map(|message| message.header.offset) {
            let Some(next) = last.checked_add(1) else {
                self.refuse(&[partition], IggyError::InvalidOffset(last));
                return;
            };
            // A new read of such a start resolves against a later end, and would skip the
            // messages in between if the epoch drops this batch.
            if !matches!(
                completed.strategy.kind,
                PollingKind::Offset | PollingKind::Next
            ) && let Some(first) = polled.messages.first()
            {
                self.seek_starts.insert(
                    partition,
                    continuation_at(partition, first.header.offset, context),
                );
            }
            self.positions
                .insert(partition, continuation_at(partition, next, context));
            polled.set_stamp(self.shared.progress.stamp(completed.generation));
            self.deliver(Reply {
                epoch: self.epoch,
                assignment: self.assignment,
                result: Ok(vec![polled]),
                capacity: completed.capacity,
            });
        }
        match failure {
            Some((failed, error, record)) => {
                self.refuse_undecoded(true, partition, failed, context, error, record);
            }
            None if polled_nothing => self.pause_idle(&[partition]),
            None => {}
        }
    }

    /// The server committed a batch that holds messages, so the batch reaches the application
    /// even after the fetch state was cleared, unless a seek replaced the generation of its
    /// partition. Returns the partition when the server committed messages that the batch does
    /// not hand over. `commits` is set for a read that stored its offset.
    fn complete_on_demand(
        &mut self,
        completed: CompletedPoll,
        alone: bool,
        commits: bool,
    ) -> Vec<u32> {
        let partition = completed.partition;
        let mut polled = match completed.result {
            Ok(polled) => {
                self.lasting_refusals.remove(&partition);
                polled
            }
            Err(DecodeError { error, record }) => {
                if !self.keeps(partition, completed.generation) {
                    if commits {
                        self.abandon_stale(&[partition], &error);
                    }
                    return Vec::new();
                }
                let strategy = completed.strategy;
                if strategy.kind == PollingKind::Next && undeliverable(&error) {
                    match strategy.context() {
                        // The server stored the offset of a reply that the worker cannot hand
                        // over. A continuation knows where that reply started, so the partition
                        // reads it again one message at a time and the close rewinds before it.
                        Some(context) if self.is_current(completed.epoch) => {
                            let start = Checkpoint::new(partition, strategy.value, context);
                            self.shared.progress.remember_undecoded(start);
                            self.positions.insert(
                                partition,
                                continuation_at(partition, strategy.value, context),
                            );
                            self.read_alone.insert(partition);
                            self.retry(&[partition]);
                        }
                        Some(_) => {}
                        // A read from the stored offset has no known start.
                        None => {
                            self.stopped.insert(partition);
                            self.report(ReadError::new(Some(partition), error));
                        }
                    }
                } else if self.is_current(completed.epoch) {
                    if names_message(alone, &strategy, &error) {
                        // The server stored the offset of the message, so the close rewinds
                        // before it.
                        self.shared.progress.remember_undecoded(Checkpoint::new(
                            partition,
                            strategy.value,
                            strategy.context().unwrap_or_default(),
                        ));
                        self.refuse_message(partition, &strategy, DecodeError { error, record });
                    } else if self.catching_up(partition).is_some() && undeliverable(&error) {
                        // A reply without message boundaries names no message, so the range is
                        // read one message at a time, and the read of a bad one names it.
                        self.read_alone.insert(partition);
                    } else {
                        self.refuse(&[partition], error);
                    }
                } else if commits {
                    self.abandon_stale(&[partition], &error);
                }
                return Vec::new();
            }
        };
        if !self.keeps(partition, completed.generation) {
            // The close moves the offset back over a committed batch that a seek dropped.
            return if commits && !polled.messages.is_empty() {
                vec![partition]
            } else {
                Vec::new()
            };
        }
        let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
            if completed.strategy == self.position(partition)
                && self.skipped_by_lost_read(
                    partition,
                    &completed.strategy,
                    polled.current_offset.saturating_add(1),
                    polled.context(),
                )
            {
                self.start_catch_up(
                    partition,
                    completed.strategy.value,
                    polled.current_offset,
                    polled.context(),
                );
                return vec![partition];
            }
            if completed.strategy == self.position(partition)
                && let Some(until) = self.catching_up(partition)
            {
                self.end_catch_up(partition, until, polled.context());
                return Vec::new();
            }
            self.pause_idle(&[partition]);
            return Vec::new();
        };
        let first = polled
            .messages
            .first()
            .map_or(last, |message| message.header.offset);
        let current = completed.strategy == self.position(partition);
        if current
            && self.skipped_by_lost_read(partition, &completed.strategy, first, polled.context())
        {
            self.start_catch_up(partition, completed.strategy.value, last, polled.context());
            polled.set_stamp(self.shared.progress.stamp(completed.generation));
            self.retained_batches.insert(
                partition,
                RetainedBatch {
                    batch: polled,
                    capacity: completed.capacity,
                },
            );
            return vec![partition];
        }
        if current && commits {
            self.lost_operations.remove(&partition);
        }
        let mut overflow = None;
        // A new assignment replaces the position that the poll started from.
        if current {
            match last.checked_add(1) {
                Some(next) => {
                    let position = self.after_delivery(partition, next, polled.context());
                    self.positions.insert(partition, position);
                }
                None => overflow = Some(IggyError::InvalidOffset(last)),
            }
        }
        polled.set_stamp(self.shared.progress.stamp(completed.generation));
        self.deliver(Reply {
            epoch: self.epoch,
            assignment: self.assignment,
            result: Ok(vec![polled]),
            capacity: completed.capacity,
        });
        if let Some(error) = overflow {
            self.refuse(&[partition], error);
        }
        Vec::new()
    }

    /// The position after a delivery that ends before `next`. With `OnRead` it reads `Next`, so
    /// the server refuses its commit once the stored offset moved, and it keeps `next` for the
    /// gap check.
    fn continue_at(&self, partition: u32, next: u64, context: PartitionContext) -> PollingStrategy {
        if self.config.on_read() {
            PollingStrategy::continuation(partition, PollingKind::Next, next, context)
        } else {
            continuation_at(partition, next, context)
        }
    }

    /// [`Self::continue_at`], except that a partition reads by offset until it passes the end of
    /// its catch-up, or of a section that did not decode.
    fn after_delivery(
        &mut self,
        partition: u32,
        next: u64,
        context: PartitionContext,
    ) -> PollingStrategy {
        if self
            .read_alone_until
            .get(&partition)
            .is_some_and(|until| next < *until)
        {
            return continuation_at(partition, next, context);
        }
        match self.catch_up.get(&partition) {
            Some(&until) if next <= until => continuation_at(partition, next, context),
            Some(_) => {
                self.catch_up.remove(&partition);
                self.retained_batches.remove(&partition);
                self.lost_operations.remove(&partition);
                self.continue_at(partition, next, context)
            }
            None => self.continue_at(partition, next, context),
        }
    }

    /// The end of the catch-up of `partition`, while its position lies inside it.
    fn catching_up(&self, partition: u32) -> Option<u64> {
        let until = *self.catch_up.get(&partition)?;
        let position = self.position(partition);
        (position.kind == PollingKind::Offset && position.value <= until).then_some(until)
    }

    /// A lost read committed `from..=until` without handing it over, so the partition reads that
    /// range again by offset without a commit before it reads `Next` again.
    fn start_catch_up(&mut self, partition: u32, from: u64, until: u64, context: PartitionContext) {
        warn!(
            "Partition: {partition} skipped offsets from: {from} to: {until}, which a lost read committed. They are read again without a commit."
        );
        self.positions
            .insert(partition, continuation_at(partition, from, context));
        self.catch_up.insert(partition, until);
        self.shared
            .progress
            .remember_undecoded(Checkpoint::new(partition, from, context));
    }

    /// A committing read from the `Next` continuation `sent` whose reply starts at `start` in the
    /// history `context` skipped messages that a lost read of the partition committed. Without
    /// a lost read, the application or another session stored that offset, so the read goes on
    /// from there.
    fn skipped_by_lost_read(
        &self,
        partition: u32,
        sent: &PollingStrategy,
        start: u64,
        context: PartitionContext,
    ) -> bool {
        self.lost_operations.contains_key(&partition) && skipped(sent, start, context)
    }

    /// A committing read whose outcome is unknown blocks its partitions on the client until its
    /// operation is abandoned, also after a seek or a new fetch state dropped its reply.
    fn abandon_stale(&mut self, partitions: &[u32], error: &IggyError) {
        if let IggyError::FetchProgressUncertain(operation) = error
            && self.config.on_read()
        {
            self.abandoning.push((*operation, partitions.to_vec()));
        }
    }

    /// Marks a committing read of `partition` that failed with `error` as lost, unless an
    /// earlier lost read is still held.
    fn lose(&mut self, partition: u32, error: &IggyError) {
        let operation = match error {
            IggyError::FetchProgressUncertain(operation) => *operation,
            _ => 0,
        };
        self.lost_operations.entry(partition).or_insert(operation);
    }

    /// A catch-up read that found nothing: the history no longer holds the rest of the range,
    /// so the reader reports it lost once and reads on after the retained batch.
    fn end_catch_up(&mut self, partition: u32, until: u64, context: PartitionContext) {
        let resume = self
            .retained_batches
            .get(&partition)
            .and_then(|retained| retained.batch.messages.last())
            .map_or(until, |last| last.header.offset)
            .saturating_add(1);
        self.interrupt_catch_up(partition);
        self.shared.progress.forget_undecoded(partition);
        let position = self.continue_at(partition, resume, context);
        self.positions.insert(partition, position);
    }

    fn interrupt_catch_up(&mut self, partition: u32) -> bool {
        let Some(until) = self.catch_up.remove(&partition) else {
            return false;
        };
        let position = self.position(partition);
        if let Some(active) = self.active.remove(&partition) {
            active.task.abort();
        }
        let retained = self.retained_batches.remove(&partition);
        let lost_end = retained
            .as_ref()
            .and_then(|retained| retained.batch.messages.first())
            .map_or(until, |first| first.header.offset.saturating_sub(1));
        let operation = self.lost_operations.remove(&partition).unwrap_or_default();
        if position.value <= lost_end {
            error!(
                "Partition: {partition} lost offsets from: {} to: {lost_end} during read recovery",
                position.value
            );
            let error = ReadError {
                partition_id: Some(partition),
                error: IggyError::FetchProgressUncertain(operation),
                skip: lost_end.checked_add(1).map(|next| {
                    continuation_at(partition, next, position.context().unwrap_or_default())
                }),
                offset: None,
                record: None,
            };
            if let Some(retained) = &retained {
                self.deliver(Reply {
                    epoch: self.epoch,
                    assignment: self.assignment,
                    result: Err(Refusal {
                        error,
                        generation: retained.batch.stamp().map(|stamp| stamp.generation),
                    }),
                    capacity: Arc::clone(&retained.capacity),
                });
            } else {
                self.report(error);
            }
        }
        if let Some(mut retained) = retained {
            retained
                .batch
                .messages
                .retain(|message| message.header.offset >= position.value);
            retained.batch.count = u32::try_from(retained.batch.messages.len()).unwrap_or(u32::MAX);
            if !retained.batch.messages.is_empty() {
                self.deliver(Reply {
                    epoch: self.epoch,
                    assignment: self.assignment,
                    result: Ok(vec![retained.batch]),
                    capacity: retained.capacity,
                });
            }
        }
        true
    }

    /// Abandons the operations of lost committing reads, so their progress blocks go. The next
    /// `Next` read of each partition finds what they committed.
    async fn abandon_lost(&mut self) {
        if self.abandoning.is_empty() {
            return;
        }
        let mut operations = std::mem::take(&mut self.abandoning);
        operations.sort_unstable_by_key(|(operation, _)| *operation);
        let client = self.config.client.clone();
        let client = client.read().await;
        if client.operation_lineage().await.is_none() {
            for (operation, partitions) in operations {
                for partition in partitions {
                    self.block(partition, operation);
                    self.report(ReadError::new(
                        Some(partition),
                        IggyError::FetchProgressUncertain(operation),
                    ));
                }
            }
            return;
        }
        operations.dedup_by_key(|(operation, _)| *operation);
        for (operation, _) in operations {
            // An operation that the client no longer retains is already free.
            let _ = client.abandon_operation(operation).await;
        }
    }

    /// A fetch that the server refused before its first read names no target,
    /// also when the history of one changed or the member no longer owns one.
    /// Each target then probes alone, and its own refusal tells.
    fn refuse_fetch(&mut self, partitions: &[u32], error: IggyError) {
        if partitions.len() > 1
            && matches!(
                error,
                IggyError::HistoryUnavailable | IggyError::ConsumerGroupPartitionNotOwned(..)
            )
        {
            self.probing.extend(partitions);
            return;
        }
        self.refuse(partitions, error);
    }

    /// The reads of one fetch share one turn for a first record above `max_bytes`, so a section
    /// that lost it is read again by a poll of its partition, which always returns that record.
    /// Only a failure of that poll is reported.
    fn refuse_partition(&mut self, partition: u32, error: IggyError) {
        if error == IggyError::InvalidSizeBytes {
            self.read_alone.insert(partition);
            return;
        }
        self.refuse(&[partition], error);
    }

    /// The one classifier of read refusals. A refusal of one partition affects only that
    /// partition, so the healthy sections of the same reply survive.
    fn refuse(&mut self, partitions: &[u32], error: IggyError) {
        for partition in partitions {
            self.backlogged.remove(partition);
        }
        match error {
            IggyError::FetchProgressUncertain(operation) if self.config.on_read() => {
                warn!(
                    "A read of partitions: {partitions:?} lost its reply. Operation: {operation} needs recovery."
                );
                for &partition in partitions {
                    self.lost_operations.insert(partition, operation);
                    let position = self.position(partition);
                    if let Some(context) = position.context() {
                        self.shared.progress.remember_undecoded(Checkpoint::new(
                            partition,
                            position.value,
                            context,
                        ));
                    }
                }
                self.abandoning.push((operation, partitions.to_vec()));
                self.retry(partitions);
            }
            // The server refused the commit of a committing read, which stored nothing.
            IggyError::TransientNotAccepted if self.config.on_read() => self.retry(partitions),
            IggyError::FetchProgressUncertain(operation) => {
                for &partition in partitions {
                    self.block(partition, operation);
                    self.report(ReadError::new(Some(partition), error.clone()));
                }
            }
            IggyError::PartitionLaneBlocked(partition, operation) => {
                self.block(partition, operation);
                self.report(ReadError::new(Some(partition), error));
            }
            IggyError::NotPrimary => {
                self.retry(partitions);
                self.refresh_now = true;
            }
            IggyError::ConsumerGroupPartitionNotOwned(..) => {
                for &partition in partitions {
                    self.interrupt_catch_up(partition);
                    self.positions.remove(&partition);
                    self.shared.progress.forget_undecoded(partition);
                }
                self.refresh_now = true;
                self.count_lasting(partitions, &error);
            }
            IggyError::ConsumerGroupMemberNotFound(..) => {
                self.forget_membership();
                self.retry(partitions);
                self.refresh_now = true;
            }
            IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient => {
                self.forget_membership();
                self.invalidate();
                self.report(ReadError::new(None, error));
            }
            IggyError::HistoryUnavailable => {
                for &partition in partitions {
                    let interrupted = self.interrupt_catch_up(partition);
                    self.shared.progress.forget_undecoded(partition);
                    // A position without a history reads the history of the cached route, which
                    // the driver drops after this refusal, so only a lost history stops.
                    if self.position(partition).context().is_none() {
                        warn!(
                            "Partition: {partition} has a new history, which the next read takes from a new route."
                        );
                        self.retry(&[partition]);
                        continue;
                    }
                    self.positions.remove(&partition);
                    self.retry_at.remove(&partition);
                    self.stopped.insert(partition);
                    if !interrupted {
                        self.report(ReadError::new(Some(partition), error.clone()));
                    }
                }
            }
            error if lasting_refusal(&error) => self.count_lasting(partitions, &error),
            error => {
                self.retry(partitions);
                let partition = match partitions {
                    [partition] => Some(*partition),
                    _ => None,
                };
                self.report(ReadError::new(partition, error));
            }
        }
    }

    /// A lasting refusal reads its partition again after a wait that doubles each time, and the
    /// [`MAX_LASTING_REFUSALS`]-th in a row stops it until a seek with one error.
    fn count_lasting(&mut self, partitions: &[u32], error: &IggyError) {
        for &partition in partitions {
            let refusals = self.lasting_refusals.entry(partition).or_default();
            *refusals += 1;
            let refusals = *refusals;
            if refusals < MAX_LASTING_REFUSALS {
                let delay = backoff(self.config.retry_interval, refusals);
                warn!(
                    "Partition: {partition} was refused: {error}. It is read again in {delay:?}, refusal {refusals} of {MAX_LASTING_REFUSALS}."
                );
                self.retry_at.insert(partition, Instant::now() + delay);
                continue;
            }
            self.lasting_refusals.remove(&partition);
            self.retry_at.remove(&partition);
            self.stopped.insert(partition);
            self.report(ReadError::new(Some(partition), error.clone()));
        }
    }

    /// The driver blocks the partition until `abandon_operation`. With `OnRead`, the partition
    /// also stops until a seek.
    fn block(&mut self, partition: u32, operation: u64) {
        self.shared.progress.block(partition, operation);
        self.retry_at.remove(&partition);
        if self.config.on_read() {
            self.stopped.insert(partition);
        }
    }

    fn forget_membership(&self) {
        if let Some(membership) = &self.shared.membership {
            membership.forget();
        }
    }

    fn retry(&mut self, partitions: &[u32]) {
        let retry_at = Instant::now() + self.config.retry_interval;
        for partition in partitions {
            self.retry_at.insert(*partition, retry_at);
        }
    }

    /// A read that waits for nothing answers an idle partition at once, so the partition reads
    /// again after the retry interval instead of at the round trip rate.
    fn pause_idle(&mut self, partitions: &[u32]) {
        if self.config.options.max_wait.as_micros() == 0 {
            self.retry(partitions);
        }
    }

    fn is_current(&self, epoch: u64) -> bool {
        epoch == self.epoch && self.epoch == self.shared.epoch.load(ORDERING)
    }
}

/// The fetches that one group starts in a turn of a reader that commits nothing.
struct GroupPlan {
    group: usize,
    /// A main fetch of the group is in flight.
    main: bool,
    quiet: Vec<u32>,
    /// The least recently served first.
    backlogged: Vec<u32>,
    /// The slots beyond the first one.
    extra: usize,
}

impl GroupPlan {
    /// The extra slots that the group can use, so that every fetch reads a backlogged partition.
    fn room(&self) -> usize {
        self.backlogged
            .len()
            .saturating_sub(usize::from(self.quiet.is_empty()))
    }
}

async fn next_event(events: &mut Option<Receiver<DiagnosticEvent>>) -> Option<DiagnosticEvent> {
    match events {
        Some(events) => events.next().await,
        None => std::future::pending().await,
    }
}

/// A fetch of a group reads `Offset` and `Next` positions only.
fn fetched_by_group_kind(kind: PollingKind) -> bool {
    matches!(kind, PollingKind::Offset | PollingKind::Next)
}

fn continuation_at(partition: u32, offset: u64, context: PartitionContext) -> PollingStrategy {
    PollingStrategy::continuation(partition, PollingKind::Offset, offset, context)
}

/// Keeps the messages before the first one that does not decrypt, and returns the index and
/// offset of that message with its error.
fn decrypt_messages(
    encryptor: &EncryptorKind,
    polled: &mut PolledMessages,
) -> Result<(), (usize, u64, IggyError)> {
    let partition = polled.partition_id;
    let failed = polled
        .messages
        .iter_mut()
        .enumerate()
        .find_map(|(index, message)| {
            let offset = message.header.offset;
            encryptor
                .decrypt_message(message)
                .err()
                .map(|error| (index, offset, error))
        });
    let Some((index, offset, error)) = failed else {
        return Ok(());
    };
    error!(
        "Failed to decrypt the message at offset: {offset}, partition ID: {partition}. A seek to the skip of its read error passes it. {error}"
    );
    polled.messages.truncate(index);
    polled.count = u32::try_from(index).unwrap_or(polled.count);
    Err((index, offset, error))
}

/// The records limit of a fetch whose whole reply, with its section table,
/// fits the reservation of one reply.
fn fetch_max_bytes(max_bytes: u32, targets: usize) -> u32 {
    let records = (max_bytes as usize)
        .saturating_sub(FetchMessagesResponse::head_size(targets))
        .max(MIN_DEFERRED_POLL_MAX_BYTES);
    u32::try_from(records).unwrap_or(max_bytes)
}

/// A section that ended before the head of its partition, so the next read of the partition
/// returns at once. An empty partition also reports the head 0.
fn ends_before_head(section: &FetchedSection) -> bool {
    section.next_offset <= section.current_offset
        && (section.messages_count > 0 || section.current_offset > 0)
}

/// The offset of the first record of `section`, or one before it. Offsets are
/// dense, so a fetch from there reads the section again.
fn section_start(section: &FetchedSection) -> u64 {
    section
        .next_offset
        .saturating_sub(u64::from(section.messages_count))
}

/// A read from the `Next` continuation `sent` whose reply starts at `start` in the history
/// `context` passed messages after the expected offset.
fn skipped(sent: &PollingStrategy, start: u64, context: PartitionContext) -> bool {
    sent.kind == PollingKind::Next
        && start > sent.value
        && sent
            .context()
            .is_some_and(|sent| same_incarnation_and_owner(sent, context))
}

/// A refusal that a retry soon after does not change: a missing permission, partition, topic
/// or stream, a request the server cannot serve, or a partition that the server keeps refusing
/// to this member after each refresh. Its partition reads again later each time, and stops
/// after [`MAX_LASTING_REFUSALS`] in a row.
pub(crate) fn lasting_refusal(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Unauthorized
            | IggyError::ConsumerGroupPartitionNotOwned(..)
            | IggyError::FeatureUnavailable
            | IggyError::InvalidCommand
            | IggyError::PartitionNotFound(..)
            | IggyError::StreamIdNotFound(..)
            | IggyError::StreamNameNotFound(..)
            | IggyError::TopicIdNotFound(..)
            | IggyError::TopicNameNotFound(..)
    )
}

/// The wait after the `refusals`-th lasting refusal in a row.
fn backoff(retry_interval: Duration, refusals: u32) -> Duration {
    retry_interval.saturating_mul(1 << refusals.saturating_sub(1).min(MAX_REFUSAL_BACKOFF_STEPS))
}

/// A reply that never arrived can commit batches that never reach the
/// application. So can an undeliverable reply, and a fetch that loses its
/// session or its permission after its first read.
fn may_hide_commit(error: &IggyError) -> bool {
    undeliverable(error)
        || matches!(
            error,
            IggyError::TransientNotCommitted
                | IggyError::FetchProgressUncertain(_)
                | IggyError::Unauthenticated
                | IggyError::StaleClient
                | IggyError::Unauthorized
        )
}

/// A read alone from an offset holds one message, so a reply of it that does
/// not decode names that message.
fn names_message(alone: bool, start: &PollingStrategy, error: &IggyError) -> bool {
    alone && start.kind == PollingKind::Offset && undeliverable(error)
}

/// A reply that arrived but does not decode, or that the worker drops for
/// holding more messages than it asked for.
fn undeliverable(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::InvalidNumberEncoding
            | IggyError::InvalidMessagePayloadLength
            | IggyError::InvalidMessagesCount
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE;

    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::http::http_client::HttpClient;
    use bytes::Bytes;
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::WirePollingStrategy;
    use iggy_binary_protocol::codes::POLL_MESSAGES_DEFERRED_CODE;
    use iggy_binary_protocol::requests::messages::DeferredPollMessagesRequest;
    use iggy_common::locking::IggyRwLock;
    use iggy_common::{Aes256GcmEncryptor, AutoCommit, Consumer, Identifier, IggyTimestamp};
    use tokio::task::yield_now;
    use tokio::time::{advance, timeout};

    use crate::clients::reader::fixtures::{
        Section, batch, builder_for, builder_on, connect_to_one_replica, message_at, refused,
        sections, served, stored_offsets,
    };
    use crate::clients::reader_builder::{DEFAULT_MAX_WAIT, ReaderBuilder};
    use crate::links::fake_cluster::{Answer, endpoint};

    type Replies = mpsc::UnboundedReceiver<Reply>;

    const GROUP_ID: u32 = 1;
    const SESSION: u64 = 1;
    const VERSION: u64 = 1;
    const OPERATION: u64 = 42;
    const KEY: [u8; 32] = [1; 32];
    const RELEASE_TIMEOUT: Duration = Duration::from_secs(1);
    /// The messages that a partition holds after a section of [`behind_head`].
    const BACKLOG: u64 = 100;

    #[tokio::test(start_paused = true)]
    async fn byte_and_message_reservations_cover_inflight_and_queued_replies() {
        const BATCH_COUNT: u32 = 10;
        const REPLY_BYTES: u32 = 1024;
        const SLOTS: usize = 2;
        for (bytes, messages) in [
            (REPLY_BYTES * 2, BATCH_COUNT * 10),
            (REPLY_BYTES * 10, BATCH_COUNT * 2),
        ] {
            let (mut worker, mut receiver) = worker_of(
                standalone([0, 1, 2, 3])
                    .options(PollOptions {
                        count: BATCH_COUNT,
                        max_bytes: REPLY_BYTES,
                        ..PollOptions::default()
                    })
                    .prefetch_bytes(bytes)
                    .prefetch_messages(messages),
            );
            assert_eq!(worker.config.slots, SLOTS);
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0, 1, 2, 3])));
            worker.schedule();
            assert_eq!(worker.tasks.len(), SLOTS, "reserve before sending");
            assert_eq!(worker.capacity.available_permits(), 0);
            fail_reads(&mut worker).await;
            worker.flush_errors();
            assert_eq!(
                receiver.len(),
                SLOTS,
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
            fail_reads(&mut worker).await;
            worker.invalidate();
            worker.report(ReadError::new(None, IggyError::Disconnected));
            worker.flush_errors();
            assert_eq!(
                worker.pending_errors.len(),
                1,
                "an error waits for a slot under backpressure"
            );
            let stale = receiver.recv().await.unwrap();
            assert_ne!(stale.epoch, worker.epoch);
            let reported = receiver.recv().await.unwrap();
            assert_eq!(reported.epoch, worker.epoch);
            assert!(matches!(
                reported.result,
                Err(Refusal {
                    error: ReadError {
                        partition_id: None,
                        error: IggyError::Disconnected,
                        ..
                    },
                    ..
                })
            ));
            drop((stale, reported));
            worker.flush_errors();
            let error = next_error(&mut receiver);
            assert!(
                error.partition_id.is_some() && error.error == IggyError::CannotReadMessage,
                "the error of a partition outlives the epoch"
            );
        }
    }

    #[tokio::test]
    async fn refused_polls_reach_the_application_and_refused_refreshes_retry_quietly() {
        let (mut worker, mut receiver) = worker_of(standalone([0]));
        for error in [
            IggyError::TransientNotAccepted,
            IggyError::DeferredPollCapacityExceeded,
        ] {
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                worker.position(0),
                0,
                Err(error.clone()),
            ));
            assert!(worker.retry_at.contains_key(&0), "the partition retries");
            worker.flush_errors();
            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(Some(0), error.clone()),
                "a refused poll reaches the application"
            );
            worker.refuse_refresh(error);
            worker.flush_errors();
            assert!(
                receiver.try_recv().is_err(),
                "the next refresh retries a transient refusal without an error"
            );
        }
    }

    #[tokio::test]
    async fn given_an_empty_next_probe_when_seeking_after_resolution_should_resolve_the_new_start_again()
     {
        let (mut worker, receiver) = worker_of(committing(standalone([0])));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            PollingStrategy::next(),
            0,
            Ok(batch(0, [], 20)),
        ));
        assert!(worker.needs_start(0));
        assert_eq!(worker.position(0), PollingStrategy::next());
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            PollingStrategy::next(),
            0,
            Ok(batch(0, [5], 20)),
        ));
        assert_eq!(worker.position(0), next_at(0, 5));
        assert!(!worker.needs_start(0));
        assert!(receiver.is_empty());
        assert!(worker.shared.progress.last_committed(0).is_none());
        worker.shared.progress.seek(0, PollingStrategy::next());
        worker.apply_seeks();
        assert!(worker.needs_start(0));
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            PollingStrategy::next(),
            0,
            Ok(batch(0, [12], 20)),
        ));
        assert_eq!(worker.position(0), next_at(0, 12));
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn given_a_dynamic_on_read_start_when_polling_alone_should_resolve_it_before_committing()
    {
        for start in [
            PollingStrategy::last(),
            PollingStrategy::timestamp(IggyTimestamp::from(1000)),
        ] {
            let (mut worker, _receiver) = worker_of(committing(standalone([0]).start(start)));
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            assert_eq!(worker.active[&0].kind, FetchKind::Check, "{start:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn committing_poll_waits_for_demand_and_survives_invalidation() {
        let (mut worker, mut receiver) = worker_of(committing(standalone([0, 1])));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0, 1])));
        if worker.config.on_read() {
            for (partition, offset) in [(0, 3), (1, 0)] {
                worker
                    .positions
                    .insert(partition, next_at(partition, offset));
            }
        }
        worker.schedule();
        assert!(
            worker.tasks.is_empty(),
            "no poll before the application asks"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.schedule();
        assert_eq!(
            worker.tasks.len(),
            2,
            "one committing poll per idle partition"
        );
        let started = worker.epoch;
        worker.invalidate();
        while let Some(completed) = worker.tasks.join_next().await {
            let Completed::Poll(completed) = completed.unwrap() else {
                panic!("a reader without readiness groups polls one partition");
            };
            assert_eq!(completed.epoch, started, "the poll survives invalidation");
            assert!(completed.commit.is_some(), "the poll commits its batch");
            worker.complete(Completed::Poll(completed));
        }
        worker.flush_errors();
        assert!(receiver.is_empty(), "an invalidated poll drops its error");
        assert!(worker.retry_at.is_empty());

        let mut late = polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 3),
            0,
            Ok(batch(0, [3, 4], 9)),
        );
        if let Completed::Poll(poll) = &mut late {
            poll.epoch = started;
        }
        worker.complete(late);
        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            [3, 4],
            "the server committed the batch, so the application gets it"
        );
        assert!(
            !worker.shared.demand.load(ORDERING),
            "the batch answers the demand"
        );
        assert_eq!(worker.position(0), next_at(0, 5));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [5], 9)),
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [5]);
        assert_eq!(worker.position(0), next_at(0, 6));
    }

    /// A reply from `Next` that starts past the expected offset shows that a lost read committed
    /// the messages in between. The partition reads them and the batch again by offset without a
    /// commit, then reads `Next` again.
    #[tokio::test]
    async fn given_a_received_batch_behind_a_lost_range_when_ownership_changes_should_report_the_gap_and_keep_the_batch()
     {
        for fetch in [false, true] {
            for reassigned in [false, true] {
                let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
                assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
                worker.positions.insert(0, next_at(0, 5));
                worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
                let completed = if fetch {
                    reply(
                        &worker,
                        1,
                        vec![(0, next_at(0, 5), worker.generation(0))],
                        Ok(sections(&[served(0, &[7, 8], 9)])),
                    )
                } else {
                    polled(
                        &worker,
                        FetchKind::Alone,
                        next_at(0, 5),
                        0,
                        Ok(batch(0, [7, 8], 8)),
                    )
                };
                worker.complete(completed);
                assert!(receiver.is_empty());
                if reassigned {
                    assert!(worker.apply(grouped(&worker, SESSION, VERSION + 1, Vec::new())));
                } else {
                    worker.refuse(&[0], IggyError::ConsumerGroupPartitionNotOwned(1, 0));
                }
                worker.flush_errors();
                let error = next_error(&mut receiver);
                assert_eq!(error.error, IggyError::FetchProgressUncertain(OPERATION));
                assert_eq!(error.partition_id, Some(0));
                assert_eq!(error.skip, Some(at(0, 7)));
                assert_eq!(offsets(&next_batches(&mut receiver)[0]), [7, 8]);
                assert!(worker.catch_up.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn given_a_partial_catch_up_when_closing_should_rewind_over_the_retained_batch() {
        let (client, cluster) = connect_to_one_replica().await;
        let (mut worker, mut receiver) = worker_of(
            committing(builder_on(client, Consumer::default()).partitions([0]))
                .prefetch_messages(1),
        );
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Ok(batch(0, [5], 8)),
        ));
        let delivered = next_batches(&mut receiver).remove(0).checkpoint().unwrap();
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.finish_fetches().await;
        worker
            .shared
            .progress
            .rewind(Vec::new(), None, true)
            .await
            .unwrap();
        assert_eq!(stored_offsets(&cluster), [5]);
    }

    #[tokio::test]
    async fn given_a_partly_recovered_batch_when_ownership_changes_should_deliver_only_its_unread_messages()
     {
        let (mut worker, mut receiver) =
            worker_of(committing(standalone([0])).prefetch_messages(1));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        assert_eq!(worker.capacity.available_permits(), 0);
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Ok(batch(0, [5, 6, 7], 8)),
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [5, 6, 7]);
        worker.refuse(&[0], IggyError::ConsumerGroupPartitionNotOwned(1, 0));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [8]);
        worker.flush_errors();
        assert!(receiver.is_empty());
        assert_eq!(worker.capacity.available_permits(), 1);
        assert_eq!(worker.recovery_capacity.available_permits(), 1);
    }

    /// The lost read still blocks its partition on the client, so the reader abandons it also
    /// when a seek replaced its generation, or a new epoch its fetch state.
    #[tokio::test]
    async fn given_a_lost_committing_read_of_a_replaced_generation_when_completed_should_still_abandon_it()
     {
        for (case, fetch) in [
            ("poll after a seek", false),
            ("fetch section after a seek", true),
        ] {
            let (mut worker, _receiver) = worker_of(committing(builder_for(group())));
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
            worker.positions.insert(0, next_at(0, 5));
            let completed = if fetch {
                let mut lost = sections(&[served(0, &[], 5)]);
                lost[0].status = Err(IggyError::FetchProgressUncertain(OPERATION));
                reply(
                    &worker,
                    1,
                    vec![(0, next_at(0, 5), worker.generation(0))],
                    Ok(lost),
                )
            } else {
                let Completed::Poll(mut completed) = polled(
                    &worker,
                    FetchKind::Alone,
                    next_at(0, 5),
                    0,
                    Ok(batch(0, [], 5)),
                ) else {
                    panic!("a poll");
                };
                completed.result = Err(DecodeError::without_record(
                    IggyError::FetchProgressUncertain(OPERATION),
                ));
                Completed::Poll(completed)
            };
            worker.shared.progress.seek(0, PollingStrategy::offset(9));
            worker.apply_seeks();

            worker.complete(completed);

            assert_eq!(
                worker.abandoning,
                [(OPERATION, vec![0])],
                "{case}: the lost operation is abandoned"
            );
        }
        let (mut worker, _receiver) = worker_of(committing(builder_for(group())));
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
        worker.positions.insert(0, next_at(0, 5));
        let completed = reply(
            &worker,
            1,
            vec![(0, next_at(0, 5), worker.generation(0))],
            Err(IggyError::FetchProgressUncertain(OPERATION)),
        );
        worker.invalidate();

        worker.complete(completed);

        assert_eq!(
            worker.abandoning,
            [(OPERATION, vec![0])],
            "a lost fetch of an older epoch is abandoned"
        );
    }

    /// The server stored the offset of a committing read whose batch a seek dropped, so the
    /// close moves the offset back to the last delivered message.
    #[tokio::test]
    async fn given_a_committing_read_whose_batch_a_seek_dropped_when_completed_should_leave_its_store_to_the_close()
     {
        for fetch in [false, true] {
            let (mut worker, receiver) = worker_of(committing(builder_for(group())));
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
            let delivered = Checkpoint::new(0, 4, PartitionContext::default());
            worker.shared.progress.deliver(&delivered);
            worker.shared.progress.record_commit(delivered);
            worker.positions.insert(0, next_at(0, 5));
            let completed = if fetch {
                reply(
                    &worker,
                    1,
                    vec![(0, next_at(0, 5), worker.generation(0))],
                    Ok(sections(&[served(0, &[5, 6], 7)])),
                )
            } else {
                polled(
                    &worker,
                    FetchKind::Alone,
                    next_at(0, 5),
                    0,
                    Ok(batch(0, [5, 6], 6)),
                )
            };
            worker.shared.progress.seek(0, PollingStrategy::offset(20));
            worker.apply_seeks();

            worker.complete(completed);

            assert!(
                receiver.is_empty(),
                "fetch: {fetch}: the seek drops the batch"
            );
            assert_eq!(
                worker.shared.progress.last_committed(0),
                None,
                "fetch: {fetch}: the close stores the delivered offset again"
            );
        }
    }

    /// A catch-up reply that does not decode has no message boundary, so the reader reads the
    /// range one message at a time, and the read of the bad message names it with a skip.
    #[tokio::test]
    async fn given_a_catch_up_reply_that_does_not_decode_when_completed_should_read_alone_and_name_the_bad_message()
     {
        let (mut worker, _receiver) = worker_of(committing(standalone([0])).prefetch_messages(2));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        worker.retry_at.clear();

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Err(IggyError::InvalidNumberEncoding),
        ));

        assert!(
            worker.read_alone.contains(&0) && worker.pending_errors.is_empty(),
            "the range is read one message at a time before any error"
        );
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Err(IggyError::InvalidNumberEncoding),
        ));
        let error = &worker.pending_errors[&Some(0)].error;
        assert_eq!(
            (&error.error, error.skip),
            (&IggyError::InvalidNumberEncoding, Some(at(0, 6)))
        );
    }

    /// The history no longer holds the range, so the reader reports it once and reads on after
    /// the retained batch, instead of reading the same empty range again at once.
    #[tokio::test]
    async fn given_a_catch_up_read_that_finds_nothing_when_completed_should_report_the_range_and_read_on_after_it()
     {
        let (mut worker, mut receiver) =
            worker_of(committing(standalone([0])).prefetch_messages(1));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        assert_eq!(worker.catching_up(0), Some(8));

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Ok(batch(0, [], 8)),
        ));

        assert_eq!(worker.catching_up(0), None, "the catch-up ends");
        let error = next_error(&mut receiver);
        assert_eq!(
            (error.partition_id, error.error, error.skip),
            (
                Some(0),
                IggyError::FetchProgressUncertain(OPERATION),
                Some(at(0, 7))
            )
        );
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [7, 8]);
        assert_eq!(worker.position(0), next_at(0, 9));
    }

    #[tokio::test]
    async fn given_a_retained_batch_when_seeking_should_discard_it_and_its_lost_range() {
        let (mut worker, mut receiver) =
            worker_of(committing(standalone([0])).prefetch_messages(1));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        worker.shared.progress.seek(0, PollingStrategy::offset(20));
        worker.apply_seeks();
        worker.refuse(&[0], IggyError::ConsumerGroupPartitionNotOwned(1, 0));
        worker.flush_errors();
        assert!(receiver.try_recv().is_err());
        assert!(worker.retained_batches.is_empty());
        assert!(worker.catch_up.is_empty());
        assert_eq!(worker.capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn given_an_interrupted_catch_up_when_its_late_reply_arrives_should_not_deliver_it_after_the_retained_batch()
     {
        let (mut worker, mut receiver) =
            worker_of(committing(standalone([0])).prefetch_messages(1));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [7, 8], 8)),
        ));
        let mut late = polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Ok(batch(0, [5, 6], 8)),
        );
        if let Completed::Poll(poll) = &mut late {
            poll.commit = None;
        }
        worker.refuse(&[0], IggyError::ConsumerGroupPartitionNotOwned(1, 0));
        worker.complete(late);
        assert!(matches!(
            next_error(&mut receiver).error,
            IggyError::FetchProgressUncertain(_)
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [7, 8]);
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn given_a_lost_on_read_tail_when_the_next_poll_is_empty_should_recover_the_skipped_messages()
     {
        let (mut worker, receiver) = worker_of(committing(standalone([0])));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [], 7)),
        ));
        assert_eq!(worker.position(0), at(0, 5));
        assert_eq!(worker.catching_up(0), Some(7));
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn given_an_on_read_poll_that_skips_offsets_when_completed_should_read_them_again_without_a_commit()
     {
        const EXPECTED: u64 = 6;
        const COMMITTED: u64 = 9;
        let (client, cluster) = connect_to_one_replica().await;
        let (mut worker, mut receiver) = worker_of(committing(
            builder_on(client, Consumer::default()).partitions([0]),
        ));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        let delivered = Checkpoint::new(0, EXPECTED - 1, PartitionContext::default());
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.positions.insert(0, next_at(0, EXPECTED));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.retry_at.clear();

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, EXPECTED),
            0,
            Ok(batch(0, [COMMITTED - 1, COMMITTED], COMMITTED + 1)),
        ));

        assert!(
            receiver.is_empty(),
            "the batch waits for the skipped messages"
        );
        assert_eq!(worker.position(0), at(0, EXPECTED));
        assert_eq!(
            worker.shared.progress.last_committed(0),
            None,
            "the server committed messages that never arrived, so the close rewinds"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.tasks.join_next().await.unwrap().unwrap();
        let catch_up = DeferredPollMessagesRequest::decode_from(
            &cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0].body,
        )
        .unwrap();
        assert_eq!(
            (
                catch_up.poll.strategy,
                catch_up.poll.count,
                catch_up.poll.auto_commit
            ),
            (
                WirePollingStrategy::offset(EXPECTED),
                u32::try_from(COMMITTED - EXPECTED + 1).unwrap(),
                false
            ),
            "the skipped messages and the batch, by offset, without a commit"
        );
        worker.active.clear();
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, EXPECTED),
            0,
            Ok(batch(0, EXPECTED..=COMMITTED, COMMITTED + 1)),
        ));
        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            (EXPECTED..=COMMITTED).collect::<Vec<_>>()
        );
        assert_eq!(worker.position(0), next_at(0, COMMITTED + 1));
        assert!(worker.catch_up.is_empty());
    }

    /// The application or another session stored an offset ahead of the reader.
    #[tokio::test]
    async fn given_no_lost_read_when_a_committing_read_starts_past_the_expected_offset_should_deliver_from_there()
     {
        for fetch in [false, true] {
            let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
            worker.positions.insert(0, next_at(0, 5));
            let completed = if fetch {
                reply(
                    &worker,
                    1,
                    vec![(0, next_at(0, 5), worker.generation(0))],
                    Ok(sections(&[served(0, &[9, 10], 11)])),
                )
            } else {
                polled(
                    &worker,
                    FetchKind::Alone,
                    next_at(0, 5),
                    0,
                    Ok(batch(0, [9, 10], 10)),
                )
            };

            worker.complete(completed);

            assert_eq!(
                offsets(&next_batches(&mut receiver)[0]),
                [9, 10],
                "fetch: {fetch}: the read goes on from the stored offset"
            );
            assert_eq!(worker.catching_up(0), None);
            assert_eq!(worker.position(0), next_at(0, 11));
        }
    }

    #[tokio::test]
    async fn given_a_lost_read_when_a_later_committing_read_returns_the_expected_offset_should_accept_the_next_jump()
     {
        let (mut worker, mut receiver) = worker_of(committing(standalone([0])));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [5, 6], 6)),
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [5, 6]);

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 7),
            0,
            Ok(batch(0, [12], 12)),
        ));

        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            [12],
            "the store of the later read fenced out the lost one"
        );
        assert_eq!(worker.catching_up(0), None);
    }

    #[tokio::test]
    async fn given_a_committing_fetch_that_lost_its_session_when_the_next_read_skips_offsets_should_read_them_again()
     {
        let (mut worker, receiver) = worker_of(committing(builder_for(group())));
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
        worker.positions.insert(0, next_at(0, 5));
        worker.complete(reply(
            &worker,
            1,
            vec![(0, next_at(0, 5), worker.generation(0))],
            Err(IggyError::Unauthenticated),
        ));

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            next_at(0, 5),
            0,
            Ok(batch(0, [8, 9], 9)),
        ));

        assert!(
            receiver.is_empty(),
            "the batch waits for the skipped messages"
        );
        assert_eq!(worker.position(0), at(0, 5));
        assert_eq!(worker.catching_up(0), Some(9));
    }

    #[tokio::test]
    async fn given_a_standalone_reader_without_readiness_groups_when_scheduled_should_keep_one_poll_per_partition_within_the_budget()
     {
        const PARTITIONS: [u32; 3] = [0, 1, 2];
        const BUDGET_SLOTS: usize = 2;
        let budget = PollOptions::default().count * u32::try_from(BUDGET_SLOTS).unwrap();
        for (builder, polled_partitions) in [
            (standalone(PARTITIONS), &PARTITIONS[..]),
            (
                standalone(PARTITIONS).prefetch_messages(budget),
                &PARTITIONS[..BUDGET_SLOTS],
            ),
        ] {
            let (mut worker, _receiver) = worker_of(builder);
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert_eq!(
                worker.config.slots.min(PARTITIONS.len()),
                polled_partitions.len()
            );
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, PARTITIONS)));
            assert!(worker.groups.is_empty());
            worker.schedule();
            worker.schedule();
            assert_eq!(
                worker.tasks.len(),
                polled_partitions.len(),
                "one waiting poll per partition, within the budget"
            );
            assert_eq!(
                worker.active.keys().copied().collect::<Vec<_>>(),
                polled_partitions
            );
            assert!(
                worker
                    .active
                    .values()
                    .all(|active| active.kind == FetchKind::Alone)
            );
        }
    }

    /// The server returns a record above `max_bytes` alone in the reply of a poll, so a poll
    /// refused for its byte limit is final.
    #[tokio::test]
    async fn given_a_poll_refused_for_its_byte_limit_when_completed_should_report_it_at_once() {
        for (builder, kind) in [
            (standalone([0]), FetchKind::Alone),
            (committing(standalone([0])), FetchKind::Alone),
            (standalone([0]), FetchKind::Check),
        ] {
            let (mut worker, mut receiver) = worker_of(builder);
            worker.complete(polled(
                &worker,
                kind,
                worker.position(0),
                0,
                Err(IggyError::InvalidSizeBytes),
            ));
            worker.flush_errors();
            assert!(!worker.read_alone.contains(&0), "{kind:?}");
            assert!(worker.retry_at.contains_key(&0), "{kind:?}");
            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(Some(0), IggyError::InvalidSizeBytes),
                "{kind:?}"
            );
        }
    }

    /// The reads of one fetch share one turn for a first record above `max_bytes`, so a section
    /// that lost the turn is read again by a poll of its partition, which always has one.
    #[tokio::test]
    async fn given_a_fetch_section_refused_for_its_byte_limit_when_scheduled_should_poll_its_partition_alone()
     {
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let (mut worker, mut receiver) = worker_of(builder);
            let on_read = worker.config.on_read();
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
            worker.shared.demand.store(true, ORDERING);
            if worker.config.on_read() {
                for (partition, offset) in [(0, 0), (1, 0)] {
                    worker
                        .positions
                        .insert(partition, next_at(partition, offset));
                }
            }
            worker.schedule();
            worker.complete(reply_of(
                &worker,
                0,
                &[refused(0, &IggyError::InvalidSizeBytes)],
            ));
            worker.flush_errors();
            assert!(worker.read_alone.contains(&0));
            assert!(
                receiver.try_recv().is_err() && worker.retry_at.is_empty(),
                "only the poll of the partition reports an error"
            );

            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            assert_eq!(worker.active[&0].kind, FetchKind::Alone);
            assert_eq!(
                worker.active[&1].kind,
                FetchKind::Main,
                "on_read {on_read}: the other partitions of the group read beside the record"
            );
        }
    }

    #[tokio::test]
    async fn new_generation_resumes_after_the_last_delivered_message() {
        let (mut worker, mut receiver) = worker_of(standalone([0]).start(PollingStrategy::first()));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        for (delivered, resumed) in [(false, PollingStrategy::offset(0)), (true, at(0, 1))] {
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                worker.position(0),
                0,
                Ok(batch(0, [0, 1, 2], 9)),
            ));
            let batches = next_batches(&mut receiver);
            if delivered {
                worker
                    .shared
                    .progress
                    .deliver(&batches[0].checkpoint_at(0).unwrap());
            }
            let stale = assigned(&worker, SESSION, VERSION, [0]);
            worker.invalidate();
            assert!(
                !worker.apply(stale),
                "a refresh from before the new epoch is read again"
            );
            assert!(worker.partitions.is_empty());
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
            assert_eq!(worker.partitions, [0]);
            assert_eq!(
                worker.position(0),
                resumed,
                "the batch that the application did not take is fetched again"
            );
        }
    }

    #[tokio::test]
    async fn rebalance_keeps_the_position_of_a_partition_that_stays_with_the_member() {
        const NEW_SESSION: u64 = SESSION + 1;
        let (mut worker, mut receiver) =
            worker_of(builder_for(group()).start(PollingStrategy::first()));
        let progress = Arc::clone(&worker.shared.progress);
        let generation = |partition| progress.generation(partition);
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0, 1, 2])));
        for partition in [0, 2] {
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                worker.position(partition),
                partition,
                Ok(batch(partition, [0, 1, 2], 9)),
            ));
            let delivered = next_batches(&mut receiver)[0].checkpoint_at(0).unwrap();
            progress.deliver(&delivered);
        }
        let before = [0, 1, 2].map(generation);

        assert!(worker.apply(assigned(&worker, SESSION, VERSION + 1, [0, 1, 3])));
        assert_eq!(
            worker.position(0),
            at(0, 1),
            "the partition continues after its last delivered message"
        );
        assert_eq!(
            [generation(0), generation(1)],
            [before[0], before[1]],
            "a partition that stays keeps its generation"
        );
        assert_eq!(
            generation(2),
            None,
            "a revoked partition loses its generation"
        );
        assert!(
            generation(3).is_some() && !before.contains(&generation(3)),
            "a newly assigned partition starts a new generation"
        );
        assert!(
            !worker.positions.contains_key(&2),
            "a partition that left starts over when it comes back"
        );

        assert!(worker.apply(assigned(&worker, SESSION, VERSION + 2, [0, 1, 2, 3])));
        assert!(
            generation(2).is_some() && generation(2) != before[2],
            "a partition that comes back starts a new generation"
        );

        assert!(worker.apply(assigned(&worker, NEW_SESSION, VERSION + 2, [0, 1, 2, 3])));
        assert_eq!(
            worker.position(0),
            PollingStrategy::offset(0),
            "a new session owns every partition anew"
        );
        assert_ne!(generation(0), before[0], "in a new generation");
    }

    #[tokio::test]
    async fn committing_member_retries_a_lost_fetch_until_the_partition_changes_owner() {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0], vec![1]])));
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.complete(reply_of(&worker, 0, &[served(0, &[3, 4], 5)]));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [3, 4]);
        let retried = worker.position(0);

        worker.complete(failure_of(
            &worker,
            1,
            IggyError::FetchProgressUncertain(OPERATION),
        ));
        assert!(
            worker.retry_at.contains_key(&1)
                && !worker.stopped.contains(&1)
                && worker.shared.progress.blocked(1).is_none(),
            "the next read of a lost reply finds what it committed"
        );
        worker.flush_errors();
        assert!(receiver.is_empty());

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.complete(failure_of(&worker, 0, IggyError::TransientNotCommitted));
        assert!(
            worker.retry_at.contains_key(&0)
                && !worker.stopped.contains(&0)
                && worker.shared.progress.blocked(0).is_none(),
            "a lost reply is a transient, so the partition retries"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::TransientNotCommitted)
        );

        assert!(worker.apply(grouped(&worker, SESSION, VERSION + 1, vec![vec![0]])));
        assert_eq!(
            worker.position(0),
            retried,
            "`Next` would start after the batches that the server may have committed"
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.complete(reply_of(
            &worker,
            0,
            &[refused(0, &IggyError::ConsumerGroupPartitionNotOwned(1, 0))],
        ));
        assert_eq!(
            worker.position(0),
            PollingStrategy::next(),
            "a partition that moved away and back resolves its position again"
        );
        assert!(worker.refresh_now);
        worker.flush_errors();
        assert!(receiver.is_empty(), "a moved partition reports nothing");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_message_that_does_not_decrypt_when_polled_should_deliver_the_messages_before_it_and_skip_it_after_a_seek()
     {
        const FAILED: u64 = 4;
        let encryptor = encryptor();
        let (mut worker, mut receiver) = encrypted_worker(standalone([0]), &encryptor);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            worker.position(0),
            0,
            Ok(sealed_batch(&encryptor, &[3, FAILED, 5], Some(FAILED))),
        ));
        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            [3],
            "the messages before the failed one arrive first"
        );
        worker.flush_errors();
        let skip = at(0, FAILED + 1);
        assert_eq!(
            next_error(&mut receiver),
            ReadError {
                partition_id: Some(0),
                error: IggyError::CannotDecryptData,
                skip: Some(skip),
                offset: None,
                record: None
            }
        );
        assert_eq!(
            worker.position(0),
            at(0, FAILED),
            "the partition reads the failed message again"
        );
        assert_eq!(
            worker.retry_at.get(&0).copied(),
            Some(Instant::now() + worker.config.retry_interval)
        );

        advance(worker.config.retry_interval).await;
        worker
            .retry_at
            .retain(|_, deadline| *deadline > Instant::now());
        worker.schedule();
        assert!(
            worker.active.contains_key(&0),
            "the partition retries after the retry interval"
        );
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, FAILED),
            0,
            Ok(sealed_batch(&encryptor, &[FAILED, 5], Some(FAILED))),
        ));
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver).skip,
            Some(skip),
            "the message fails again until a seek"
        );
        assert!(receiver.is_empty());

        worker.shared.progress.seek(0, skip);
        worker.apply_seeks();
        assert!(!worker.retry_at.contains_key(&0), "a seek ends the retry");
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            skip,
            0,
            Ok(sealed_batch(&encryptor, &[5], None)),
        ));
        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            [5],
            "the seek reads on after the failed message"
        );
        assert_eq!(worker.position(0), at(0, 6));
    }

    #[tokio::test]
    async fn committing_member_sends_one_fetch_per_group_on_demand() {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(
            &worker,
            SESSION,
            VERSION,
            vec![vec![0, 2], vec![1]]
        )));
        if worker.config.on_read() {
            for (partition, offset) in [(0, 0), (1, 0), (2, 0)] {
                worker
                    .positions
                    .insert(partition, next_at(partition, offset));
            }
        }
        worker.schedule();
        assert!(
            worker.tasks.is_empty(),
            "no fetch before the application asks"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.schedule();
        assert_eq!(worker.tasks.len(), 2, "one fetch per primary group");
        assert_eq!(fetch_of(&worker, 0), fetch_of(&worker, 2));

        worker.complete(failure_of(&worker, 1, IggyError::TransientNotAccepted));
        assert!(worker.retry_at.contains_key(&1), "the partition retries");
        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "a refused committing fetch stored nothing, so it retries without an error"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            !worker.active.contains_key(&1),
            "a refused partition waits for its retry"
        );

        let epoch = worker.epoch;
        worker.complete(reply_of(
            &worker,
            0,
            &[
                served(0, &[0], 1),
                refused(2, &IggyError::ConsumerGroupPartitionNotOwned(1, 2)),
            ],
        ));
        assert_eq!(
            worker.epoch, epoch,
            "a moved partition keeps the other sections of its reply"
        );
        assert!(
            worker.refresh_now && !worker.positions.contains_key(&2),
            "a moved partition refreshes the assignment at once"
        );
        assert_eq!(
            next_batches(&mut receiver)[0].partition_id,
            0,
            "the server committed the batch, so the application gets it"
        );
        worker.flush_errors();
        assert!(receiver.is_empty(), "a moved partition reports nothing");
    }

    #[tokio::test]
    async fn group_fetch_starts_the_next_fetch_from_the_section_table() {
        let (mut worker, mut receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        let partitions = worker.partitions.clone();
        worker.schedule();
        assert_eq!(
            worker.tasks.len(),
            1,
            "one fetch reads every partition of the group"
        );
        let first = fetch_of(&worker, 0);
        assert!(
            partitions
                .iter()
                .all(|partition| fetch_of(&worker, *partition) == first)
        );

        worker.complete(reply_of(&worker, 3, &[served(3, &[5, 6], 7)]));
        let next = fetch_of(&worker, 3);
        assert!(
            next != first
                && partitions
                    .iter()
                    .all(|partition| fetch_of(&worker, *partition) == next),
            "the next fetch reads every partition again"
        );
        assert_eq!(worker.position(3), at(3, 7));
        let batches = next_batches(&mut receiver);
        assert_eq!(
            (batches[0].partition_id, offsets(&batches[0])),
            (3, vec![5, 6])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn partitions_that_turn_idle_share_a_second_fetch_until_the_main_fetch_ends() {
        let (mut worker, _receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.retry(&[0, 1]);
        worker.schedule();
        let main = worker.active[&2].task.id();
        assert!(!worker.active.contains_key(&0));

        worker.retry_at.remove(&0);
        worker.schedule();
        let first = worker.active[&0].task.id();
        assert!(
            first != main && worker.active[&0].kind == FetchKind::Second,
            "a partition that turns idle while the main fetch parks goes into a second fetch"
        );
        assert!(
            timeout(Duration::from_millis(1), worker.tasks.join_next())
                .await
                .is_err(),
            "a second fetch cancels nothing"
        );

        worker.retry_at.remove(&1);
        worker.schedule();
        let second = worker.active[&1].task.id();
        assert_eq!(
            worker.active[&0].task.id(),
            second,
            "partitions that turn idle share the second fetch"
        );
        assert_eq!(
            worker.active[&2].task.id(),
            main,
            "the main fetch keeps its partitions"
        );
        let replaced = worker.tasks.join_next_with_id().await.unwrap();
        assert!(
            replaced.is_err_and(|error| error.is_cancelled() && error.id() == first),
            "a newly idle partition restarts only the second fetch"
        );

        worker.complete(reply_of(&worker, 2, &[]));
        let next = fetch_of(&worker, 0);
        assert!(
            worker.active[&0].kind == FetchKind::Main
                && worker
                    .partitions
                    .iter()
                    .all(|partition| fetch_of(&worker, *partition) == next),
            "the next main fetch takes every idle partition"
        );
        let replaced = worker.tasks.join_next_with_id().await.unwrap();
        assert!(
            replaced.is_err_and(|error| error.is_cancelled() && error.id() == second),
            "the next main fetch cancels the second"
        );
    }

    #[tokio::test]
    async fn changed_groups_restart_only_the_fetches_that_commit_nothing() {
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let (mut worker, _receiver) = worker_of(builder);
            let on_read = worker.config.on_read();
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            worker.shared.demand.store(true, ORDERING);
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
            if worker.config.on_read() {
                for (partition, offset) in [(0, 0), (1, 0)] {
                    worker
                        .positions
                        .insert(partition, next_at(partition, offset));
                }
            }
            worker.schedule();
            let fetch = worker.active[&0].task.id();
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
            worker.schedule();
            assert_eq!(
                worker.active[&0].task.id(),
                fetch,
                "a refresh that keeps the groups keeps the fetches"
            );

            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0], vec![1]])));
            worker.schedule();
            if on_read {
                assert!(
                    worker.active[&0].task.id() == fetch && worker.tasks.len() == 1,
                    "a committing fetch runs on, and its partitions wait for its reply"
                );
            } else {
                assert!(
                    worker.active[&0].task.id() != fetch
                        && fetch_of(&worker, 0) != fetch_of(&worker, 1),
                    "the fetches start over in the new groups"
                );
            }

            worker.invalidate();
            assert_eq!(
                worker.tasks.len(),
                usize::from(on_read),
                "a rewind cancels only the fetches that commit nothing"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn committing_shutdown_waits_for_its_fetch_and_cancels_its_checks() {
        let (mut worker, receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        worker.positions.insert(1, PollingStrategy::last());
        let delivered = Checkpoint::new(0, 4, PartitionContext::default());
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.shared.demand.store(true, ORDERING);
        worker.positions.insert(0, next_at(0, 5));
        worker.schedule();
        assert_eq!(worker.active[&0].kind, FetchKind::Main);
        assert_eq!(
            worker.active[&1].kind,
            FetchKind::Check,
            "a last start resolves with a poll that commits nothing"
        );

        let started = Instant::now();
        worker.finish_fetches().await;
        assert!(worker.tasks.is_empty());
        assert_eq!(
            started.elapsed(),
            worker.request_timeout(),
            "close waits for the committing fetch until its deadline and no longer"
        );
        assert_eq!(
            worker.shared.progress.last_committed(0),
            Some(delivered),
            "a fetch that never left the client hides no commit"
        );
        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "a committing fetch that stored nothing retries without an error"
        );
        assert!(
            !worker.retry_at.contains_key(&1),
            "close cancels the check, which commits nothing"
        );
    }

    #[tokio::test]
    async fn served_targets_go_last_and_next_targets_continue_from_an_offset() {
        let (mut worker, _receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        let partitions = worker.partitions.clone();
        let order = |worker: &Worker| -> Vec<u32> {
            worker
                .fetch_targets(partitions.clone())
                .into_iter()
                .map(|(partition, ..)| partition)
                .collect()
        };
        worker.schedule();
        worker.complete(reply_of(
            &worker,
            0,
            &[served(0, &[3, 4], 5), served(1, &[], 9)],
        ));
        assert_eq!(worker.position(0), at(0, 5));
        assert_eq!(
            worker.position(1),
            at(1, 9),
            "a `Next` target continues from its section, also without a read"
        );
        assert_eq!(
            worker.position(2),
            PollingStrategy::next(),
            "a target without a section keeps its position"
        );
        let first = order(&worker);
        assert!(
            first.last() == Some(&0) && first[..2] == [1, 2],
            "a served target goes to the end of the next fetch"
        );

        worker.complete(reply_of(&worker, 1, &[served(1, &[9], 10)]));
        assert_eq!(
            order(&worker)[partitions.len() - 2..],
            [0, 1],
            "the target served last goes last"
        );
    }

    #[tokio::test]
    async fn refused_sections_resync_only_their_partitions() {
        let (mut worker, mut receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        for (partition, offset) in [(1, 11), (2, 21), (3, 31), (4, 41)] {
            worker.positions.insert(partition, at(partition, offset));
        }
        worker.schedule();
        let epoch = worker.epoch;
        worker.complete(reply_of(
            &worker,
            0,
            &[
                served(0, &[0], 1),
                refused(1, &IggyError::HistoryUnavailable),
                refused(2, &IggyError::NotPrimary),
                refused(4, &IggyError::ConsumerGroupPartitionNotOwned(1, 4)),
            ],
        ));
        assert_eq!(
            next_batches(&mut receiver)[0].partition_id,
            0,
            "the healthy section reaches the application"
        );
        assert_eq!(
            worker.epoch, epoch,
            "a moved partition keeps the other sections of its reply"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::HistoryUnavailable),
            "a lost history reaches the application"
        );
        assert!(
            receiver.is_empty(),
            "a moved primary and a moved partition report nothing"
        );
        assert!(
            !worker.positions.contains_key(&1)
                && worker.stopped.contains(&1)
                && !worker.retry_at.contains_key(&1),
            "a lost history stops its partition until a seek"
        );
        assert!(
            !worker.positions.contains_key(&4),
            "a moved partition resolves its start again"
        );
        assert!(
            worker.refresh_now,
            "a moved primary groups the partitions again"
        );
        assert!(worker.retry_at.contains_key(&2) && worker.retry_at.contains_key(&4));
        assert_eq!(
            worker.position(3),
            at(3, 31),
            "a target without a section keeps its position"
        );
        assert!(
            worker.active.contains_key(&3) && !worker.active.contains_key(&1),
            "the next fetch leaves out only the refused partitions"
        );
    }

    #[tokio::test]
    async fn refused_fetch_probes_each_target_alone() {
        let (mut worker, mut receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        let partitions = worker.partitions.clone();
        worker.positions.insert(4, at(4, 21));
        worker.positions.insert(6, at(6, 31));
        worker.schedule();
        worker.complete(failure_of(&worker, 0, IggyError::HistoryUnavailable));
        assert_eq!(
            worker.probing,
            partitions.iter().copied().collect(),
            "a refusal before the first read names no target, so each one probes alone"
        );
        worker.flush_errors();
        assert!(receiver.is_empty(), "only a probe reports the lost history");
        assert_eq!(worker.positions.get(&4), Some(&at(4, 21)));

        worker.complete(polled(
            &worker,
            FetchKind::Check,
            at(4, 21),
            4,
            Err(IggyError::HistoryUnavailable),
        ));
        assert!(
            !worker.positions.contains_key(&4) && worker.stopped.contains(&4),
            "a lost history stops its partition until a seek"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(4), IggyError::HistoryUnavailable)
        );

        let epoch = worker.epoch;
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            at(6, 31),
            6,
            Err(IggyError::ConsumerGroupPartitionNotOwned(1, 6)),
        ));
        assert!(
            !worker.positions.contains_key(&6)
                && worker.retry_at.contains_key(&6)
                && worker.refresh_now
                && worker.epoch == epoch,
            "a moved partition resolves its start after the refresh"
        );
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            PollingStrategy::next(),
            5,
            Ok(batch(5, [], 0)),
        ));
        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "neither a moved partition nor a probe delivers"
        );

        worker.schedule();
        assert_eq!(
            worker.active[&5].kind,
            FetchKind::Main,
            "a partition that passed its probe joins the group fetch again"
        );
        assert!(
            worker
                .active
                .iter()
                .all(|(partition, active)| *partition == 5 || active.kind == FetchKind::Check),
            "a probing partition is left out of the group fetch"
        );
    }

    #[tokio::test]
    async fn fetched_batch_that_does_not_decrypt_keeps_its_position() {
        let (mut worker, mut receiver) =
            in_one_group(encrypted_worker(builder_for(group()), &encryptor()));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.positions.insert(0, PollingStrategy::offset(3));
        worker.schedule();
        worker.complete(reply_of(
            &worker,
            0,
            &[served(0, &[3, 4], 5), served(1, &[], 9)],
        ));
        assert_eq!(
            worker.position(0),
            at(0, 3),
            "a batch that does not decrypt keeps its position"
        );
        assert!(
            worker.retry_at.contains_key(&0) && !worker.active.contains_key(&0),
            "the next fetch leaves the partition out until its retry"
        );
        assert_eq!(worker.position(1), at(1, 9));
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError {
                partition_id: Some(0),
                error: IggyError::CannotDecryptData,
                skip: Some(at(0, 4)),
                offset: Some(3),
                record: Some(Bytes::from(vec![0; BATCH_MESSAGE_HEADER_SIZE])),
            },
            "the error names the message and carries its record as served"
        );
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn given_undecodable_healthy_data_beside_unknown_progress_should_keep_its_start_until_retry_succeeds()
     {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        let context = PartitionContext {
            incarnation: 3,
            owner_generation: 4,
            metadata_op: 5,
        };
        let delivered = Checkpoint::new(0, 6, context);
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.shared.demand.store(true, ORDERING);
        if worker.config.on_read() {
            for (partition, offset) in [(0, 7), (1, 0)] {
                worker
                    .positions
                    .insert(partition, next_at(partition, offset));
            }
        }
        worker.schedule();
        let fetch = fetch_of(&worker, 0);
        let (mut header, _) = served(0, &[7], 8);
        header.context = context;
        header.records_len = 1;
        let mut response = sections(&[
            (header, vec![0]),
            refused(1, &IggyError::TransientNotCommitted),
        ]);
        response[1].status = Err(IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(reply(
            &worker,
            fetch,
            targets_of(&worker, fetch),
            Ok(response),
        ));
        let start = worker.shared.progress.undecoded(0).unwrap();
        assert_eq!((start.offset(), start.context()), (7, context));
        assert_eq!(
            worker.shared.progress.undecoded(1),
            Some(Checkpoint::new(1, 0, PartitionContext::default()))
        );
        assert_eq!(
            worker.position(0),
            continuation_at(0, 7, context),
            "the committed records are read again from their start"
        );
        assert_eq!(
            worker.shared.progress.last_committed(0),
            None,
            "the server committed records that never arrived, so the close stores the delivered offset again"
        );
        assert_eq!(worker.shared.progress.blocked(1), None);
        assert_eq!(worker.abandoning, [(OPERATION, vec![1])]);
        worker.flush_errors();
        let undecoded = next_error(&mut receiver);
        assert_eq!(
            (undecoded.partition_id, undecoded.skip),
            (Some(0), Some(continuation_at(0, 8, context)))
        );
        assert!(
            receiver.is_empty(),
            "a lost section is no error of the reader"
        );

        worker.retry_at.clear();
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        let (mut retry, records) = served(0, &[7], 8);
        retry.context = context;
        worker.complete(reply_of(&worker, 0, &[(retry, records)]));
        assert_eq!(
            offsets(&next_batches(&mut receiver)[0]),
            [7],
            "the retry from the start delivers the records"
        );
        assert_eq!(
            worker.position(0),
            PollingStrategy::continuation(0, PollingKind::Next, 8, context)
        );
        assert_eq!(
            worker.shared.progress.undecoded(0),
            None,
            "a successful retry removes the saved start"
        );

        worker.shared.progress.remember_undecoded(start);
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        let (mut recreated, records) = served(0, &[0], 1);
        recreated.context = PartitionContext {
            incarnation: context.incarnation + 1,
            ..context
        };
        worker.complete(reply_of(&worker, 0, &[(recreated, records)]));
        assert_eq!(
            worker.shared.progress.undecoded(0),
            None,
            "a new incarnation must not reuse the old rewind context"
        );
    }

    #[tokio::test]
    async fn section_that_does_not_decode_is_read_again_from_its_start() {
        for stale in [served(0, &[5], 6), refused(0, &IggyError::InvalidSizeBytes)] {
            let (mut worker, mut receiver) = fetching_worker();
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            worker.positions.insert(0, PollingStrategy::offset(3));
            worker.schedule();
            let (mut header, records) = served(0, &[3], 5);
            header.messages_count = 2;
            worker.complete(reply_of(&worker, 0, &[(header, records)]));
            let start = at(0, 3);
            assert_eq!(
                worker.position(0),
                start,
                "the records are read again from their start"
            );
            assert!(worker.read_alone.contains(&0));
            worker.flush_errors();
            assert!(
                receiver.is_empty(),
                "a failure of two messages names neither of them"
            );

            let fetch = fetch_of(&worker, 0);
            worker.complete(reply(
                &worker,
                fetch,
                vec![(0, at(0, 5), worker.generation(0))],
                Ok(sections(&[stale])),
            ));
            assert_eq!(
                worker.position(0),
                start,
                "a fetch sent before the failure reads stale"
            );
            worker.flush_errors();
            assert!(receiver.is_empty());
            assert_eq!(worker.active[&0].kind, FetchKind::Alone);

            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                start,
                0,
                Err(IggyError::InvalidMessagesCount),
            ));
            assert_eq!(worker.position(0), start);
            assert!(
                worker.read_alone.contains(&0) && worker.retry_at.contains_key(&0),
                "the retry reads the message alone again"
            );
            worker.flush_errors();
            assert_eq!(
                next_error(&mut receiver),
                ReadError {
                    partition_id: Some(0),
                    error: IggyError::InvalidMessagesCount,
                    skip: Some(at(0, 4)),
                    offset: None,
                    record: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn given_a_section_that_does_not_decode_when_its_messages_read_alone_should_read_alone_to_its_end()
     {
        const START: u64 = 3;
        const END: u64 = START + 4;
        let (mut worker, _receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.positions.insert(0, at(0, START));
        worker.schedule();
        let (mut header, records) = served(0, &[START], END);
        header.messages_count = 4;
        worker.complete(reply_of(&worker, 0, &[(header, records)]));
        assert!(worker.read_alone.contains(&0));

        for offset in START..END {
            assert!(
                worker.read_alone.contains(&0),
                "offset {offset}: a message of the section is read alone"
            );
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                at(0, offset),
                0,
                Ok(batch(0, [offset], offset + 1)),
            ));
        }

        assert_eq!(worker.position(0), at(0, END));
        assert!(
            !worker.read_alone.contains(&0),
            "past the section the group fetch reads the partition again"
        );
    }

    #[tokio::test]
    async fn given_a_last_start_when_the_epoch_changes_before_a_delivery_should_read_again_from_the_resolved_start()
     {
        let (mut worker, _receiver) = worker_of(standalone([0]).start(PollingStrategy::last()));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            PollingStrategy::last(),
            0,
            Ok(batch(0, [7, 8], 9)),
        ));
        assert_eq!(worker.position(0), at(0, 9));

        worker.invalidate();

        assert_eq!(
            worker.position(0),
            at(0, 7),
            "a new `Last` read would skip the messages that arrived since the first one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_auto_commit_when_a_rebalance_takes_a_partition_away_should_store_its_counted_position()
     {
        const COUNTED: u64 = 7;
        let (client, cluster) = connect_to_one_replica().await;
        let (mut worker, _receiver) = worker_of(builder_on(client, group()).options(PollOptions {
            commit: CommitPolicy::Auto(AutoCommit::default()),
            ..PollOptions::default()
        }));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0, 1])));
        let progress = Arc::clone(&worker.shared.progress);
        let stamp = progress.stamp(worker.generation(1));
        progress.count(
            Checkpoint::stamped(1, COUNTED, PartitionContext::default(), Some(stamp)),
            1,
            false,
        );

        assert!(worker.apply(assigned(&worker, SESSION, VERSION + 1, [0])));
        progress.flush_counted().await.unwrap();

        assert_eq!(
            stored_offsets(&cluster),
            [COUNTED],
            "the count of the partition that left is stored, not dropped with its generation"
        );
    }

    #[tokio::test]
    async fn given_a_committing_read_alone_when_its_message_does_not_decode_should_remember_it_for_the_close()
     {
        const BAD: u64 = 4;
        let (mut worker, _receiver) = worker_of(committing(standalone([0])));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, at(0, BAD));
        worker.read_alone.insert(0);

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, BAD),
            0,
            Err(IggyError::InvalidMessagePayloadLength),
        ));

        assert_eq!(
            worker.shared.progress.undecoded(0),
            Some(Checkpoint::new(0, BAD, PartitionContext::default())),
            "the server stored the offset of the message, so the close moves it back"
        );
    }

    #[tokio::test]
    async fn given_a_committed_section_that_does_not_decode_when_read_alone_should_deliver_the_good_message_and_skip_only_the_bad_one()
     {
        const GOOD: u64 = 3;
        const BAD: u64 = GOOD + 1;
        let (mut worker, mut receiver) = in_one_group(worker_of(committing(builder_for(group()))));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.positions.insert(0, at(0, GOOD));
        worker.shared.demand.store(true, ORDERING);
        if worker.config.on_read() {
            for partition in worker.partitions.clone() {
                worker
                    .positions
                    .entry(partition)
                    .or_insert(next_at(partition, 0));
            }
        }
        worker.schedule();
        let (mut header, records) = served(0, &[GOOD], BAD + 1);
        header.messages_count = 2;
        worker.complete(reply_of(&worker, 0, &[(header, records)]));
        assert_eq!(worker.position(0), at(0, GOOD));
        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "a failure of two messages names neither of them"
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(worker.active[&0].kind, FetchKind::Alone);
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, GOOD),
            0,
            Ok(batch(0, [GOOD], BAD)),
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [GOOD]);
        assert_eq!(worker.position(0), at(0, BAD));
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(
            worker.active[&0].kind,
            FetchKind::Alone,
            "the next message of the section reads alone, not in a group fetch"
        );

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, BAD),
            0,
            Err(IggyError::InvalidMessagePayloadLength),
        ));
        assert_eq!(worker.position(0), at(0, BAD));
        assert!(
            worker.read_alone.contains(&0) && worker.retry_at.contains_key(&0),
            "the retry reads the message alone again"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError {
                partition_id: Some(0),
                error: IggyError::InvalidMessagePayloadLength,
                skip: Some(at(0, BAD + 1)),
                offset: None,
                record: None,
            }
        );
    }

    #[tokio::test]
    async fn given_partial_unknown_progress_when_fetch_completes_should_deliver_healthy_data_and_read_the_lost_target_again()
     {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        worker.shared.demand.store(true, ORDERING);
        if worker.config.on_read() {
            for (partition, offset) in [(0, 7), (1, 0)] {
                worker
                    .positions
                    .insert(partition, next_at(partition, offset));
            }
        }
        worker.schedule();
        let fetch = fetch_of(&worker, 0);
        let mut response = sections(&[
            served(0, &[7], 8),
            refused(1, &IggyError::TransientNotCommitted),
        ]);
        response[1].status = Err(IggyError::FetchProgressUncertain(OPERATION));
        worker.complete(reply(
            &worker,
            fetch,
            targets_of(&worker, fetch),
            Ok(response),
        ));
        worker.flush_errors();
        let batches = next_batches(&mut receiver);
        assert_eq!(
            (batches.len(), batches[0].partition_id, offsets(&batches[0])),
            (1, 0, vec![7]),
            "the committed data reaches the application"
        );
        assert!(
            receiver.is_empty(),
            "a lost section is no error of the reader"
        );
        assert_eq!(worker.shared.progress.blocked(1), None);
        assert_eq!(worker.abandoning, [(OPERATION, vec![1])]);

        worker.retry_at.clear();
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            worker.active.contains_key(&1),
            "the lost target reads again, and its read finds what the lost one committed"
        );
    }

    #[tokio::test]
    async fn blocked_lane_of_a_target_holds_only_its_partition() {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        let delivered = Checkpoint::new(1, 7, PartitionContext::default());
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.shared.demand.store(true, ORDERING);
        if worker.config.on_read() {
            for (partition, offset) in [(0, 0), (1, 8)] {
                worker
                    .positions
                    .insert(partition, next_at(partition, offset));
            }
        }
        worker.schedule();
        worker.complete(failure_of(
            &worker,
            0,
            IggyError::PartitionLaneBlocked(1, OPERATION),
        ));
        assert_eq!(
            worker.shared.progress.last_committed(1),
            Some(delivered),
            "a fetch refused before its read commits nothing, so no close store moves the offset back"
        );
        assert_eq!(
            (
                worker.shared.progress.blocked(1),
                worker.shared.progress.blocked(0)
            ),
            (Some(OPERATION), None)
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::PartitionLaneBlocked(1, OPERATION))
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            worker.active.contains_key(&0) && !worker.active.contains_key(&1),
            "the other target is fetched again"
        );
    }

    #[tokio::test]
    async fn committing_member_resolves_a_timestamp_start_with_a_check_that_commits_nothing() {
        const TIMESTAMP_US: u64 = 1_000;
        let (mut worker, mut receiver) = worker_of(committing(
            builder_for(group()).start(PollingStrategy::first()),
        ));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        let timestamp = PollingStrategy::timestamp(IggyTimestamp::from(TIMESTAMP_US));
        worker.positions.insert(1, timestamp);
        worker.schedule();
        assert_eq!(
            worker.active[&1].kind,
            FetchKind::Check,
            "a check runs without demand, because it commits nothing"
        );
        assert!(
            !worker.active.contains_key(&0),
            "a `First` start needs no check"
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        let fetch = fetch_of(&worker, 0);
        assert_eq!(
            targets_of(&worker, fetch),
            [(0, PollingStrategy::offset(0), worker.generation(0))],
            "`First` goes into the fetch as offset 0, which leaves out the partition of the check"
        );
        worker.complete(polled(
            &worker,
            FetchKind::Check,
            timestamp,
            1,
            Ok(batch(1, [5, 6], 9)),
        ));
        assert_eq!(
            worker.position(1),
            at(1, 5),
            "the group fetch reads the checked messages again"
        );
        assert!(receiver.is_empty(), "a check delivers nothing");

        worker.complete(reply_of(&worker, 0, &[served(0, &[0], 1)]));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [0]);
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(
            fetch_of(&worker, 0),
            fetch_of(&worker, 1),
            "the resolved start joins the group fetch"
        );
    }

    #[tokio::test]
    async fn group_member_should_drop_only_the_continuation_of_the_refused_partition() {
        for error in [
            IggyError::HistoryUnavailable,
            IggyError::ConsumerGroupPartitionNotOwned(1, 3),
        ] {
            let stops = error == IggyError::HistoryUnavailable;
            let (mut worker, mut receiver) = worker_of(builder_for(group()));
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [3, 4])));
            worker.positions.insert(3, at(3, 11));
            worker.positions.insert(4, at(4, 21));
            let epoch = worker.epoch;
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                at(3, 11),
                3,
                Err(error.clone()),
            ));
            assert!(!worker.positions.contains_key(&3));
            assert_eq!(worker.positions.get(&4), Some(&at(4, 21)));
            assert_eq!(
                worker.epoch, epoch,
                "a refused partition keeps the reads of the others"
            );
            assert_eq!(
                worker.stopped.contains(&3),
                stops,
                "a lost history stops its partition until a seek"
            );
            assert_eq!(
                worker.retry_at.contains_key(&3),
                !stops,
                "a moved partition retries after the refresh"
            );
            worker.flush_errors();
            if stops {
                assert_eq!(next_error(&mut receiver), ReadError::new(Some(3), error));
                worker.shared.progress.seek(3, PollingStrategy::offset(5));
                worker.apply_seeks();
                assert!(
                    worker.is_idle(3) && worker.position(3) == PollingStrategy::offset(5),
                    "a seek resumes the partition from its start"
                );
            } else {
                assert!(receiver.is_empty(), "a moved partition reports nothing");
                assert!(worker.refresh_now);
            }
        }
    }

    #[tokio::test]
    async fn given_a_zero_wait_when_a_poll_returns_nothing_should_poll_again_after_the_retry_interval()
     {
        let on_read_without_wait = PollOptions {
            commit: CommitPolicy::OnRead,
            ..PollOptions::default()
        };
        for (builder, waits) in [
            (standalone([0]).options(PollOptions::default()), false),
            (standalone([0]).options(on_read_without_wait), false),
            (standalone([0]), true),
            (committing(standalone([0])), true),
        ] {
            let (mut worker, _receiver) = worker_of(builder);
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));

            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                PollingStrategy::next(),
                0,
                Ok(batch(0, Vec::new(), 0)),
            ));

            assert_eq!(
                worker.retry_at.contains_key(&0),
                !waits,
                "on_read: {}, max_wait: {}: a read that waited for nothing pauses an idle partition",
                worker.config.on_read(),
                worker.config.options.max_wait
            );
        }
    }

    #[tokio::test]
    async fn given_a_zero_wait_when_a_fetch_returns_nothing_should_fetch_again_after_the_retry_interval()
     {
        let (mut worker, _receiver) =
            worker_of(builder_for(group()).options(PollOptions::default()));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1, 2]])));
        worker.schedule();

        worker.complete(reply_of(
            &worker,
            0,
            &[served(1, &[], 0), served(2, &[7], 8)],
        ));

        assert!(
            worker.retry_at.contains_key(&0)
                && worker.retry_at.contains_key(&1)
                && !worker.retry_at.contains_key(&2),
            "only the partitions that returned nothing pause"
        );
        assert_eq!(
            worker.active.keys().copied().collect::<Vec<_>>(),
            [2],
            "the partition that returned messages reads on"
        );
    }

    #[tokio::test]
    async fn given_a_position_without_history_when_its_read_finds_no_history_should_retry_instead_of_stopping()
     {
        let (mut worker, mut receiver) = worker_of(standalone([3]));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [3])));

        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            PollingStrategy::next(),
            3,
            Err(IggyError::HistoryUnavailable),
        ));

        worker.flush_errors();
        assert!(
            receiver.try_recv().is_err(),
            "only a stop reports a lost history"
        );
        assert!(
            !worker.stopped.contains(&3) && worker.retry_at.contains_key(&3),
            "the next read takes the history of a new route"
        );
        assert_eq!(worker.position(3), PollingStrategy::next());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lasting_refusal_when_it_repeats_should_retry_later_each_time_and_then_stop_with_one_error()
     {
        let (mut worker, mut receiver) = worker_of(standalone([0]));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        let mut delays = Vec::new();

        for _ in 0..MAX_LASTING_REFUSALS {
            worker.flush_errors();
            assert!(
                receiver.try_recv().is_err(),
                "a refusal before the last one is only logged"
            );
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                PollingStrategy::next(),
                0,
                Err(IggyError::Unauthorized),
            ));
            if let Some(retry_at) = worker.retry_at.remove(&0) {
                delays.push(retry_at - Instant::now());
            }
        }

        let interval = worker.config.retry_interval;
        assert_eq!(
            delays,
            [1, 2, 4, 8, 16].map(|factor| interval * factor),
            "the wait doubles with each refusal in a row"
        );
        assert!(
            worker.stopped.contains(&0),
            "the partition stops until a seek"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::Unauthorized)
        );
    }

    /// A member that the server keeps refusing after each refresh loops without a sign, so the
    /// refusal counts as lasting, while each one still drops the position and refreshes.
    #[tokio::test(start_paused = true)]
    async fn given_a_partition_not_owned_when_it_repeats_after_each_refresh_should_report_it_once_after_the_limit()
     {
        let not_owned = IggyError::ConsumerGroupPartitionNotOwned(1, 0);
        let (mut worker, mut receiver) = worker_of(builder_for(group()));
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));

        for refusal in 1..=MAX_LASTING_REFUSALS {
            worker.flush_errors();
            assert!(
                receiver.try_recv().is_err(),
                "refusal {refusal}: a refusal before the last one is only logged"
            );
            worker.positions.insert(0, at(0, 5));
            worker.refuse(&[0], not_owned.clone());
            assert!(
                !worker.positions.contains_key(&0) && std::mem::take(&mut worker.refresh_now),
                "refusal {refusal}: the position goes and the assignment refreshes"
            );
            worker.retry_at.remove(&0);
        }

        assert!(worker.stopped.contains(&0));
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), not_owned)
        );
        assert!(receiver.try_recv().is_err(), "the refusal is reported once");
    }

    #[tokio::test]
    async fn given_a_partition_not_owned_when_a_read_then_succeeds_should_count_again_from_zero() {
        let not_owned = IggyError::ConsumerGroupPartitionNotOwned(1, 0);
        let (mut worker, _receiver) = worker_of(builder_for(group()));
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
        for _ in 1..MAX_LASTING_REFUSALS {
            worker.refuse(&[0], not_owned.clone());
        }
        assert_eq!(
            worker.lasting_refusals.get(&0),
            Some(&(MAX_LASTING_REFUSALS - 1))
        );
        worker.retry_at.clear();
        worker.schedule();

        worker.complete(reply_of(&worker, 0, &[served(0, &[5], 6)]));
        worker.refuse(&[0], not_owned);

        assert_eq!(
            worker.lasting_refusals.get(&0),
            Some(&1),
            "a good read starts the count again"
        );
        assert!(!worker.stopped.contains(&0) && worker.pending_errors.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_transient_refusal_when_it_repeats_should_retry_after_the_interval_and_never_stop()
     {
        let (mut worker, mut receiver) = worker_of(standalone([0]));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));

        for _ in 0..MAX_LASTING_REFUSALS * 2 {
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                PollingStrategy::next(),
                0,
                Err(IggyError::TransientNotAccepted),
            ));
            assert_eq!(
                worker
                    .retry_at
                    .remove(&0)
                    .map(|retry_at| retry_at - Instant::now()),
                Some(worker.config.retry_interval)
            );
            worker.flush_errors();
            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(Some(0), IggyError::TransientNotAccepted)
            );
        }

        assert!(!worker.stopped.contains(&0));
    }

    #[tokio::test]
    async fn committing_worker_should_drop_only_the_continuation_of_a_partition_without_history() {
        let (mut worker, mut receiver) = worker_of(committing(
            standalone([3, 4]).start(PollingStrategy::first()),
        ));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [3, 4])));
        worker.positions.insert(3, at(3, 11));
        worker.positions.insert(4, at(4, 21));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(3, 11),
            3,
            Err(IggyError::HistoryUnavailable),
        ));
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(3), IggyError::HistoryUnavailable)
        );
        assert!(!worker.positions.contains_key(&3));
        assert!(
            worker.stopped.contains(&3) && !worker.retry_at.contains_key(&3),
            "a lost history stops its partition instead of a retry"
        );
        assert_eq!(worker.positions.get(&4), Some(&at(4, 21)));

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            !worker.active.contains_key(&3) && worker.active.contains_key(&4),
            "a stopped partition reads nothing until a seek"
        );
        worker.shared.progress.seek(3, PollingStrategy::first());
        worker.apply_seeks();
        assert_eq!(
            worker.position(3),
            PollingStrategy::offset(0),
            "a seek to `First` reads from offset 0"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            worker.active.contains_key(&3),
            "a seek resumes the stopped partition"
        );
    }

    #[tokio::test]
    async fn given_a_standalone_reader_with_readiness_groups_when_a_primary_moves_should_regroup_its_fetches()
     {
        let (mut worker, receiver) = worker_of(standalone([0, 1, 2]));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1, 2]])));
        worker.schedule();
        let fetch = fetch_of(&worker, 0);
        assert!(
            worker.tasks.len() == 1
                && worker.active[&0].kind == FetchKind::Main
                && targets_of(&worker, fetch).len() == 3,
            "a standalone reader fetches its partitions by group"
        );

        worker.complete(failure_of(&worker, 0, IggyError::NotPrimary));
        assert!(
            [0, 1, 2]
                .iter()
                .all(|partition| worker.retry_at.contains_key(partition)),
            "the partitions retry"
        );
        assert!(worker.refresh_now, "the next turn refreshes the groups");
        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "a moved primary reaches no application"
        );

        assert!(worker.apply(grouped(
            &worker,
            SESSION,
            VERSION,
            vec![vec![0], vec![1, 2]]
        )));
        worker.retry_at.clear();
        worker.schedule();
        assert!(
            fetch_of(&worker, 0) != fetch_of(&worker, 1)
                && fetch_of(&worker, 1) == fetch_of(&worker, 2),
            "the next fetches follow the new groups"
        );
    }

    #[tokio::test]
    async fn given_a_next_start_when_a_poll_returns_nothing_should_stay_next() {
        for builder in [standalone([0]), committing(standalone([0]))] {
            let (mut worker, receiver) = worker_of(builder);
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                PollingStrategy::next(),
                0,
                Ok(batch(0, [], 9)),
            ));
            assert_eq!(
                worker.position(0),
                PollingStrategy::next(),
                "only a fetch section resolves `Next` without a message"
            );
            assert!(receiver.is_empty());
        }
    }

    #[tokio::test]
    async fn given_a_last_start_when_reads_return_nothing_should_keep_it_until_one_reads() {
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let (mut worker, mut receiver) = worker_of(builder.start(PollingStrategy::last()));
            let on_read = worker.config.on_read();
            let kind = if on_read {
                FetchKind::Check
            } else {
                FetchKind::Alone
            };
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
            worker.schedule();
            assert_eq!(
                worker.active[&0].kind, kind,
                "a `Last` start reads alone before the group fetch"
            );
            worker.complete(polled(
                &worker,
                kind,
                PollingStrategy::last(),
                0,
                Ok(batch(0, [], 9)),
            ));
            assert_eq!(
                worker.position(0),
                PollingStrategy::last(),
                "an empty read keeps the start"
            );
            worker.schedule();
            assert_eq!(worker.active[&0].kind, kind, "the start takes another read");
            worker.complete(polled(
                &worker,
                kind,
                PollingStrategy::last(),
                0,
                Ok(batch(0, [7, 8], 9)),
            ));
            if on_read {
                assert_eq!(
                    worker.position(0),
                    at(0, 7),
                    "a check moves the position to its first message, which the committing fetch reads again"
                );
                assert!(receiver.is_empty(), "a check delivers nothing");
            } else {
                assert_eq!(worker.position(0), at(0, 9));
                assert_eq!(
                    offsets(&next_batches(&mut receiver)[0]),
                    [7, 8],
                    "a poll alone delivers its batch"
                );
            }
            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            assert_eq!(
                worker.active[&0].kind,
                FetchKind::Main,
                "the resolved start joins the group fetch"
            );
        }
    }

    #[tokio::test]
    async fn given_a_seek_when_a_read_of_the_older_generation_completes_should_drop_it_without_advance()
     {
        const SEEK: u64 = 10;
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let (mut worker, mut receiver) = worker_of(builder);
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
            worker.shared.demand.store(true, ORDERING);
            if worker.config.on_read() {
                for (partition, offset) in [(0, 3), (1, 7)] {
                    worker
                        .positions
                        .insert(partition, next_at(partition, offset));
                }
            }
            worker.schedule();
            let fetch = reply_of(&worker, 0, &[served(0, &[3, 4], 5), served(1, &[7], 8)]);
            let poll = polled(
                &worker,
                FetchKind::Alone,
                worker.position(0),
                0,
                Ok(batch(0, [3, 4], 5)),
            );
            worker
                .shared
                .progress
                .seek(0, PollingStrategy::offset(SEEK));
            worker.apply_seeks();

            worker.complete(fetch);
            assert_eq!(
                worker.position(0),
                PollingStrategy::offset(SEEK),
                "a section of the older generation moves nothing"
            );
            let delivered: Vec<u32> = next_batches(&mut receiver)
                .iter()
                .map(|batch| batch.partition_id)
                .collect();
            assert_eq!(delivered, [1], "only the other partition delivers");
            worker.complete(poll);
            assert_eq!(
                worker.position(0),
                PollingStrategy::offset(SEEK),
                "a poll of the older generation moves nothing"
            );
            assert!(receiver.is_empty(), "nor does it deliver");
        }
    }

    /// The next `Next` read finds what a lost committing read stored, so the worker abandons
    /// the operation itself and reads again after the retry interval, without an error.
    #[tokio::test]
    async fn given_an_empty_or_uncertain_fetch_section_when_closing_should_rewind_only_uncertain_progress()
     {
        for uncertain in [false, true] {
            let (client, cluster) = connect_to_one_replica().await;
            let (mut worker, _receiver) = worker_of(committing(
                builder_on(client, Consumer::default()).partitions([0]),
            ));
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
            worker.positions.insert(0, next_at(0, 5));
            let client = worker.config.client.clone();
            let blocked = client.write().await;
            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            let fetch = fetch_of(&worker, 0);
            let mut response = sections(&[served(0, &[], 5)]);
            if uncertain {
                response[0].status = Err(IggyError::FetchProgressUncertain(OPERATION));
            }
            worker.complete(reply(
                &worker,
                fetch,
                targets_of(&worker, fetch),
                Ok(response),
            ));
            worker.tasks.abort_all();
            drop(blocked);
            worker.abandon_lost().await;
            worker
                .shared
                .progress
                .rewind(Vec::new(), None, true)
                .await
                .unwrap();
            assert_eq!(
                stored_offsets(&cluster),
                if uncertain { vec![4] } else { Vec::new() }
            );
        }
    }

    #[tokio::test]
    async fn given_http_when_a_committing_reply_is_lost_should_keep_its_partition_blocked() {
        let builder = ReaderBuilder::new(
            IggyRwLock::new(ClientWrapper::Http(HttpClient::default())),
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
            Consumer::default(),
            None,
        )
        .partitions([0]);
        let (mut worker, mut receiver) = worker_of(committing(builder));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.positions.insert(0, next_at(0, 5));
        worker.refuse(&[0], IggyError::FetchProgressUncertain(OPERATION));
        worker.abandon_lost().await;
        worker.flush_errors();
        assert_eq!(worker.shared.progress.blocked(0), Some(OPERATION));
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::FetchProgressUncertain(OPERATION))
        );
        assert!(worker.abandoning.is_empty());
    }

    #[tokio::test]
    async fn given_a_lost_committing_reply_when_completed_should_abandon_it_and_read_on() {
        let (mut worker, receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
        worker.shared.demand.store(true, ORDERING);
        worker.positions.insert(0, next_at(0, 0));
        worker.schedule();

        worker.complete(failure_of(
            &worker,
            0,
            IggyError::FetchProgressUncertain(OPERATION),
        ));

        worker.flush_errors();
        assert!(
            receiver.is_empty(),
            "a lost reply is no error of the reader"
        );
        assert_eq!(worker.shared.progress.blocked(0), None);
        assert!(!worker.stopped.contains(&0));
        assert!(worker.retry_at.contains_key(&0));
        assert_eq!(worker.abandoning, [(OPERATION, vec![0])]);
        drop(blocked_connection);
        worker.abandon_lost().await;
        assert!(worker.abandoning.is_empty());
        worker.retry_at.clear();
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(worker.active[&0].kind, FetchKind::Main);
    }

    #[tokio::test]
    async fn given_a_committing_poll_that_does_not_decode_when_it_read_from_next_should_stop_its_partition_until_a_seek()
     {
        const DELIVERED: u64 = 4;
        let undecodable = IggyError::InvalidMessagePayloadLength;
        for (start, stops) in [
            (PollingStrategy::next(), true),
            (at(0, DELIVERED + 1), false),
        ] {
            let (mut worker, mut receiver) = worker_of(committing(standalone([0])));
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
            let progress = Arc::clone(&worker.shared.progress);
            let delivered = Checkpoint::new(0, DELIVERED, PartitionContext::default());
            progress.deliver(&delivered);
            progress.record_commit(delivered);

            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                start,
                0,
                Err(undecodable.clone()),
            ));
            worker.flush_errors();

            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(Some(0), undecodable.clone()),
                "{start:?}"
            );
            assert_eq!(
                progress.last_committed(0),
                None,
                "{start:?}: the server stored the offset of the batch, so the close stores the delivered offset again"
            );
            assert_eq!(
                (
                    worker.stopped.contains(&0),
                    worker.retry_at.contains_key(&0)
                ),
                (stops, !stops),
                "{start:?}: an offset reads the batch again, but `Next` starts after it"
            );
            progress.seek(0, PollingStrategy::offset(DELIVERED + 1));
            worker.apply_seeks();
            assert!(
                !worker.stopped.contains(&0),
                "{start:?}: a seek resumes the partition"
            );
        }
    }

    #[tokio::test]
    async fn given_many_regroups_when_fetches_restart_should_never_hold_more_than_the_slots() {
        const SLOTS: u32 = 2;
        const REGROUPS: usize = 8;
        let options = PollOptions::default();
        let (mut worker, receiver) = worker_of(
            builder_for(group())
                .options(options)
                .prefetch_bytes(options.max_bytes * SLOTS),
        );
        let slots = worker.config.slots;
        assert_eq!(slots, SLOTS as usize);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.report(ReadError::new(None, IggyError::CannotReadMessage));
        worker.flush_errors();
        let layouts = [vec![vec![0, 1], vec![2, 3]], vec![vec![0, 2], vec![1, 3]]];
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, layouts[0].clone())));
        worker.schedule();
        assert_eq!(
            worker.tasks.len() + receiver.len(),
            slots,
            "a queued reply holds a slot, so one group waits"
        );

        for layout in layouts.iter().cycle().skip(1).take(REGROUPS) {
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, layout.clone())));
            worker.schedule();
            assert!(
                worker.tasks.is_empty(),
                "a cancelled fetch holds its slot until it ends"
            );
            timeout(RELEASE_TIMEOUT, async {
                while worker.capacity.available_permits() + receiver.len() < slots {
                    yield_now().await;
                }
            })
            .await
            .expect("a cancelled fetch returns its slot");
            worker.schedule();
            assert_eq!(
                worker.tasks.len() + receiver.len(),
                slots,
                "every slot holds a read in flight or a queued reply"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_more_groups_than_slots_when_each_fetch_ends_should_give_every_group_a_turn() {
        const GROUPS: u32 = 3;
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let max_bytes = PollOptions::default().max_bytes;
            let (mut worker, _receiver) = worker_of(builder.prefetch_bytes(max_bytes));
            assert_eq!(worker.config.slots, 1);
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            let groups = (0..GROUPS).map(|partition| vec![partition]).collect();
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, groups)));

            let mut fetched = Vec::new();
            for _ in 0..GROUPS * 2 {
                worker.shared.demand.store(true, ORDERING);
                if worker.config.on_read() {
                    for (partition, offset) in [(0, 0), (1, 0), (2, 0)] {
                        worker
                            .positions
                            .insert(partition, next_at(partition, offset));
                    }
                }
                worker.schedule();
                let partitions: Vec<u32> = worker.active.keys().copied().collect();
                fetched.extend(partitions);
                let Completed::Fetch(mut completed) =
                    worker.tasks.join_next().await.unwrap().unwrap()
                else {
                    panic!("a group reads with a fetch");
                };
                completed.result = Ok(Vec::new());
                worker.complete(Completed::Fetch(completed));
            }

            assert_eq!(
                fetched,
                [0, 1, 2, 0, 1, 2],
                "on_read: {}: each group fetches in turn",
                worker.config.on_read()
            );
        }
    }

    #[tokio::test]
    async fn given_one_fetch_slot_when_another_partition_needs_a_start_should_check_it() {
        let options = PollOptions {
            count: 1,
            commit: CommitPolicy::OnRead,
            ..PollOptions::default()
        };
        let (mut worker, _receiver) = worker_of(
            builder_for(group())
                .options(options)
                .prefetch_messages(options.count),
        );
        assert_eq!(worker.config.slots, 1);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        worker.positions.insert(0, next_at(0, 0));
        worker.shared.demand.store(true, ORDERING);

        worker.schedule();

        assert_eq!(worker.active[&0].kind, FetchKind::Main);
        assert_eq!(worker.active[&1].kind, FetchKind::Check);
    }

    #[tokio::test]
    async fn given_a_group_without_a_fetch_when_a_slot_is_free_should_start_it_before_a_second_fetch()
     {
        const SLOTS: u32 = 2;
        let options = PollOptions::default();
        let (mut worker, _receiver) = worker_of(
            builder_for(group())
                .options(options)
                .prefetch_bytes(options.max_bytes * SLOTS),
        );
        assert_eq!(worker.config.slots, SLOTS as usize);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(
            &worker,
            SESSION,
            VERSION,
            vec![vec![0, 1], vec![2]]
        )));
        worker.retry(&[1, 2]);
        worker.schedule();
        assert_eq!(worker.active[&0].kind, FetchKind::Main);

        worker.retry_at.clear();
        worker.schedule();

        assert!(
            worker
                .active
                .get(&2)
                .is_some_and(|active| active.kind == FetchKind::Main)
                && !worker.active.contains_key(&1),
            "the idle group takes the last slot before a second fetch of a busy group"
        );
    }

    #[tokio::test]
    async fn given_backlogged_partitions_of_one_group_when_slots_are_free_should_read_each_in_its_own_fetch()
     {
        const BACKLOGGED: [u32; 4] = [0, 1, 2, 3];
        let (mut worker, _receiver) = fetching_worker();
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        worker.schedule();
        assert_eq!(worker.tasks.len(), 1);

        let backlog: Vec<Section> = BACKLOGGED
            .iter()
            .map(|&partition| behind_head(partition, &[5], 6))
            .collect();
        let completed = reply_of(&worker, 0, &backlog);
        worker.tasks.shutdown().await;
        worker.complete(completed);

        let fetches: BTreeSet<u64> = BACKLOGGED
            .iter()
            .map(|partition| fetch_of(&worker, *partition))
            .collect();
        assert_eq!(
            fetches.len(),
            BACKLOGGED.len(),
            "each backlogged partition reads in a fetch of its own"
        );
        let quiet = fetch_of(&worker, 4);
        assert!(
            !fetches.contains(&quiet)
                && worker
                    .partitions
                    .iter()
                    .filter(|partition| !BACKLOGGED.contains(partition))
                    .all(|partition| fetch_of(&worker, *partition) == quiet),
            "the quiet partitions share one fetch"
        );
        assert!(
            worker
                .active
                .values()
                .all(|active| active.kind == FetchKind::Main)
        );

        worker.complete(reply_of(&worker, 0, &[behind_head(0, &[6], 7)]));
        assert!(
            fetch_of(&worker, 0) != quiet
                && worker.active[&0].kind == FetchKind::Main
                && worker.active[&4].fetch == quiet,
            "a backlogged partition that turns idle reads again beside the parked fetch"
        );
        assert!(
            timeout(Duration::from_millis(1), worker.tasks.join_next())
                .await
                .is_err(),
            "a backlogged partition cancels no fetch"
        );
    }

    #[tokio::test]
    async fn given_more_backlogged_partitions_than_free_slots_when_scheduled_should_deal_them_over_the_slots()
     {
        const SLOTS: u32 = 3;
        let options = PollOptions::default();
        let (mut worker, _receiver) = worker_of(
            builder_for(group())
                .options(options)
                .prefetch_bytes(options.max_bytes * SLOTS),
        );
        assert_eq!(worker.config.slots, SLOTS as usize);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(
            &worker,
            SESSION,
            VERSION,
            vec![vec![0, 1, 2, 3, 4]]
        )));
        worker.schedule();

        let backlog: Vec<Section> = (0..5)
            .map(|partition| behind_head(partition, &[5], 6))
            .collect();
        let completed = reply_of(&worker, 0, &backlog);
        worker.tasks.shutdown().await;
        worker.complete(completed);

        assert_eq!(
            worker.tasks.len(),
            usize::try_from(SLOTS).unwrap() - 1,
            "the queued reply holds one slot, and a fetch takes each other one"
        );
        assert_eq!(
            fetches_in_flight(&worker),
            [vec![0, 2, 4], vec![1, 3]],
            "the backlogged partitions are dealt in turn over the fetches"
        );
    }

    #[tokio::test]
    async fn given_backlogged_partitions_of_a_committing_group_when_the_application_asks_should_deal_them_over_the_free_slots()
     {
        const SLOTS: u32 = 3;
        const PARTITIONS: [u32; 5] = [0, 1, 2, 3, 4];
        let options = PollOptions {
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::from(DEFAULT_MAX_WAIT))
        };
        let (mut worker, mut receiver) = worker_of(
            builder_for(group())
                .options(options)
                .prefetch_bytes(options.max_bytes * SLOTS),
        );
        assert_eq!(worker.config.slots, SLOTS as usize);
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(
            &worker,
            SESSION,
            VERSION,
            vec![PARTITIONS.to_vec()]
        )));
        for partition in PARTITIONS {
            worker.positions.insert(partition, next_at(partition, 0));
        }
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(worker.tasks.len(), 1, "one fetch per group before a reply");

        let backlog: Vec<Section> = PARTITIONS
            .iter()
            .map(|&partition| behind_head(partition, &[0], 1))
            .collect();
        answer(&mut worker, 0, &backlog).await;
        worker.schedule();
        assert!(
            worker.tasks.is_empty(),
            "nothing starts before the application asks again"
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert_eq!(
            worker.tasks.len(),
            usize::try_from(SLOTS).unwrap() - 1,
            "the queued reply holds one slot, and a committing fetch takes each other one"
        );
        assert_eq!(
            fetches_in_flight(&worker),
            [vec![0, 2, 4], vec![1, 3]],
            "the backlogged partitions are dealt in turn over disjoint committing fetches"
        );
        assert!(
            worker
                .active
                .values()
                .all(|active| active.kind == FetchKind::Backlog)
        );

        next_batches(&mut receiver);
        answer(&mut worker, 1, &[served(1, &[1], 2), served(3, &[1], 2)]).await;
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        let quiet = fetch_of(&worker, 1);
        assert!(
            fetch_of(&worker, 3) == quiet && worker.active[&1].kind == FetchKind::Main,
            "partitions that caught up share one fetch that may park"
        );

        next_batches(&mut receiver);
        answer(
            &mut worker,
            0,
            &[
                behind_head(0, &[1], 2),
                behind_head(2, &[1], 2),
                behind_head(4, &[1], 2),
            ],
        )
        .await;
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            fetch_of(&worker, 1) == quiet
                && worker.active[&0].kind == FetchKind::Backlog
                && fetches_in_flight(&worker) == [vec![0, 2, 4], vec![1, 3]],
            "backlogged partitions read again beside the parked fetch"
        );
        assert!(
            timeout(Duration::from_millis(1), worker.tasks.join_next())
                .await
                .is_err(),
            "a backlog fetch cancels nothing"
        );
    }

    /// A poll alone reads one message within `max_bytes`: the server returns a larger first
    /// message alone anyway, and a reply of one message that does not decode names it.
    #[tokio::test]
    async fn given_a_partition_that_reads_alone_when_polled_should_ask_for_one_message_within_max_bytes()
     {
        let (client, cluster) = connect_to_one_replica().await;
        cluster.script(
            &endpoint(0),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Refuse(IggyError::InvalidSizeBytes)],
        );
        let (mut worker, mut receiver) =
            worker_of(builder_on(client, Consumer::default()).partitions([0]));
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
        worker.read_alone.insert(0);
        worker.schedule();
        let completed = worker.tasks.join_next().await.unwrap().unwrap();
        let request = DeferredPollMessagesRequest::decode_from(
            &cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0].body,
        )
        .unwrap();
        assert_eq!(
            (request.poll.count, request.min_count, request.max_bytes),
            (
                SINGLE_RECORD,
                SINGLE_RECORD,
                worker.config.options.max_bytes
            ),
            "one message within the byte limit of the reader"
        );
        worker.complete(completed);
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::InvalidSizeBytes)
        );
        assert!(!worker.read_alone.contains(&0));
    }

    /// The client never reads a refused poll again, whatever its start and commit policy.
    #[tokio::test]
    async fn given_a_poll_refused_for_its_byte_limit_when_read_should_send_it_once_and_report_it() {
        const COUNT: u32 = 2;
        for commit in [CommitPolicy::Manual, CommitPolicy::OnRead] {
            for start in [PollingStrategy::offset(0), PollingStrategy::last()] {
                let case = format!("{commit:?} {start}");
                let (client, cluster) = connect_to_one_replica().await;
                cluster.script(
                    &endpoint(0),
                    POLL_MESSAGES_DEFERRED_CODE,
                    [Answer::Refuse(IggyError::InvalidSizeBytes)],
                );
                let (mut worker, mut receiver) = worker_of(
                    builder_on(client, Consumer::default())
                        .partitions([0])
                        .start(start)
                        .options(PollOptions {
                            count: COUNT,
                            commit,
                            ..PollOptions::default()
                        }),
                );
                assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
                worker.shared.demand.store(true, ORDERING);
                worker.schedule();
                let completed = worker.tasks.join_next().await.unwrap().unwrap();
                let polls = cluster.received(POLL_MESSAGES_DEFERRED_CODE);
                assert_eq!(polls.len(), 1, "{case}");
                let request = DeferredPollMessagesRequest::decode_from(&polls[0].body).unwrap();
                assert_eq!(
                    request.poll.count,
                    if worker.needs_start(0) {
                        SINGLE_RECORD
                    } else {
                        COUNT
                    },
                    "{case}"
                );
                worker.complete(completed);
                worker.flush_errors();
                assert_eq!(
                    next_error(&mut receiver),
                    ReadError::new(Some(0), IggyError::InvalidSizeBytes),
                    "{case}"
                );
                assert_eq!(worker.position(0), start, "{case}");
                assert!(!worker.read_alone.contains(&0), "{case}");
            }
        }
    }

    /// The client sends no heartbeat inside a read, so the default wait stays well below the
    /// 30 s operation deadline.
    #[tokio::test]
    async fn given_no_options_when_building_a_reader_should_wait_up_to_five_seconds() {
        let (worker, _receiver) = worker_of(standalone([0]));

        assert_eq!(
            worker.config.options,
            PollOptions::wait(IggyDuration::from(Duration::from_secs(5)))
        );
    }

    fn group() -> Consumer {
        Consumer::group(Identifier::numeric(GROUP_ID).unwrap())
    }

    fn standalone(partitions: impl IntoIterator<Item = u32>) -> ReaderBuilder {
        builder_for(Consumer::default()).partitions(partitions)
    }

    /// The server stores the offset of each read, which waits as long as a reader does by
    /// default.
    fn committing(builder: ReaderBuilder) -> ReaderBuilder {
        builder.options(PollOptions {
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::from(DEFAULT_MAX_WAIT))
        })
    }

    fn worker_of(builder: ReaderBuilder) -> (Worker, Replies) {
        let (config, shared) = builder.into_parts().unwrap();
        worker_on(config, shared)
    }

    fn worker_on(config: Config, shared: Shared) -> (Worker, Replies) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let worker = Worker::new(Arc::new(config), Arc::new(shared), sender, None);
        (worker, receiver)
    }

    fn encryptor() -> Arc<EncryptorKind> {
        Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&KEY).unwrap(),
        ))
    }

    fn encrypted_worker(
        builder: ReaderBuilder,
        encryptor: &Arc<EncryptorKind>,
    ) -> (Worker, Replies) {
        let (mut config, shared) = builder.into_parts().unwrap();
        config.encryptor = Some(Arc::clone(encryptor));
        worker_on(config, shared)
    }

    /// A group member that commits nothing, with one partition more than its fetch slots, all
    /// in one group.
    fn fetching_worker() -> (Worker, Replies) {
        in_one_group(worker_of(builder_for(group())))
    }

    /// Assigns one partition more than the fetch slots of the worker, all in one group.
    fn in_one_group((mut worker, receiver): (Worker, Replies)) -> (Worker, Replies) {
        let partitions: Vec<u32> = (0..=u32::try_from(worker.config.slots).unwrap()).collect();
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![partitions])));
        (worker, receiver)
    }

    /// A refresh in the current fetch state that read `partitions`, on a transport without
    /// readiness groups.
    fn assigned(
        worker: &Worker,
        session: u64,
        version: u64,
        partitions: impl Into<Vec<u32>>,
    ) -> Refreshed {
        Refreshed {
            epoch: worker.epoch,
            assignment: AssignmentVersion {
                session,
                generation: version,
            },
            partitions: partitions.into(),
            groups: None,
        }
    }

    /// A refresh in the current fetch state that read the partitions of `groups`.
    fn grouped(worker: &Worker, session: u64, version: u64, groups: Vec<Vec<u32>>) -> Refreshed {
        let mut partitions: Vec<u32> = groups.iter().flatten().copied().collect();
        partitions.sort_unstable();
        Refreshed {
            groups: Some(groups),
            ..assigned(worker, session, version, partitions)
        }
    }

    /// A read of `partition` from `strategy` in the current fetch state and generation, which
    /// commits as the read that the worker starts.
    fn polled(
        worker: &Worker,
        kind: FetchKind,
        strategy: PollingStrategy,
        partition: u32,
        result: Result<PolledMessages, IggyError>,
    ) -> Completed {
        let commits = worker.config.on_read()
            && kind == FetchKind::Alone
            && worker.catching_up(partition).is_none();
        Completed::Poll(CompletedPoll {
            epoch: worker.epoch,
            kind,
            partition,
            strategy,
            generation: worker.generation(partition),
            result: result.map_err(DecodeError::without_record),
            capacity: worker.reserve_recovery(partition).unwrap(),
            commit: commits
                .then(|| PendingCommit::new(Arc::clone(&worker.shared.progress), vec![partition])),
        })
    }

    fn fetch_of(worker: &Worker, partition: u32) -> u64 {
        worker.active[&partition].fetch
    }

    /// The partitions of each fetch in flight, sorted.
    fn fetches_in_flight(worker: &Worker) -> Vec<Vec<u32>> {
        let mut targets: Vec<Vec<u32>> = Vec::new();
        for (partition, active) in &worker.active {
            match targets
                .iter_mut()
                .find(|targets| fetch_of(worker, targets[0]) == active.fetch)
            {
                Some(targets) => targets.push(*partition),
                None => targets.push(vec![*partition]),
            }
        }
        targets.sort();
        targets
    }

    /// A section like [`served`] whose partition holds more messages after it.
    fn behind_head(partition: u32, offsets: &[u64], next_offset: u64) -> Section {
        let (mut header, records) = served(partition, offsets, next_offset);
        header.current_offset = next_offset + BACKLOG;
        (header, records)
    }

    /// The reply with `served` to the fetch in flight that reads `partition`. The fetch read
    /// each target from its current position.
    fn reply_of(worker: &Worker, partition: u32, served: &[Section]) -> Completed {
        let fetch = fetch_of(worker, partition);
        reply(
            worker,
            fetch,
            targets_of(worker, fetch),
            Ok(sections(served)),
        )
    }

    /// Ends the fetch in flight that reads `partition` with the reply `served`: its task gives
    /// its slot back, and the reply takes one, as a real fetch does.
    async fn answer(worker: &mut Worker, partition: u32, served: &[Section]) {
        let completed = reply_of(worker, partition, served);
        worker.active[&partition].task.abort();
        worker.tasks.join_next().await;
        worker.complete(completed);
    }

    fn failure_of(worker: &Worker, partition: u32, error: IggyError) -> Completed {
        let fetch = fetch_of(worker, partition);
        reply(worker, fetch, targets_of(worker, fetch), Err(error))
    }

    fn targets_of(worker: &Worker, fetch: u64) -> Vec<(u32, PollingStrategy, u64)> {
        worker
            .active
            .iter()
            .filter(|(_, active)| active.fetch == fetch)
            .map(|(partition, _)| {
                (
                    *partition,
                    worker.position(*partition),
                    worker.generation(*partition),
                )
            })
            .collect()
    }

    /// The reply to `fetch`, which read `targets`, in the current fetch state.
    fn reply(
        worker: &Worker,
        fetch: u64,
        targets: Vec<(u32, PollingStrategy, u64)>,
        result: Result<Vec<FetchedSection>, IggyError>,
    ) -> Completed {
        let partitions = targets.iter().map(|(partition, ..)| *partition).collect();
        Completed::Fetch(CompletedFetch {
            epoch: worker.epoch,
            fetch,
            commit: worker
                .config
                .on_read()
                .then(|| PendingCommit::new(Arc::clone(&worker.shared.progress), partitions)),
            targets,
            result,
            capacity: worker.reserve().unwrap(),
        })
    }

    /// Fails the reads in flight, which time out on the blocked connection.
    async fn fail_reads(worker: &mut Worker) {
        while let Some(completed) = worker.tasks.join_next().await {
            let Completed::Poll(mut completed) = completed.unwrap() else {
                panic!("a reader without readiness groups polls one partition");
            };
            assert!(
                matches!(
                    completed.result,
                    Err(DecodeError {
                        error: IggyError::TransientNotAccepted,
                        ..
                    })
                ),
                "a read that waits past its deadline for the client was never sent"
            );
            completed.result = Err(DecodeError::without_record(IggyError::CannotReadMessage));
            worker.complete(Completed::Poll(completed));
        }
    }

    fn next_batches(receiver: &mut Replies) -> Vec<PolledMessages> {
        match receiver.try_recv().unwrap().result {
            Ok(batches) => batches,
            Err(refusal) => panic!("expected batches, got {}", refusal.error),
        }
    }

    fn next_error(receiver: &mut Replies) -> ReadError {
        match receiver.try_recv().unwrap().result {
            Ok(batches) => panic!("expected an error, got {} batches", batches.len()),
            Err(refusal) => refusal.error,
        }
    }

    fn offsets(batch: &PolledMessages) -> Vec<u64> {
        batch
            .messages
            .iter()
            .map(|message| message.header.offset)
            .collect()
    }

    /// The continuation of `partition` at `offset` in the default history.
    fn at(partition: u32, offset: u64) -> PollingStrategy {
        continuation_at(partition, offset, PartitionContext::default())
    }

    /// The `Next` continuation of `partition` that expects `offset`, in the default history: the
    /// position of an `OnRead` partition after a delivery.
    fn next_at(partition: u32, offset: u64) -> PollingStrategy {
        PollingStrategy::continuation(
            partition,
            PollingKind::Next,
            offset,
            PartitionContext::default(),
        )
    }

    /// A batch of partition 0 with a message at each of `offsets`, whose payloads `encryptor`
    /// sealed, except the one at `unsealed`.
    fn sealed_batch(
        encryptor: &EncryptorKind,
        offsets: &[u64],
        unsealed: Option<u64>,
    ) -> PolledMessages {
        let messages: Vec<_> = offsets
            .iter()
            .map(|&offset| {
                let mut message = message_at(offset);
                if Some(offset) != unsealed {
                    message.payload = Bytes::from(encryptor.encrypt(&message.payload).unwrap());
                }
                message
            })
            .collect();
        PolledMessages::new(
            PartitionContext::default(),
            0,
            offsets.last().copied().unwrap_or_default(),
            u32::try_from(messages.len()).unwrap(),
            messages,
        )
    }
}
