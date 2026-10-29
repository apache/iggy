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
    AssignmentVersion, Checkpoint, Client, DeferredPollOptions, DiagnosticEvent, EncryptorKind,
    FetchedSection, IggyDuration, IggyError, MIN_DEFERRED_POLL_MAX_BYTES, MessageClient,
    PollOptions, PolledMessages, PollingKind, PollingStrategy, ServerCommitPolicy,
};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, timeout_at};
use tracing::{error, warn};

use super::progress::{PendingCommit, same_incarnation_and_owner};
use super::{Config, FetchCapacity, ORDERING, ReadError, Refusal, Reply, Shared, first_as_offset};
use crate::driver::{
    DEFERRED_CONSUMER_CAPACITY, MAX_DEFERRED_REQUESTS_PER_CONSUMER, with_deadline,
};

const ASSIGNMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// The message limit of a probe, and the size of a section whose failure names its message.
const SINGLE_RECORD: u32 = 1;

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
    result: Result<PolledMessages, IggyError>,
    capacity: FetchCapacity,
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
    capacity: FetchCapacity,
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
    /// fetch parked.
    Second,
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
pub(super) struct Worker {
    config: Arc<Config>,
    shared: Arc<Shared>,
    sender: mpsc::Sender<Reply>,
    events: Option<Receiver<DiagnosticEvent>>,
    capacity: Arc<Semaphore>,
    request_capacity: Arc<Semaphore>,
    tasks: JoinSet<Completed>,
    /// At most one refresh, apart from the fetches, so a slow one never holds
    /// their replies back.
    refreshing: JoinSet<Result<Refreshed, IggyError>>,
    /// The poll or the fetch that reads each partition.
    active: BTreeMap<u32, Active>,
    /// Numbers the polls and the fetches.
    fetches: u64,
    /// Partitions whose next read takes one message without a byte limit: a record above
    /// `max_bytes`, or the first message of a section that did not decode.
    read_alone: BTreeSet<u32>,
    /// Partitions that probe alone after the server refused a fetch of theirs.
    probing: BTreeSet<u32>,
    /// Partitions that read nothing until a seek.
    stopped: BTreeSet<u32>,
    positions: BTreeMap<u32, PollingStrategy>,
    /// The generation of each position.
    generations: BTreeMap<u32, u64>,
    /// The position that a seek started the generation of a partition at.
    seek_starts: BTreeMap<u32, PollingStrategy>,
    retry_at: BTreeMap<u32, Instant>,
    partitions: Vec<u32>,
    assignment: Option<AssignmentVersion>,
    /// The latest error of each partition, and of the whole reader, that waits for a slot.
    pending_errors: BTreeMap<Option<u32>, Refusal>,
    epoch: u64,
    cursor: Option<u32>,
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
        sender: mpsc::Sender<Reply>,
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
            request_capacity: Arc::new(Semaphore::new(MAX_DEFERRED_REQUESTS_PER_CONSUMER)),
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
            probing: BTreeSet::new(),
            stopped: BTreeSet::new(),
            positions: BTreeMap::new(),
            generations,
            seek_starts: BTreeMap::new(),
            retry_at: BTreeMap::new(),
            partitions: Vec::new(),
            assignment: None,
            pending_errors: BTreeMap::new(),
            cursor: None,
            groups: Vec::new(),
            served: BTreeMap::new(),
            replies: 0,
            refresh_now: false,
        }
    }

    pub(super) async fn run(mut self) {
        if self.events.is_none() {
            self.events = Some(self.config.client.read().await.subscribe_events().await);
        }
        let mut refresh = tokio::time::interval(ASSIGNMENT_REFRESH_INTERVAL);
        refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if self.sender.is_closed() {
                return;
            }
            if self.shared.closing.load(ORDERING) {
                self.finish_fetches().await;
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
                            if self.apply(refreshed) {
                                refresh.reset();
                            } else {
                                refresh.reset_immediately();
                            }
                        }
                        Ok(Err(error)) => {
                            self.refuse_refresh(error);
                            refresh.reset_after(self.config.retry_interval);
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
    /// and every other fetch ends with the worker.
    async fn finish_fetches(&mut self) {
        if !self.config.on_read() {
            return;
        }
        for active in self.active.values() {
            if active.kind == FetchKind::Check {
                active.task.abort();
            }
        }
        while let Some(completed) = self.tasks.join_next().await {
            if let Ok(completed) = completed {
                self.complete(completed);
            }
        }
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
        self.probing.clear();
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
            self.probing.remove(&partition);
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
                membership.join().await?;
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
            self.invalidate();
            if self.config.is_group() {
                self.generations = self.shared.progress.assign(&refreshed.partitions, &staying);
            }
            let stays = |partition: &u32| staying.binary_search(partition).is_ok();
            self.positions.retain(|partition, _| stays(partition));
            self.seek_starts.retain(|partition, _| stays(partition));
            self.stopped.retain(stays);
            self.pending_errors
                .retain(|partition, _| partition.as_ref().is_none_or(stays));
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
            error => self.report(ReadError::new(None, error)),
        }
    }

    fn reserve(&self) -> Option<FetchCapacity> {
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .ok()
            .map(|permit| FetchCapacity {
                permit: Some(permit),
                wake: Arc::clone(&self.shared.wake),
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
        let _ = self.sender.try_send(reply);
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
        if self.config.on_read() {
            self.schedule_committing_groups();
        } else {
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
            let Some(capacity) = self.reserve() else {
                return;
            };
            self.spawn_poll(partition, FetchKind::Alone, capacity);
        }
    }

    /// Every fetch of this mode commits its batches. Each group runs one fetch,
    /// and only on demand, so at most one batch per group is committed without
    /// the application asking for it. A partition that reads alone takes the
    /// turn of its group.
    fn schedule_committing_groups(&mut self) {
        if !self.shared.demand.load(ORDERING) {
            return;
        }
        for group in 0..self.groups.len() {
            let partitions = &self.groups[group];
            if partitions.iter().any(|partition| {
                self.active
                    .get(partition)
                    .is_some_and(|active| active.kind != FetchKind::Check)
            }) {
                continue;
            }
            let alone = partitions
                .iter()
                .copied()
                .find(|partition| self.read_alone.contains(partition) && self.is_idle(*partition));
            let targets = self.targets(group);
            if alone.is_none() && targets.is_empty() {
                continue;
            }
            let Some(capacity) = self.reserve() else {
                return;
            };
            match alone {
                Some(partition) => self.spawn_poll(partition, FetchKind::Alone, capacity),
                None => self.spawn_fetch(targets, FetchKind::Main, capacity),
            }
        }
    }

    /// A fetch takes no partitions after it starts, so the main fetch of a group
    /// runs until it ends. The partitions that become idle meanwhile share a
    /// second fetch, and each new one restarts only that fetch. This keeps a busy
    /// partition among quiet ones from waiting up to `max_wait` for the next main
    /// fetch. The next main fetch takes over the partitions of the second one, so
    /// the second one stays small.
    fn schedule_groups(&mut self) {
        for group in 0..self.groups.len() {
            let mut targets = self.targets(group);
            if targets.is_empty() {
                continue;
            }
            let Some(capacity) = self.reserve() else {
                return;
            };
            let in_flight = |kind| {
                self.groups[group].iter().find_map(|partition| {
                    self.active
                        .get(partition)
                        .filter(|active| active.kind == kind)
                        .map(|active| (active.fetch, active.task.clone()))
                })
            };
            let main = in_flight(FetchKind::Main).is_some();
            if let Some((second, task)) = in_flight(FetchKind::Second) {
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
                targets.extend(restarted);
            }
            let kind = if main {
                FetchKind::Second
            } else {
                FetchKind::Main
            };
            self.spawn_fetch(targets, kind, capacity);
        }
    }

    /// Polls partitions alone, from the one after the last polled one. With
    /// readiness groups, it polls only the partitions that the group fetches
    /// cannot read, and keeps one fetch slot per group free of them, so polls that
    /// wait for a first message never hold every slot.
    fn schedule_alone(&mut self) {
        let by_group = !self.groups.is_empty();
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
            let kind = if !by_group {
                FetchKind::Alone
            } else if self.fetched_by_group(partition)
                || (on_read && self.read_alone.contains(&partition))
            {
                continue;
            } else if on_read || self.probing.contains(&partition) {
                FetchKind::Check
            } else {
                FetchKind::Alone
            };
            let Some(capacity) = self.reserve() else {
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
            && !self.read_alone.contains(&partition)
            && matches!(
                self.position(partition).kind,
                PollingKind::Offset | PollingKind::Next
            )
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

    fn spawn_fetch(&mut self, partitions: Vec<u32>, kind: FetchKind, capacity: FetchCapacity) {
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
            async move {
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
            },
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

    fn spawn_poll(&mut self, partition: u32, kind: FetchKind, capacity: FetchCapacity) {
        let config = Arc::clone(&self.config);
        let strategy = self.position(partition);
        let generation = self.generation(partition);
        let options = if self.probing.contains(&partition) {
            // Only a refusal answers a probe, so it does not wait.
            PollOptions {
                count: SINGLE_RECORD,
                min_count: SINGLE_RECORD,
                max_wait: IggyDuration::from(Duration::ZERO),
                ..config.options
            }
        } else if self.read_alone.contains(&partition) {
            config.options.alone()
        } else {
            config.options
        };
        // A check delivers nothing, so it does not commit.
        let on_read = config.on_read() && kind == FetchKind::Alone;
        let commit =
            on_read.then(|| PendingCommit::new(Arc::clone(&self.shared.progress), vec![partition]));
        let server_commit = if on_read {
            ServerCommitPolicy::OnRead
        } else {
            ServerCommitPolicy::Disabled
        };
        let epoch = self.epoch;
        self.fetches += 1;
        let fetch = self.fetches;
        let task = self.tasks.spawn(DEFERRED_CONSUMER_CAPACITY.scope(
            Arc::clone(&self.request_capacity),
            async move {
                let started = Instant::now();
                let deadline = started + options.request_timeout().get_duration();
                // As in `spawn_fetch`, only the lock wait has a limit here.
                let result = async {
                    let client = timeout_at(deadline, config.client.read())
                        .await
                        .map_err(|_| IggyError::TransientNotAccepted)?;
                    let remaining =
                        DeferredPollOptions::from(options).remaining(started.elapsed())?;
                    let polled = client
                        .poll_messages(
                            &config.stream,
                            &config.topic,
                            partition,
                            &config.consumer,
                            &strategy,
                            PollOptions {
                                max_wait: remaining.max_wait,
                                server_commit,
                                ..options
                            },
                        )
                        .await?;
                    if polled.count > options.count
                        || polled.messages.len() > options.count as usize
                    {
                        return Err(IggyError::InvalidMessagesCount);
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
            },
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
            Completed::Poll(completed) => self.complete_poll(completed),
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
        let sections = match result {
            Ok(sections) => sections,
            Err(error) => {
                if let Some(commit) = commit
                    && !may_hide_commit(&error)
                {
                    commit.settle(Vec::new());
                }
                if self.is_current(epoch) {
                    self.refuse_fetch(&held, error);
                }
                return;
            }
        };
        let mut served = Vec::with_capacity(sections.len());
        for section in sections {
            let partition = section.partition_id;
            let Ok(target) = targets.binary_search_by_key(&partition, |(target, ..)| *target)
            else {
                continue;
            };
            let (_, sent, generation) = targets[target];
            match &section.status {
                Err(error) => {
                    if self.is_current(epoch)
                        && is_held(&partition)
                        && self.keeps(partition, generation)
                    {
                        self.refuse_partition(partition, error.clone());
                    }
                }
                Ok(()) => served.push((sent, generation, section)),
            }
        }
        let committing = commit.is_some();
        let current = self.is_current(epoch);
        // The server committed the batches of a committing fetch, so they reach the
        // application even after the fetch state was cleared, unless a seek replaced the
        // generation of their partition.
        served.retain(|(sent, generation, section)| {
            let partition = section.partition_id;
            self.keeps(partition, *generation)
                && (committing
                    || (current && is_held(&partition) && *sent == self.position(partition)))
        });
        self.replies += 1;
        let mut batches = Vec::with_capacity(served.len());
        let mut lost = Vec::new();
        let mut undecoded = Vec::with_capacity(served.len());
        for (sent, generation, section) in served {
            let Some(encryptor) = self.config.encryptor.clone() else {
                let moved = self.advance(sent, &section);
                undecoded.push((moved, generation, section));
                continue;
            };
            let partition = section.partition_id;
            let mut polled = match section.decode() {
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
                Err((failed, error)) => {
                    let rewind = sent == self.position(partition);
                    self.refuse_undecoded(rewind, partition, failed, section.context, error);
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
            match section.decode() {
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
        error: IggyError,
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
                "Failed to decode {count} messages from offset: {start}, partition ID: {partition}. They are read again one at a time. {error}",
                count = section.messages_count
            );
            self.positions.insert(
                partition,
                continuation_at(partition, start, section.context),
            );
            self.read_alone.insert(partition);
            return;
        }
        self.refuse_undecoded(rewind, partition, start, section.context, error);
    }

    /// The message at `start` does not decode. Its partition reads it alone again until a seek
    /// to the skip of the error.
    fn refuse_message(&mut self, partition: u32, start: &PollingStrategy, error: IggyError) {
        self.read_alone.insert(partition);
        let context = start.context().unwrap_or_default();
        self.refuse_undecoded(false, partition, start.value, context, error);
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
        });
    }

    /// Moves the position of the partition of `section` after it, unless a new
    /// assignment or a seek replaced the position that the fetch read from. A
    /// `Next` target continues from an offset too, also without a read.
    fn advance(&mut self, sent: PollingStrategy, section: &FetchedSection) -> bool {
        let partition = section.partition_id;
        self.shared
            .progress
            .forget_undecoded_history(partition, section.context);
        if sent != self.position(partition) {
            return false;
        }
        self.positions.insert(
            partition,
            continuation_at(partition, section.next_offset, section.context),
        );
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
        self.probing.remove(&partition);
        if !self.is_current(completed.epoch) || !self.keeps(partition, completed.generation) {
            return;
        }
        match completed.result {
            Ok(polled) => {
                if !matches!(
                    completed.strategy.kind,
                    PollingKind::Offset | PollingKind::Next
                ) && completed.strategy == self.position(partition)
                    && let Some(first) = polled.messages.first()
                {
                    self.positions.insert(
                        partition,
                        continuation_at(partition, first.header.offset, polled.context()),
                    );
                }
            }
            // The client already read a record above `max_bytes` alone, or skipped that read for
            // a `Last` start, so the refusal is final.
            Err(error) => self.refuse(&[partition], error),
        }
    }

    fn complete_poll(&mut self, mut completed: CompletedPoll) {
        let partition = completed.partition;
        self.active.remove(&partition);
        if let Some(commit) = completed.commit.take()
            && !completed.result.as_ref().is_err_and(may_hide_commit)
        {
            commit.settle(Vec::new());
        }
        // A mark holds for one read, so a record above `max_bytes` then gets a final refusal.
        // Another failure to decode marks the partition again.
        let alone = self.read_alone.remove(&partition);
        if self.config.on_read() {
            self.complete_on_demand(completed, alone);
            return;
        }
        if !self.is_current(completed.epoch) || !self.keeps(partition, completed.generation) {
            return;
        }
        let mut polled = match completed.result {
            Ok(polled) => polled,
            Err(error) if names_message(alone, &completed.strategy, &error) => {
                self.refuse_message(partition, &completed.strategy, error);
                return;
            }
            Err(error) => {
                self.refuse(&[partition], error);
                return;
            }
        };
        let context = polled.context();
        let failure = self
            .config
            .encryptor
            .as_deref()
            .and_then(|encryptor| decrypt_messages(encryptor, &mut polled).err());
        if let Some(last) = polled.messages.last().map(|message| message.header.offset) {
            let Some(next) = last.checked_add(1) else {
                self.refuse(&[partition], IggyError::InvalidOffset(last));
                return;
            };
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
        if let Some((failed, error)) = failure {
            self.refuse_undecoded(true, partition, failed, context, error);
        }
    }

    /// The server committed a batch that holds messages, so the batch reaches
    /// the application even after the fetch state was cleared, unless a seek
    /// replaced the generation of its partition.
    fn complete_on_demand(&mut self, completed: CompletedPoll, alone: bool) {
        let partition = completed.partition;
        let mut polled = match completed.result {
            Ok(polled) => polled,
            Err(error) => {
                if !self.keeps(partition, completed.generation) {
                    return;
                }
                // The server stored the offset of a reply that the worker cannot hand over, so a
                // read from `Next` would start after it, and its start offset is unknown.
                if completed.strategy.kind == PollingKind::Next && undeliverable(&error) {
                    self.stopped.insert(partition);
                    self.report(ReadError::new(Some(partition), error));
                } else if self.is_current(completed.epoch) {
                    if names_message(alone, &completed.strategy, &error) {
                        self.refuse_message(partition, &completed.strategy, error);
                    } else {
                        self.refuse(&[partition], error);
                    }
                }
                return;
            }
        };
        if !self.keeps(partition, completed.generation) {
            return;
        }
        let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
            return;
        };
        let mut overflow = None;
        // A new assignment replaces the position that the poll started from.
        // `Next` continues from an explicit offset too.
        if completed.strategy == self.position(partition) {
            match last.checked_add(1) {
                Some(next) => {
                    self.positions.insert(
                        partition,
                        continuation_at(partition, next, polled.context()),
                    );
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

    /// A record above `max_bytes` would stall the fetches of its partition, so
    /// the next read takes it alone. Only a failure of that read is reported.
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
        match error {
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
                    self.positions.remove(&partition);
                    self.shared.progress.forget_undecoded(partition);
                }
                self.retry(partitions);
                self.refresh_now = true;
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
                    self.positions.remove(&partition);
                    self.shared.progress.forget_undecoded(partition);
                    self.retry_at.remove(&partition);
                    self.stopped.insert(partition);
                    self.report(ReadError::new(Some(partition), error.clone()));
                }
            }
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

    fn is_current(&self, epoch: u64) -> bool {
        epoch == self.epoch && self.epoch == self.shared.epoch.load(ORDERING)
    }
}

async fn next_event(events: &mut Option<Receiver<DiagnosticEvent>>) -> Option<DiagnosticEvent> {
    match events {
        Some(events) => events.next().await,
        None => std::future::pending().await,
    }
}

fn continuation_at(partition: u32, offset: u64, context: PartitionContext) -> PollingStrategy {
    PollingStrategy::continuation(partition, PollingKind::Offset, offset, context)
}

/// Keeps the messages before the first one that does not decrypt, and returns the offset of
/// that message with its error.
fn decrypt_messages(
    encryptor: &EncryptorKind,
    polled: &mut PolledMessages,
) -> Result<(), (u64, IggyError)> {
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
    Err((offset, error))
}

/// The records limit of a fetch whose whole reply, with its section table,
/// fits the reservation of one reply.
fn fetch_max_bytes(max_bytes: u32, targets: usize) -> u32 {
    let records = (max_bytes as usize)
        .saturating_sub(FetchMessagesResponse::head_size(targets))
        .max(MIN_DEFERRED_POLL_MAX_BYTES);
    u32::try_from(records).unwrap_or(max_bytes)
}

/// The offset of the first record of `section`, or one before it. Offsets are
/// dense, so a fetch from there reads the section again.
fn section_start(section: &FetchedSection) -> u64 {
    section
        .next_offset
        .saturating_sub(u64::from(section.messages_count))
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

    use bytes::Bytes;
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::codes::POLL_MESSAGES_DEFERRED_CODE;
    use iggy_binary_protocol::requests::messages::DeferredPollMessagesRequest;
    use iggy_common::{Aes256GcmEncryptor, Consumer, Identifier, IggyTimestamp};
    use tokio::task::yield_now;
    use tokio::time::{advance, timeout};

    use crate::clients::reader::fixtures::{
        Section, batch, builder_for, builder_on, connect_to_one_replica, message_at, refused,
        sections, served,
    };
    use crate::clients::reader_builder::ReaderBuilder;
    use crate::links::fake_cluster::{Answer, endpoint};

    type Replies = mpsc::Receiver<Reply>;

    const GROUP_ID: u32 = 1;
    const SESSION: u64 = 1;
    const VERSION: u64 = 1;
    const OPERATION: u64 = 42;
    const KEY: [u8; 32] = [1; 32];
    const RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

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
    async fn refused_polls_reach_the_application_and_retry_later() {
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
            worker.refuse_refresh(error.clone());
            worker.flush_errors();
            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(None, error),
                "a refused refresh reaches the application"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn committing_poll_waits_for_demand_and_survives_invalidation() {
        let (mut worker, mut receiver) = worker_of(committing(standalone([0, 1])));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0, 1])));
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
            PollingStrategy::next(),
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
        assert_eq!(worker.position(0), at(0, 5));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            at(0, 5),
            0,
            Ok(batch(0, [5], 9)),
        ));
        assert_eq!(offsets(&next_batches(&mut receiver)[0]), [5]);
        assert_eq!(worker.position(0), at(0, 6));
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

    /// A poll reads a record above `max_bytes` alone itself, so its refusal is final.
    #[tokio::test]
    async fn oversized_record_of_a_poll_is_reported_at_once() {
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

    #[tokio::test]
    async fn oversized_record_of_a_fetch_is_polled_alone() {
        for builder in [builder_for(group()), committing(builder_for(group()))] {
            let (mut worker, mut receiver) = worker_of(builder);
            let on_read = worker.config.on_read();
            let client = worker.config.client.clone();
            let _blocked_connection = client.write().await;
            assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
            worker.shared.demand.store(true, ORDERING);
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
                "only the poll without a byte limit reports the error"
            );

            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            assert_eq!(worker.active[&0].kind, FetchKind::Alone);
            if on_read {
                assert!(
                    !worker.active.contains_key(&1),
                    "the record takes the turn of its group"
                );
            } else {
                assert_eq!(worker.active[&1].kind, FetchKind::Main);
            }
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
        assert_eq!(
            worker.shared.progress.blocked(1),
            Some(OPERATION),
            "an unknown outcome blocks its partition"
        );
        assert!(
            worker.stopped.contains(&1),
            "with OnRead the partition stops until a seek"
        );
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::FetchProgressUncertain(OPERATION))
        );

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            !worker.active.contains_key(&1),
            "a stopped partition is not fetched"
        );
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
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::TransientNotAccepted),
            "the application sees a refused fetch"
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
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        worker.positions.insert(1, PollingStrategy::last());
        let delivered = Checkpoint::new(0, 4, PartitionContext::default());
        worker.shared.progress.deliver(&delivered);
        worker.shared.progress.record_commit(delivered);
        worker.shared.demand.store(true, ORDERING);
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
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::TransientNotAccepted)
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
            worker
                .positions
                .insert(partition, PollingStrategy::offset(offset));
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
            PollingStrategy::offset(31),
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
        worker.positions.insert(4, PollingStrategy::offset(21));
        worker.positions.insert(6, PollingStrategy::offset(31));
        worker.schedule();
        worker.complete(failure_of(&worker, 0, IggyError::HistoryUnavailable));
        assert_eq!(
            worker.probing,
            partitions.iter().copied().collect(),
            "a refusal before the first read names no target, so each one probes alone"
        );
        worker.flush_errors();
        assert!(receiver.is_empty(), "only a probe reports the lost history");
        assert_eq!(worker.positions.get(&4), Some(&PollingStrategy::offset(21)));

        worker.complete(polled(
            &worker,
            FetchKind::Check,
            PollingStrategy::offset(21),
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
            PollingStrategy::offset(31),
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
            }
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
        assert_eq!(worker.shared.progress.undecoded(1), None);
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
        assert_eq!(worker.shared.progress.blocked(1), Some(OPERATION));
        worker.flush_errors();
        let undecoded = next_error(&mut receiver);
        assert_eq!(
            (undecoded.partition_id, undecoded.skip),
            (Some(0), Some(continuation_at(0, 8, context)))
        );
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::FetchProgressUncertain(OPERATION))
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
        assert_eq!(worker.position(0), continuation_at(0, 8, context));
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
                }
            );
        }
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
        for start in [GOOD, BAD] {
            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            let (mut header, records) = served(0, &[start], start + 2);
            header.messages_count = 2;
            worker.complete(reply_of(&worker, 0, &[(header, records)]));
            assert_eq!(worker.position(0), at(0, start));
            worker.flush_errors();
            assert!(
                receiver.is_empty(),
                "a failure of two messages names neither of them"
            );

            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            assert_eq!(worker.active[&0].kind, FetchKind::Alone);
            if start == GOOD {
                worker.complete(polled(
                    &worker,
                    FetchKind::Alone,
                    at(0, GOOD),
                    0,
                    Ok(batch(0, [GOOD], BAD)),
                ));
                assert_eq!(offsets(&next_batches(&mut receiver)[0]), [GOOD]);
                assert_eq!(worker.position(0), at(0, BAD));
            }
        }

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
            }
        );
    }

    #[tokio::test]
    async fn given_partial_unknown_progress_when_fetch_completes_should_deliver_healthy_data_and_stop_only_blocked_targets()
     {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0, 1]])));
        worker.shared.demand.store(true, ORDERING);
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
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(1), IggyError::FetchProgressUncertain(OPERATION)),
            "so does the operation of the unknown outcome"
        );
        assert_eq!(worker.shared.progress.blocked(1), Some(OPERATION));

        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(worker.active.contains_key(&0));
        assert!(!worker.active.contains_key(&1));
        worker.retry_at.clear();
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            !worker.active.contains_key(&1),
            "the blocked target must not retry automatically"
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
            worker.positions.insert(3, PollingStrategy::offset(11));
            worker.positions.insert(4, PollingStrategy::offset(21));
            let epoch = worker.epoch;
            worker.complete(polled(
                &worker,
                FetchKind::Alone,
                PollingStrategy::offset(11),
                3,
                Err(error.clone()),
            ));
            assert!(!worker.positions.contains_key(&3));
            assert_eq!(worker.positions.get(&4), Some(&PollingStrategy::offset(21)));
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
    async fn committing_worker_should_drop_only_the_continuation_of_a_partition_without_history() {
        let (mut worker, mut receiver) = worker_of(committing(
            standalone([3, 4]).start(PollingStrategy::first()),
        ));
        let client = worker.config.client.clone();
        let _blocked_connection = client.write().await;
        assert!(worker.apply(assigned(&worker, SESSION, VERSION, [3, 4])));
        worker.positions.insert(3, PollingStrategy::offset(11));
        worker.positions.insert(4, PollingStrategy::offset(21));
        worker.complete(polled(
            &worker,
            FetchKind::Alone,
            PollingStrategy::offset(11),
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
        assert_eq!(worker.positions.get(&4), Some(&PollingStrategy::offset(21)));

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

    #[tokio::test]
    async fn given_an_unknown_committing_outcome_when_the_client_abandons_it_should_stay_stopped_until_a_seek()
     {
        let (mut worker, mut receiver) = worker_of(committing(builder_for(group())));
        let client = worker.config.client.clone();
        let blocked_connection = client.write().await;
        assert!(worker.apply(grouped(&worker, SESSION, VERSION, vec![vec![0]])));
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        worker.complete(failure_of(
            &worker,
            0,
            IggyError::FetchProgressUncertain(OPERATION),
        ));
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::FetchProgressUncertain(OPERATION))
        );
        assert_eq!(worker.shared.progress.blocked(0), Some(OPERATION));

        // The lift asks the client, which retains no operation.
        drop(blocked_connection);
        worker.shared.progress.unblock_abandoned().await;
        assert_eq!(
            worker.shared.progress.blocked(0),
            None,
            "the block ends when the client abandons the operation"
        );
        worker.shared.demand.store(true, ORDERING);
        worker.schedule();
        assert!(
            !worker.active.contains_key(&0),
            "with OnRead the partition stays stopped until a seek"
        );

        worker.shared.progress.seek(0, PollingStrategy::next());
        worker.apply_seeks();
        worker.schedule();
        assert_eq!(
            worker.active[&0].kind,
            FetchKind::Main,
            "a seek resumes the partition"
        );
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

    #[tokio::test]
    async fn given_a_record_above_max_bytes_when_polled_alone_should_ask_for_one_message_without_a_byte_limit()
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
        let alone = worker.config.options.alone();
        assert_eq!(
            (request.poll.count, request.min_count, request.max_bytes),
            (alone.count, alone.min_count, alone.max_bytes),
            "the record is read alone, without a byte limit"
        );
        assert_eq!(
            cluster.count(POLL_MESSAGES_DEFERRED_CODE),
            1,
            "a read without a byte limit is not repeated"
        );
        worker.complete(completed);
        worker.flush_errors();
        assert_eq!(
            next_error(&mut receiver),
            ReadError::new(Some(0), IggyError::InvalidSizeBytes),
            "only the read without a byte limit reports the error"
        );
        assert!(!worker.read_alone.contains(&0));
    }

    #[tokio::test]
    async fn given_a_last_start_when_its_first_record_is_above_max_bytes_should_report_the_refusal_without_a_read_alone()
     {
        // Above 1, because a `Last` read of one message keeps its read alone: both start at the
        // last message.
        const COUNT: u32 = 2;
        for server_commit in [ServerCommitPolicy::Disabled, ServerCommitPolicy::OnRead] {
            let (client, cluster) = connect_to_one_replica().await;
            cluster.script(
                &endpoint(0),
                POLL_MESSAGES_DEFERRED_CODE,
                [Answer::Refuse(IggyError::InvalidSizeBytes)],
            );
            let (mut worker, mut receiver) = worker_of(
                builder_on(client, Consumer::default())
                    .partitions([0])
                    .start(PollingStrategy::last())
                    .options(PollOptions {
                        count: COUNT,
                        server_commit,
                        ..PollOptions::default()
                    }),
            );
            assert!(worker.apply(assigned(&worker, SESSION, VERSION, [0])));
            worker.shared.demand.store(true, ORDERING);
            worker.schedule();
            let completed = worker.tasks.join_next().await.unwrap().unwrap();
            let polls = cluster.received(POLL_MESSAGES_DEFERRED_CODE);
            assert_eq!(
                polls.len(),
                1,
                "{server_commit:?}: a `Last` read of one message would start at the last message and skip the ones before it"
            );
            let request = DeferredPollMessagesRequest::decode_from(&polls[0].body).unwrap();
            assert_eq!(request.poll.count, COUNT);
            worker.complete(completed);
            worker.flush_errors();
            assert_eq!(
                next_error(&mut receiver),
                ReadError::new(Some(0), IggyError::InvalidSizeBytes),
                "{server_commit:?}"
            );
            assert_eq!(worker.position(0), PollingStrategy::last());
            assert!(!worker.read_alone.contains(&0));
        }
    }

    fn group() -> Consumer {
        Consumer::group(Identifier::numeric(GROUP_ID).unwrap())
    }

    fn standalone(partitions: impl IntoIterator<Item = u32>) -> ReaderBuilder {
        builder_for(Consumer::default()).partitions(partitions)
    }

    /// The server stores the offset of each read.
    fn committing(builder: ReaderBuilder) -> ReaderBuilder {
        builder.options(PollOptions {
            server_commit: ServerCommitPolicy::OnRead,
            ..PollOptions::default()
        })
    }

    fn worker_of(builder: ReaderBuilder) -> (Worker, Replies) {
        let (config, shared) = builder.into_parts().unwrap();
        worker_on(config, shared)
    }

    fn worker_on(config: Config, shared: Shared) -> (Worker, Replies) {
        let (sender, receiver) = mpsc::channel(config.slots);
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
        let commits = worker.config.on_read() && kind == FetchKind::Alone;
        Completed::Poll(CompletedPoll {
            epoch: worker.epoch,
            kind,
            partition,
            strategy,
            generation: worker.generation(partition),
            result,
            capacity: worker.reserve().unwrap(),
            commit: commits
                .then(|| PendingCommit::new(Arc::clone(&worker.shared.progress), vec![partition])),
        })
    }

    fn fetch_of(worker: &Worker, partition: u32) -> u64 {
        worker.active[&partition].fetch
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
                matches!(completed.result, Err(IggyError::TransientNotAccepted)),
                "a read that waits past its deadline for the client was never sent"
            );
            completed.result = Err(IggyError::CannotReadMessage);
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
