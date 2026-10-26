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

//! Deferred reads remain provisional until the owner selects one result.
//! One registry owns readiness deadlines and quotas. Detached readers
//! carry only a request identity and a reservation that survives cancellation.
//! Readiness waits share the registry, because it owns the one poll notifier
//! of each partition.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE;
use iggy_common::{DeferredPollOptions, IggyError, PollingKind, PollingStrategy};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{Partition, PollReadResult, PollVisibility, PollingArgs, PollingConsumer};
use server_common::poll::PollHistoryId;
use server_common::sharding::IggyNamespace;

use crate::config::DeferredPollConfig;
use crate::metrics::DeferredPollMetrics;
use crate::shards_table::ShardsTable;
use crate::{IggyShard, PartitionReadReply, Sender};
use prometheus_client::metrics::gauge::Gauge;

const SERVICE_BUDGET: usize = 8;
const READ_BUDGET_PARTS: usize = 3;
const MAINTENANCE_BUDGET: usize = 16;

/// Auxiliary connections are charged to their parent session. HTTP requests
/// without a binary session are charged to their authenticated user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PollQuotaIdentity {
    Session(u128),
    User(u32),
}

#[derive(Debug)]
pub struct DeferredPollContext {
    pub session: Option<SessionAttachment>,
    pub metadata: PollMetadata,
    pub user_id: u32,
    pub user_created_at: Option<u64>,
    pub quota: PollQuotaIdentity,
    pub primary: bool,
}

/// A deferred poll as the origin resolved it.
///
/// The owner shard sets both deadlines from its own clock when the request
/// arrives through `partition_read`, so a request that arrives any other way
/// expires at once.
#[derive(Debug)]
pub struct DeferredPollRequest {
    pub consumer: PollingConsumer,
    pub args: PollingArgs,
    pub context: DeferredPollContext,
    pub options: DeferredPollOptions,
    pub(crate) deadline: u64,
    pub(crate) request_deadline: u64,
}

impl DeferredPollRequest {
    #[must_use]
    pub const fn new(
        consumer: PollingConsumer,
        args: PollingArgs,
        context: DeferredPollContext,
        options: DeferredPollOptions,
    ) -> Self {
        Self {
            consumer,
            args,
            context,
            options,
            deadline: 0,
            request_deadline: 0,
        }
    }
}

/// One partition of a readiness wait, resolved and authorized by the origin.
#[derive(Debug)]
pub struct ReadinessTarget {
    pub namespace: IggyNamespace,
    pub partition_id: u32,
    pub consumer: PollingConsumer,
    /// `Offset` or `Next`, the start point of a poll with the same strategy.
    pub strategy: PollingStrategy,
    pub context: DeferredPollContext,
}

/// The targets of one readiness wait that a single owner shard holds.
#[derive(Debug)]
pub struct ReadinessWait {
    pub targets: Vec<ReadinessTarget>,
    pub deadline: u64,
}

/// A readiness waiter answers once. `admitted` holds the history and view of
/// each target at admission, and any change rejects the waiter.
struct ReadinessWaiter {
    wait: ReadinessWait,
    admitted: Vec<(PollHistoryId, u32)>,
    quota: PollQuotaIdentity,
    reply: Sender<PartitionReadReply>,
}

struct Waiter {
    namespace: IggyNamespace,
    request: DeferredPollRequest,
    reply: Sender<PartitionReadReply>,
    history: PollHistoryId,
    view: u32,
    /// The visibility at the last read, `None` before the first read.
    last_visibility: Option<PollVisibility>,
    running: bool,
    terminal: bool,
    /// When a full read budget first refused this waiter. It keeps the
    /// waiter's place in the read queue until one of its reads starts.
    refused_at: Option<u64>,
    /// At most what a read from the start through `last_visibility` selects.
    selected: Selection,
}

impl Waiter {
    /// At most what a read from the start selects at `visibility`. Offsets
    /// are consecutive, so the offsets committed after the last read bound
    /// the messages that a read can add, whatever the polling kind.
    fn selection_at(&self, visibility: PollVisibility) -> Option<Selection> {
        let last = self.last_visibility?;
        let args = &self.request.args;
        let mut from = last.offset.map_or(0, |offset| offset.saturating_add(1));
        if args.kind == PollingKind::Offset {
            from = from.max(args.value);
        }
        let messages = visibility
            .offset
            .map_or(0, |offset| offset.saturating_add(1).saturating_sub(from));
        let bytes = visibility
            .committed_bytes
            .saturating_sub(last.committed_bytes);
        Some(Selection {
            messages: u32::try_from(messages).map_or(u32::MAX, |messages| {
                self.selected.messages.saturating_add(messages)
            }),
            bytes: usize::try_from(bytes).map_or(usize::MAX, |bytes| {
                self.selected.bytes.saturating_add(bytes)
            }),
        })
    }
}

#[derive(Clone, Copy, Default)]
struct Selection {
    messages: u32,
    /// Counted as `PollReadResult::limit_bytes` counts them.
    bytes: usize,
}

impl Selection {
    /// The reasons of `accept_deferred_result` that do not depend on time. A
    /// disk read that its walk budget would stop earlier is not predicted, and
    /// it runs when one of these holds or when the wait expires.
    fn finishes_read(self, options: &DeferredPollOptions, max_read_bytes: usize) -> bool {
        let byte_limit = (options.max_bytes as usize)
            .min(max_read_bytes)
            .saturating_sub(POLL_RESPONSE_HEADER_SIZE);
        self.messages >= options.min_count || self.bytes >= byte_limit
    }
}

/// Only a watched partition has a notifier, so only its commits mark it.
#[derive(Default)]
struct Signals {
    queued: HashSet<IggyNamespace>,
    dirty: VecDeque<IggyNamespace>,
    waker: Option<Waker>,
}

impl Signals {
    fn mark(&mut self, namespace: IggyNamespace) {
        if self.queued.insert(namespace) {
            self.dirty.push_back(namespace);
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

#[derive(Default)]
struct State {
    next_id: u64,
    waiters: BTreeMap<u64, Waiter>,
    readiness: BTreeMap<u64, ReadinessWaiter>,
    partitions: BTreeMap<IggyNamespace, BTreeSet<u64>>,
    deadlines: BTreeSet<(u64, u64)>,
    sessions: BTreeMap<PollQuotaIdentity, usize>,
    ready: VecDeque<u64>,
    queued: BTreeSet<u64>,
    /// Parked waiters whose read the budget refused, by `(refused_at, id)`.
    read_queue: BTreeSet<(u64, u64)>,
    maintenance_cursor: u64,
    closed: bool,
}

impl State {
    fn enqueue(&mut self, id: u64) {
        if (self.waiters.contains_key(&id) || self.readiness.contains_key(&id))
            && self.queued.insert(id)
        {
            self.ready.push_back(id);
        }
    }

    fn pending(&self) -> usize {
        self.waiters.len() + self.readiness.len()
    }

    /// Returns a new waiter id if the registry and the session quota have room.
    fn admit(
        &mut self,
        quota: PollQuotaIdentity,
        config: &DeferredPollConfig,
    ) -> Result<u64, IggyError> {
        if self.closed {
            return Err(IggyError::TransientNotAccepted);
        }
        if self.pending() >= config.max_pending
            || self.sessions.get(&quota).copied().unwrap_or(0) >= config.max_pending_per_session
        {
            return Err(IggyError::DeferredPollCapacityExceeded);
        }
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(IggyError::TransientNotAccepted)?;
        self.next_id = id;
        Ok(id)
    }

    fn ids_after(&self, cursor: u64) -> Vec<u64> {
        let after = (Excluded(cursor), Unbounded);
        let mut ids: Vec<_> = self
            .waiters
            .range(after)
            .map(|(id, _)| *id)
            .take(MAINTENANCE_BUDGET)
            .chain(
                self.readiness
                    .range(after)
                    .map(|(id, _)| *id)
                    .take(MAINTENANCE_BUDGET),
            )
            .collect();
        ids.sort_unstable();
        ids.truncate(MAINTENANCE_BUDGET);
        ids
    }
}

pub struct DeferredPolls {
    metrics: DeferredPollMetrics,
    config: RefCell<DeferredPollConfig>,
    state: RefCell<State>,
    signals: Rc<RefCell<Signals>>,
    inflight_bytes: Arc<AtomicUsize>,
}

impl DeferredPolls {
    pub(crate) fn new(metrics: DeferredPollMetrics) -> Self {
        Self {
            metrics,
            config: RefCell::new(DeferredPollConfig::default()),
            state: RefCell::new(State::default()),
            signals: Rc::new(RefCell::new(Signals::default())),
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The wait a request gets. A client wait only bounds it, so a wait above
    /// this server's limit waits for the limit instead of failing.
    pub(crate) fn wait_us(&self, wait_us: u64) -> u64 {
        wait_us.min(self.config.borrow().max_wait_us)
    }

    pub(crate) fn earliest_deadline(&self) -> Option<u64> {
        self.state
            .borrow()
            .deadlines
            .first()
            .map(|(deadline, _)| *deadline)
    }

    pub(crate) fn poll_ready(&self, context: &Context<'_>) -> Poll<()> {
        let mut signals = self.signals.borrow_mut();
        if !signals.dirty.is_empty() || !self.state.borrow().ready.is_empty() {
            Poll::Ready(())
        } else {
            signals.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }

    pub(crate) fn maintenance(&self) {
        let mut state = self.state.borrow_mut();
        let mut ids = state.ids_after(state.maintenance_cursor);
        if ids.is_empty() {
            ids = state.ids_after(0);
        }
        if let Some(last) = ids.last() {
            state.maintenance_cursor = *last;
        }
        for id in ids {
            state.enqueue(id);
        }
    }

    pub(crate) const fn pump_guard(&self) -> PumpGuard<'_> {
        PumpGuard(self)
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.borrow_mut();
        state.closed = true;
        for reply in state
            .waiters
            .values()
            .map(|waiter| &waiter.reply)
            .chain(state.readiness.values().map(|waiter| &waiter.reply))
        {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        }
        state.waiters.clear();
        state.readiness.clear();
        state.partitions.clear();
        state.deadlines.clear();
        state.sessions.clear();
        state.ready.clear();
        state.queued.clear();
        state.read_queue.clear();
        self.metrics.pending.set(0);
        *self.signals.borrow_mut() = Signals::default();
    }

    fn has_read_room(&self) -> bool {
        Self::with_read(
            &self.config.borrow(),
            self.inflight_bytes.load(Ordering::Relaxed),
        )
        .is_some()
    }

    /// Takes a waiter out of the read queue for a probe. Only a read that the
    /// budget refuses again puts it back, at the same place.
    fn take_read_place(&self, id: u64, waiter: &mut Waiter) -> Option<u64> {
        let refused_at = waiter.refused_at.take()?;
        self.state.borrow_mut().read_queue.remove(&(refused_at, id));
        Some(refused_at)
    }

    /// A read starts only ahead of every refused read, so that a waiter
    /// whose read keeps coming back cannot take the budget from a refused
    /// one. A refused read joins the queue at `place`.
    fn reserve_read_in_turn(&self, place: (u64, u64)) -> Option<ReadReservation> {
        let mut state = self.state.borrow_mut();
        let reservation = state
            .read_queue
            .first()
            .is_none_or(|first| place < *first)
            .then(|| self.reserve_read().ok())
            .flatten();
        if reservation.is_none() {
            state.read_queue.insert(place);
        }
        reservation
    }

    fn reserve_read(&self) -> Result<ReadReservation, IggyError> {
        let config = *self.config.borrow();
        self.inflight_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                Self::with_read(&config, used)
            })
            .map_err(|_| IggyError::DeferredPollCapacityExceeded)?;
        self.metrics
            .inflight_bytes
            .inc_by(i64::try_from(config.max_read_bytes).unwrap_or(i64::MAX));
        Ok(ReadReservation {
            gauge: self.metrics.inflight_bytes.clone(),
            bytes: config.max_read_bytes,
            used: Arc::clone(&self.inflight_bytes),
        })
    }

    /// The in-flight read bytes after one more reservation, if they fit.
    fn with_read(config: &DeferredPollConfig, used: usize) -> Option<usize> {
        used.checked_add(config.max_read_bytes)
            .filter(|next| *next <= config.max_inflight_bytes)
    }

    /// Returns the namespaces that no waiter watches any more.
    fn remove(
        &self,
        id: u64,
        deadlines: &[u64],
        quota: PollQuotaIdentity,
        namespaces: impl IntoIterator<Item = IggyNamespace>,
    ) -> Vec<IggyNamespace> {
        let mut state = self.state.borrow_mut();
        for deadline in deadlines {
            state.deadlines.remove(&(*deadline, id));
        }
        state.queued.remove(&id);
        state.ready.retain(|queued| *queued != id);
        state.read_queue.retain(|(_, queued)| *queued != id);
        self.metrics.pending.dec();
        if let Some(count) = state.sessions.get_mut(&quota) {
            *count -= 1;
            if *count == 0 {
                state.sessions.remove(&quota);
            }
        }
        let mut unwatched = Vec::new();
        for namespace in namespaces {
            let empty = state.partitions.get_mut(&namespace).is_some_and(|ids| {
                ids.remove(&id);
                ids.is_empty()
            });
            if empty {
                state.partitions.remove(&namespace);
                unwatched.push(namespace);
            }
        }
        unwatched
    }
}

pub struct PumpGuard<'a>(&'a DeferredPolls);

impl Drop for PumpGuard<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// May cross the completion channel; it accounts for running and queued reads.
pub struct ReadReservation {
    gauge: Gauge,
    bytes: usize,
    used: Arc<AtomicUsize>,
}

impl Drop for ReadReservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::Relaxed);
        self.gauge
            .dec_by(i64::try_from(self.bytes).unwrap_or(i64::MAX));
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    M: StreamsFrontend,
    SB: SuperblockStore,
{
    pub fn set_deferred_poll_config(&self, config: DeferredPollConfig) {
        *self.deferred_polls.config.borrow_mut() = config;
    }

    pub fn deferred_wait_us(&self, wait_us: u64) -> u64 {
        self.deferred_polls.wait_us(wait_us)
    }

    fn validate_deferred_context(
        &self,
        namespace: IggyNamespace,
        context: &DeferredPollContext,
        consumer: PollingConsumer,
    ) -> Result<(PollHistoryId, PollVisibility, u32), IggyError> {
        if context
            .session
            .as_ref()
            .is_some_and(|session| !session.is_valid())
        {
            return Err(IggyError::StaleClient);
        }
        if !self
            .plane
            .metadata()
            .applied_frontier()
            .partition_requests_allowed(self.bus.monotonic_now())
        {
            return Err(IggyError::TransientNotAccepted);
        }
        let metadata = &self.plane.metadata().mux_stm;
        if context.user_created_at.is_some_and(|created_at| {
            !metadata
                .users()
                .read(|users| users.is_active_account(context.user_id, created_at))
        }) {
            return Err(IggyError::Unauthenticated);
        }
        metadata.users().authorize(|permissioner| {
            permissioner.poll_messages(context.user_id, namespace.stream_id(), namespace.topic_id())
        })?;
        if !context.metadata.is_valid(metadata.streams(), namespace) {
            return Err(IggyError::TransientNotAccepted);
        }
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                if partition.history_deleted() {
                    return Err(IggyError::HistoryUnavailable);
                }
                if context.primary && consensus.is_normal_backup() {
                    return Err(IggyError::NotPrimary);
                }
                if let PollingConsumer::ConsumerGroup(group, _) = consumer
                    && let Some(error) = super::group_owner_rejection(
                        namespace,
                        group as u64,
                        partition.consumer_group_owner(group as u64),
                        &context.metadata,
                    )
                {
                    return Err(error);
                }
                if partition.fatal().is_some()
                    || partition.read_history_is_changing()
                    || (context.primary && !consensus.applied_view_start())
                    || partition.requires_state_transfer()
                    || !consensus.is_normal()
                    || consensus.is_transferring()
                    || (context.primary && !consensus.is_primary())
                    || !context
                        .metadata
                        .matches_partition(self.shards_table.epoch_for(namespace))
                {
                    return Err(IggyError::TransientNotAccepted);
                }
                let (history, visibility) = partition.poll_visibility();
                Ok((history, visibility, consensus.view()))
            })
            .ok_or(IggyError::TransientNotAccepted)?
    }

    /// The visibility of the partition, while its history and view are those
    /// of the admission.
    fn validate_waiter(&self, waiter: &Waiter) -> Result<PollVisibility, IggyError> {
        let (history, visibility, view) = self.validate_deferred_context(
            waiter.namespace,
            &waiter.request.context,
            waiter.request.consumer,
        )?;
        if history != waiter.history || view != waiter.view {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(visibility)
    }

    /// The pump services the admitted waiter at the top of its next turn.
    #[allow(clippy::too_many_lines)]
    pub(super) fn admit_deferred_poll(
        &self,
        namespace: IggyNamespace,
        mut request: DeferredPollRequest,
        reply: &Sender<PartitionReadReply>,
    ) {
        if reply.is_disconnected() {
            return;
        }
        let admission = (|| {
            request.options.validate(request.args.count)?;
            let (history, _, view) =
                self.validate_deferred_context(namespace, &request.context, request.consumer)?;
            let config = *self.deferred_polls.config.borrow();
            let mut state = self.deferred_polls.state.borrow_mut();
            let id = state.admit(request.context.quota, &config)?;
            if request.args.kind == PollingKind::Next {
                let stored = self
                    .plane
                    .partitions()
                    .with_partition(&namespace, |partition| {
                        partition.get_consumer_offset(request.consumer)
                    })
                    .ok_or(IggyError::TransientNotAccepted)?;
                let offset = stored
                    .map(|offset| {
                        offset
                            .checked_add(1)
                            .ok_or(IggyError::InvalidOffset(offset))
                    })
                    .transpose()?
                    .unwrap_or(0);
                request.args.kind = PollingKind::Offset;
                request.args.value = offset;
            }
            self.register_waiter(
                &mut state,
                id,
                (request.context.quota, request.deadline),
                [namespace],
            );
            state.waiters.insert(
                id,
                Waiter {
                    namespace,
                    request,
                    reply: reply.clone(),
                    history,
                    view,
                    last_visibility: None,
                    running: false,
                    terminal: false,
                    refused_at: None,
                    selected: Selection::default(),
                },
            );
            state.enqueue(id);
            Ok(())
        })();
        if let Err(error) = admission {
            self.metrics.deferred_polls.rejected.inc();
            let _ = reply.try_send(PartitionReadReply::Rejected(error));
        }
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn service_deferred_polls(&self) {
        for _ in 0..SERVICE_BUDGET {
            let now = self.bus.monotonic_micros();
            let next = {
                let mut state = self.deferred_polls.state.borrow_mut();
                if let Some(&(deadline, id)) = state.deadlines.first()
                    && deadline <= now
                {
                    Some(id)
                } else {
                    let namespace = {
                        let mut signals = self.deferred_polls.signals.borrow_mut();
                        let namespace = signals.dirty.pop_front();
                        if let Some(namespace) = namespace {
                            signals.queued.remove(&namespace);
                        }
                        namespace
                    };
                    if let Some(namespace) = namespace {
                        let ids: Vec<_> = state
                            .partitions
                            .get(&namespace)
                            .into_iter()
                            .flat_map(|ids| ids.iter().copied())
                            .collect();
                        for id in ids {
                            state.enqueue(id);
                        }
                    }
                    // Freed room goes to the read that the budget refused first.
                    if let Some(&(_, id)) = state.read_queue.first()
                        && self.deferred_polls.has_read_room()
                    {
                        state.enqueue(id);
                    }
                    let next = state.ready.pop_front();
                    if let Some(id) = next {
                        state.queued.remove(&id);
                    }
                    next
                }
            };
            let Some(id) = next else { break };
            let waiter = self.deferred_polls.state.borrow_mut().waiters.remove(&id);
            if let Some(waiter) = waiter {
                self.probe_deferred_poll(id, waiter).await;
                continue;
            }
            let waiter = self.deferred_polls.state.borrow_mut().readiness.remove(&id);
            if let Some(waiter) = waiter {
                self.probe_readiness(id, waiter);
            }
        }
    }

    #[allow(clippy::future_not_send)]
    async fn probe_deferred_poll(&self, id: u64, mut waiter: Waiter) {
        let refused_at = self.deferred_polls.take_read_place(id, &mut waiter);
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_deferred_poll(id, &waiter);
            return;
        }
        let visibility = match self.validate_waiter(&waiter) {
            Ok(visibility) => visibility,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        if self.bus.monotonic_micros() >= waiter.request.deadline {
            waiter.terminal = true;
            let mut state = self.deferred_polls.state.borrow_mut();
            state.deadlines.remove(&(waiter.request.deadline, id));
            state
                .deadlines
                .insert((waiter.request.request_deadline, id));
        }
        // A read from the start that cannot finish would only park the waiter
        // again. What was committed after the last read bounds what it adds.
        if !waiter.running
            && !waiter.terminal
            && waiter.last_visibility != Some(visibility)
            && let Some(selected) = waiter.selection_at(visibility)
            && !selected.finishes_read(
                &waiter.request.options,
                self.deferred_polls.config.borrow().max_read_bytes,
            )
        {
            waiter.last_visibility = Some(visibility);
            waiter.selected = selected;
        }
        if waiter.running || (!waiter.terminal && waiter.last_visibility == Some(visibility)) {
            self.deferred_polls
                .state
                .borrow_mut()
                .waiters
                .insert(id, waiter);
            return;
        }
        let Some(plan) = self.plane.partitions().build_poll_snapshot(
            &waiter.namespace,
            waiter.request.consumer,
            &waiter.request.args,
        ) else {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotAccepted);
            return;
        };
        if !plan.needs_off_pump_io() {
            waiter.last_visibility = Some(visibility);
            let result = plan.execute_resident();
            self.accept_deferred_result(id, waiter, result).await;
            return;
        }
        // A busy budget only delays the read. The waiter keeps its last
        // visibility, so that its next probe does not skip the read.
        let place = (
            refused_at.unwrap_or_else(|| self.bus.monotonic_micros()),
            id,
        );
        let Some(reservation) = self.deferred_polls.reserve_read_in_turn(place) else {
            waiter.refused_at = Some(place.0);
            self.deferred_polls
                .state
                .borrow_mut()
                .waiters
                .insert(id, waiter);
            return;
        };
        // Snapshot, disk selection and index loading can coexist.
        let max_bytes = reservation.bytes / READ_BUDGET_PARTS;
        let completion =
            match self
                .poll_completions
                .try_reserve_deferred(waiter.namespace, id, reservation)
            {
                Ok(completion) => completion,
                Err(error) => {
                    self.reject_deferred_poll(id, &waiter, error);
                    return;
                }
            };
        waiter.last_visibility = Some(visibility);
        waiter.running = true;
        self.deferred_polls
            .state
            .borrow_mut()
            .waiters
            .insert(id, waiter);
        self.bus.spawn(async move {
            completion.complete(plan.execute_with_limit(max_bytes).await);
        });
    }

    #[allow(clippy::future_not_send)]
    pub(super) async fn on_deferred_poll_completed(&self, id: u64, result: PollReadResult) {
        let waiter = self.deferred_polls.state.borrow_mut().waiters.remove(&id);
        if let Some(mut waiter) = waiter {
            waiter.running = false;
            self.accept_deferred_result(id, waiter, result).await;
        } else {
            self.metrics.deferred_polls.late.inc();
        }
    }

    #[allow(clippy::future_not_send)]
    async fn accept_deferred_result(&self, id: u64, mut waiter: Waiter, result: PollReadResult) {
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_deferred_poll(id, &waiter);
            return;
        }
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        let visibility = match self.validate_waiter(&waiter) {
            Ok(visibility) => visibility,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        let result = match result.checked() {
            Ok(result) => result,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        let max_read_bytes = self.deferred_polls.config.borrow().max_read_bytes;
        let (mut result, byte_limited) =
            match result.limit_bytes(waiter.request.options.max_bytes as usize, max_read_bytes) {
                Ok(result) => result,
                Err(error) => {
                    self.reject_deferred_poll(id, &waiter, error);
                    return;
                }
            };
        if result.message_count() >= waiter.request.options.min_count
            || byte_limited
            || result.truncated()
            || waiter.terminal
            || self.bus.monotonic_micros() >= waiter.request.deadline
        {
            // Charge what the reply carries, not what the read walked over. A
            // selection that borrows journal buffers can still pin more than
            // the reservation, so copy it out instead of refusing a serveable
            // poll.
            if result.retained_bytes() > max_read_bytes {
                result = result.compacted();
            }
            self.finish_deferred_poll(id, waiter, result).await;
            return;
        }
        waiter.selected = Selection {
            messages: result.message_count(),
            bytes: result.encoded_bytes(),
        };
        let changed = waiter.last_visibility != Some(visibility);
        let mut state = self.deferred_polls.state.borrow_mut();
        state.waiters.insert(id, waiter);
        if changed {
            state.enqueue(id);
        }
    }

    fn remove_deferred_poll(&self, id: u64, waiter: &Waiter) {
        let request = &waiter.request;
        self.unwatch_partitions(self.deferred_polls.remove(
            id,
            &[request.deadline, request.request_deadline],
            request.context.quota,
            [waiter.namespace],
        ));
    }

    /// Counts the waiter toward its quota and makes a visibility change of
    /// any of `namespaces` wake it.
    fn register_waiter(
        &self,
        state: &mut State,
        id: u64,
        (quota, deadline): (PollQuotaIdentity, u64),
        namespaces: impl IntoIterator<Item = IggyNamespace>,
    ) {
        self.metrics.deferred_polls.pending.inc();
        *state.sessions.entry(quota).or_default() += 1;
        state.deadlines.insert((deadline, id));
        for namespace in namespaces {
            state.partitions.entry(namespace).or_default().insert(id);
            let signals = Rc::downgrade(&self.deferred_polls.signals);
            if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                partition.set_poll_notifier(Some(Rc::new(move || {
                    if let Some(signals) = signals.upgrade() {
                        signals.borrow_mut().mark(namespace);
                    }
                })));
            }
        }
    }

    fn unwatch_partitions(&self, namespaces: Vec<IggyNamespace>) {
        for namespace in namespaces {
            if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                partition.set_poll_notifier(None);
            }
        }
    }

    fn reject_deferred_poll(&self, id: u64, waiter: &Waiter, error: IggyError) {
        self.metrics.deferred_polls.failed.inc();
        self.remove_deferred_poll(id, waiter);
        let _ = waiter.reply.try_send(PartitionReadReply::Rejected(error));
    }

    #[allow(clippy::future_not_send)]
    async fn finish_deferred_poll(&self, id: u64, waiter: Waiter, result: PollReadResult) {
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        if let Err(error) = self.validate_waiter(&waiter) {
            self.reject_deferred_poll(id, &waiter, error);
            return;
        }
        self.remove_deferred_poll(id, &waiter);
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            return;
        }
        let before_deadline = self.bus.monotonic_micros() < waiter.request.deadline;
        if before_deadline {
            self.metrics.deferred_polls.filled.inc();
        } else {
            self.metrics.deferred_polls.expired.inc();
        }
        self.accept_poll_result(waiter.namespace, result, waiter.reply)
            .await;
    }

    /// Answers at once with the ready targets. If none is ready and the wait
    /// did not expire, parks one waiter for all targets and answers with its
    /// id, so that the origin can cancel it.
    pub(crate) fn admit_readiness_wait(
        &self,
        wait: ReadinessWait,
        reply: &Sender<PartitionReadReply>,
    ) {
        if reply.is_disconnected() {
            return;
        }
        let answer = self
            .park_readiness_wait(wait, reply)
            .unwrap_or_else(|error| {
                self.metrics.deferred_polls.rejected.inc();
                PartitionReadReply::Rejected(error)
            });
        let _ = reply.try_send(answer);
    }

    pub(crate) fn cancel_readiness_wait(&self, id: u64) {
        let waiter = self.deferred_polls.state.borrow_mut().readiness.remove(&id);
        if let Some(waiter) = waiter {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_readiness_wait(id, &waiter);
        }
    }

    fn park_readiness_wait(
        &self,
        wait: ReadinessWait,
        reply: &Sender<PartitionReadReply>,
    ) -> Result<PartitionReadReply, IggyError> {
        let quota = wait
            .targets
            .first()
            .ok_or(IggyError::InvalidCommand)?
            .context
            .quota;
        let mut admitted = Vec::with_capacity(wait.targets.len());
        let mut partitions = Vec::new();
        for target in &wait.targets {
            let (history, view, ready) = self.evaluate_readiness(target)?;
            admitted.push((history, view));
            if ready {
                partitions.push(target.partition_id);
            }
        }
        let expired = self.bus.monotonic_micros() >= wait.deadline;
        if !partitions.is_empty() || expired {
            if partitions.is_empty() {
                self.metrics.deferred_polls.expired.inc();
            } else {
                self.metrics.deferred_polls.filled.inc();
            }
            return Ok(PartitionReadReply::Ready {
                shard: self.id,
                parked: None,
                partitions,
            });
        }
        let config = *self.deferred_polls.config.borrow();
        let mut state = self.deferred_polls.state.borrow_mut();
        let id = state.admit(quota, &config)?;
        self.register_waiter(
            &mut state,
            id,
            (quota, wait.deadline),
            wait.targets.iter().map(|target| target.namespace),
        );
        state.readiness.insert(
            id,
            ReadinessWaiter {
                wait,
                admitted,
                quota,
                reply: reply.clone(),
            },
        );
        Ok(PartitionReadReply::Ready {
            shard: self.id,
            parked: Some(id),
            partitions,
        })
    }

    /// Returns the history and view of the target, and whether a poll with its
    /// strategy returns a message now. This is the empty-plan test of
    /// `build_poll_plan`.
    fn evaluate_readiness(
        &self,
        target: &ReadinessTarget,
    ) -> Result<(PollHistoryId, u32, bool), IggyError> {
        let (history, visibility, view) =
            self.validate_deferred_context(target.namespace, &target.context, target.consumer)?;
        let start = match target.strategy.kind {
            PollingKind::Offset => target.strategy.value,
            PollingKind::Next => self
                .plane
                .partitions()
                .with_partition(&target.namespace, |partition| {
                    partition.get_consumer_offset(target.consumer)
                })
                .ok_or(IggyError::TransientNotAccepted)?
                .map_or(Ok(0), |offset| {
                    offset
                        .checked_add(1)
                        .ok_or(IggyError::InvalidOffset(offset))
                })?,
            _ => return Err(IggyError::InvalidCommand),
        };
        Ok((
            history,
            view,
            visibility.offset.is_some_and(|commit| start <= commit),
        ))
    }

    fn probe_readiness(&self, id: u64, waiter: ReadinessWaiter) {
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_readiness_wait(id, &waiter);
            return;
        }
        // The origin answers an expired wait itself.
        if self.bus.monotonic_micros() >= waiter.wait.deadline {
            self.metrics.deferred_polls.expired.inc();
            self.remove_readiness_wait(id, &waiter);
            return;
        }
        let partitions = waiter
            .wait
            .targets
            .iter()
            .zip(&waiter.admitted)
            .filter_map(|(target, admitted)| match self.evaluate_readiness(target) {
                Ok((history, view, ready)) if (history, view) == *admitted => {
                    ready.then_some(Ok(target.partition_id))
                }
                Ok(_) => Some(Err(IggyError::TransientNotAccepted)),
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>();
        let answer = match partitions {
            Ok(partitions) if partitions.is_empty() => {
                self.deferred_polls
                    .state
                    .borrow_mut()
                    .readiness
                    .insert(id, waiter);
                return;
            }
            Ok(partitions) => {
                self.metrics.deferred_polls.filled.inc();
                PartitionReadReply::Ready {
                    shard: self.id,
                    parked: None,
                    partitions,
                }
            }
            Err(error) => {
                self.metrics.deferred_polls.failed.inc();
                PartitionReadReply::Rejected(error)
            }
        };
        self.remove_readiness_wait(id, &waiter);
        let _ = waiter.reply.try_send(answer);
    }

    fn remove_readiness_wait(&self, id: u64, waiter: &ReadinessWaiter) {
        self.unwatch_partitions(self.deferred_polls.remove(
            id,
            &[waiter.wait.deadline],
            waiter.quota,
            waiter.wait.targets.iter().map(|target| target.namespace),
        ));
    }
}

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;
