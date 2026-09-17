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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_common::{DeferredPollOptions, IggyError, PollingKind, PollingStrategy};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{Partition, PollReadResult, PollingArgs, PollingConsumer};
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
    pub quota: PollQuotaIdentity,
    pub primary: bool,
}

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

struct Waiter {
    namespace: IggyNamespace,
    request: DeferredPollRequest,
    reply: Sender<PartitionReadReply>,
    history: PollHistoryId,
    view: u32,
    last_visibility: Option<(PollHistoryId, Option<u64>)>,
    running: bool,
    terminal: bool,
}

#[derive(Default)]
struct Signals {
    active: HashSet<IggyNamespace>,
    queued: HashSet<IggyNamespace>,
    dirty: VecDeque<IggyNamespace>,
    waker: Option<Waker>,
}

impl Signals {
    fn mark(&mut self, namespace: IggyNamespace) {
        if self.active.contains(&namespace) && self.queued.insert(namespace) {
            self.dirty.push_back(namespace);
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    fn remove(&mut self, namespace: IggyNamespace) {
        self.active.remove(&namespace);
        self.queued.remove(&namespace);
        self.dirty.retain(|queued| *queued != namespace);
    }
}

#[derive(Default)]
struct State {
    next_id: u64,
    waiters: BTreeMap<u64, Waiter>,
    partitions: BTreeMap<IggyNamespace, BTreeSet<u64>>,
    deadlines: BTreeSet<(u64, u64)>,
    sessions: BTreeMap<PollQuotaIdentity, usize>,
    ready: VecDeque<u64>,
    queued: BTreeSet<u64>,
    maintenance_cursor: u64,
    closed: bool,
}

impl State {
    fn enqueue(&mut self, id: u64) {
        if self.waiters.contains_key(&id) && self.queued.insert(id) {
            self.ready.push_back(id);
        }
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
}

impl DeferredPolls {
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
        let mut ids: Vec<_> = state
            .waiters
            .range((Excluded(state.maintenance_cursor), Unbounded))
            .take(MAINTENANCE_BUDGET)
            .map(|(id, _)| *id)
            .collect();
        if ids.is_empty() {
            ids.extend(state.waiters.keys().take(MAINTENANCE_BUDGET).copied());
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
        for waiter in state.waiters.values() {
            let _ = waiter.reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        }
        state.waiters.clear();
        state.partitions.clear();
        state.deadlines.clear();
        state.sessions.clear();
        state.ready.clear();
        state.queued.clear();
        self.metrics.pending.set(0);
        *self.signals.borrow_mut() = Signals::default();
    }

    fn reserve_read(&self) -> Result<ReadReservation, IggyError> {
        let config = *self.config.borrow();
        self.inflight_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(config.max_read_bytes)
                    .filter(|next| *next <= config.max_inflight_bytes)
            })
            .map_err(|_| IggyError::TransientNotAccepted)?;
        self.metrics
            .inflight_bytes
            .inc_by(i64::try_from(config.max_read_bytes).unwrap_or(i64::MAX));
        Ok(ReadReservation {
            gauge: self.metrics.inflight_bytes.clone(),
            bytes: config.max_read_bytes,
            used: Arc::clone(&self.inflight_bytes),
        })
    }

    fn remove(&self, id: u64, waiter: &Waiter) -> bool {
        let mut state = self.state.borrow_mut();
        state.deadlines.remove(&(waiter.request.deadline, id));
        state
            .deadlines
            .remove(&(waiter.request.request_deadline, id));
        state.queued.remove(&id);
        state.ready.retain(|queued| *queued != id);
        self.metrics.pending.dec();
        if let Some(count) = state.sessions.get_mut(&waiter.request.context.quota) {
            *count -= 1;
            if *count == 0 {
                state.sessions.remove(&waiter.request.context.quota);
            }
        }
        let empty = state
            .partitions
            .get_mut(&waiter.namespace)
            .is_some_and(|ids| {
                ids.remove(&id);
                ids.is_empty()
            });
        if empty {
            state.partitions.remove(&waiter.namespace);
            self.signals.borrow_mut().remove(waiter.namespace);
        }
        empty
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

    /// # Errors
    /// Rejects invalid readiness limits and waits outside the admission limit.
    pub fn validate_deferred_poll(
        &self,
        count: u32,
        options: DeferredPollOptions,
    ) -> Result<(), IggyError> {
        options.validate(count)?;
        if options.max_wait.as_micros() > self.deferred_polls.config.borrow().max_wait_us {
            return Err(IggyError::InvalidCommand);
        }
        Ok(())
    }

    fn validate_deferred_context(
        &self,
        namespace: IggyNamespace,
        context: &DeferredPollContext,
    ) -> Result<(PollHistoryId, Option<u64>, u32), IggyError> {
        if context
            .session
            .as_ref()
            .is_some_and(|session| !session.is_valid())
        {
            return Err(IggyError::StaleClient);
        }
        let metadata = &self.plane.metadata().mux_stm;
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
                if partition.fatal().is_some()
                    || partition.requires_state_transfer()
                    || !consensus.is_normal()
                    || consensus.is_transferring()
                    || (context.primary && !consensus.is_primary())
                    || !context.metadata.matches_partition(
                        self.shards_table.epoch_for(namespace),
                        partition.applied_purge_generation(),
                    )
                {
                    return Err(IggyError::TransientNotAccepted);
                }
                let (history, offset) = partition.poll_visibility();
                Ok((history, offset, consensus.view()))
            })
            .ok_or(IggyError::TransientNotAccepted)?
    }

    fn validate_waiter(&self, waiter: &Waiter) -> Result<(PollHistoryId, Option<u64>), IggyError> {
        let (history, offset, view) =
            self.validate_deferred_context(waiter.namespace, &waiter.request.context)?;
        if history != waiter.history || view != waiter.view {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok((history, offset))
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(super) async fn admit_deferred_poll(
        &self,
        namespace: IggyNamespace,
        mut request: DeferredPollRequest,
        reply: Sender<PartitionReadReply>,
    ) {
        if reply.is_disconnected() {
            return;
        }
        let admission = (|| {
            self.validate_deferred_poll(request.args.count, request.options)?;
            let (history, _, view) = self.validate_deferred_context(namespace, &request.context)?;
            let config = *self.deferred_polls.config.borrow();
            let mut state = self.deferred_polls.state.borrow_mut();
            if state.closed
                || state.waiters.len() >= config.max_pending
                || state
                    .sessions
                    .get(&request.context.quota)
                    .copied()
                    .unwrap_or(0)
                    >= config.max_pending_per_session
            {
                return Err(IggyError::TransientNotAccepted);
            }
            let id = state
                .next_id
                .checked_add(1)
                .ok_or(IggyError::TransientNotAccepted)?;
            state.next_id = id;
            if request.args.strategy.kind == PollingKind::Next {
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
                request.args.strategy = PollingStrategy::offset(offset);
            }
            self.metrics.deferred_polls.pending.inc();
            *state.sessions.entry(request.context.quota).or_default() += 1;
            state.deadlines.insert((request.deadline, id));
            state.partitions.entry(namespace).or_default().insert(id);
            self.deferred_polls
                .signals
                .borrow_mut()
                .active
                .insert(namespace);
            let signals = Rc::downgrade(&self.deferred_polls.signals);
            if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                partition.set_poll_notifier(Some(Rc::new(move || {
                    if let Some(signals) = signals.upgrade() {
                        signals.borrow_mut().mark(namespace);
                    }
                })));
            }
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
                },
            );
            state.enqueue(id);
            Ok(())
        })();
        if let Err(error) = admission {
            self.metrics.deferred_polls.rejected.inc();
            let _ = reply.try_send(PartitionReadReply::Rejected(error));
        }
        self.service_deferred_polls().await;
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
            }
        }
    }

    #[allow(clippy::future_not_send)]
    async fn probe_deferred_poll(&self, id: u64, mut waiter: Waiter) {
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
        if waiter.running || (!waiter.terminal && waiter.last_visibility == Some(visibility)) {
            self.deferred_polls
                .state
                .borrow_mut()
                .waiters
                .insert(id, waiter);
            return;
        }
        let reservation = match self.deferred_polls.reserve_read() {
            Ok(reservation) => reservation,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        // Snapshot, disk selection and index loading can coexist.
        let max_bytes = reservation.bytes / READ_BUDGET_PARTS;
        let Some(plan) = self.plane.partitions().build_poll_snapshot(
            &waiter.namespace,
            waiter.request.consumer,
            &waiter.request.args,
        ) else {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotAccepted);
            return;
        };
        waiter.last_visibility = Some(visibility);
        if plan.needs_off_pump_io() {
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
            waiter.running = true;
            self.deferred_polls
                .state
                .borrow_mut()
                .waiters
                .insert(id, waiter);
            self.bus.spawn(async move {
                completion.complete(plan.execute_with_limit(max_bytes).await);
            });
        } else {
            let result = plan.execute_resident();
            self.accept_deferred_result(id, waiter, result).await;
        }
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
    async fn accept_deferred_result(&self, id: u64, waiter: Waiter, result: PollReadResult) {
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
        let (mut result, byte_limited) =
            match result.limit_bytes(waiter.request.options.max_bytes as usize) {
                Ok(result) => result,
                Err(error) => {
                    self.reject_deferred_poll(id, &waiter, error);
                    return;
                }
            };
        // Charge what the reply carries, not what the read walked over. A
        // selection that borrows journal buffers can still pin more than the
        // reservation, so copy it out instead of refusing a serveable poll.
        let config = *self.deferred_polls.config.borrow();
        if result.retained_bytes() > config.max_read_bytes {
            result = result.compacted();
        }
        if result.message_count() >= waiter.request.options.min_count
            || byte_limited
            || waiter.terminal
            || self.bus.monotonic_micros() >= waiter.request.deadline
        {
            self.finish_deferred_poll(id, waiter, result).await;
            return;
        }
        let changed = waiter.last_visibility != Some(visibility);
        let mut state = self.deferred_polls.state.borrow_mut();
        state.waiters.insert(id, waiter);
        if changed {
            state.enqueue(id);
        }
    }

    fn remove_deferred_poll(&self, id: u64, waiter: &Waiter) {
        if self.deferred_polls.remove(id, waiter)
            && let Some(partition) = self.plane.partitions().get_mut_by_ns(&waiter.namespace)
        {
            partition.set_poll_notifier(None);
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
}

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;
