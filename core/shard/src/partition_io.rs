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

//! Captured file work retains its slot and resource leases until owner acceptance.
//! Interrupted jobs keep those resources fenced, including after a late completion.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use consensus::PartitionsHandle;
use futures::future::poll_fn;
use journal::local_gate::OwnedLocalGateGuard;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{
    CapturedPartitionIo, PartitionIncarnation, PartitionIoIdentity, PartitionIoResources,
    PartitionIoResult,
};
use prometheus_client::metrics::counter::Counter;
use server_common::sharding::IggyNamespace;
pub use server_common::sharding::PARTITION_IO_CAPACITY_MAX;
use thiserror::Error;

use crate::IggyShard;

pub const DEFAULT_PARTITION_IO_CAPACITY: usize = 16;
const CONTINUATIONS_PER_RETRY: usize = 16;

/// Per-shard limits validated to admit the largest indivisible legal file job.
#[derive(Clone, Copy, Debug)]
pub struct PartitionIoLimits {
    capacity: usize,
    bytes_max: usize,
}

/// A capacity or allocation ceiling that cannot safely service partition file jobs.
#[derive(Debug, Error)]
pub enum PartitionIoLimitsError {
    #[error("sharding.partition_io_capacity must be in 1..={PARTITION_IO_CAPACITY_MAX}; got {0}")]
    Capacity(usize),
    #[error("partition I/O allocation charge exceeds addressable memory")]
    Overflow,
    #[error(
        "sharding.partition_io_bytes_max must be at least {minimum} and fit addressable memory; got {value}"
    )]
    Bytes { value: usize, minimum: usize },
}

impl PartitionIoLimits {
    /// Resolve omitted bytes using the same allocation calculation as dispatch.
    ///
    /// # Errors
    /// Rejects invalid slot counts, arithmetic overflow and undersized byte limits.
    pub fn new(capacity: usize, bytes_max: Option<usize>) -> Result<Self, PartitionIoLimitsError> {
        if capacity == 0 || capacity > PARTITION_IO_CAPACITY_MAX {
            return Err(PartitionIoLimitsError::Capacity(capacity));
        }
        let minimum = partitions::largest_legal_job_charge()
            .filter(|charge| isize::try_from(*charge).is_ok())
            .ok_or(PartitionIoLimitsError::Overflow)?;
        let bytes_max = bytes_max.unwrap_or(minimum);
        if bytes_max < minimum || bytes_max > isize::MAX as usize {
            return Err(PartitionIoLimitsError::Bytes {
                value: bytes_max,
                minimum,
            });
        }
        Ok(Self {
            capacity,
            bytes_max,
        })
    }

    #[must_use]
    pub const fn bytes_max(self) -> usize {
        self.bytes_max
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionIoToken {
    slot: usize,
    identity: PartitionIoIdentity,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Reserved,
    Running,
    Queued,
    Settled,
    Interrupted,
}

struct PartitionIoSlot<SB> {
    namespace: IggyNamespace,
    incarnation: PartitionIncarnation,
    identity: Cell<Option<PartitionIoIdentity>>,
    state: Cell<SlotState>,
    elapsed: Cell<Duration>,
    charge: usize,
    result: RefCell<Option<PartitionIoResult>>,
    resources: RefCell<Option<PartitionIoResources<SB>>>,
    gate: RefCell<Option<OwnedLocalGateGuard>>,
    quiescence: RefCell<Option<Rc<partitions::PartitionIoQuiescence>>>,
}

#[derive(Default)]
struct ReadyPartitions {
    queue: RefCell<VecDeque<(IggyNamespace, PartitionIncarnation)>>,
    retries: RefCell<VecDeque<(IggyNamespace, PartitionIncarnation)>>,
    retry_present: RefCell<BTreeSet<(IggyNamespace, PartitionIncarnation)>>,
    continuations: Cell<usize>,
    waiting: RefCell<VecDeque<(IggyNamespace, PartitionIncarnation)>>,
    // Only the oldest capacity waiter may also be in the runnable queue.
    waiting_ready: Cell<bool>,
    completions: RefCell<VecDeque<PartitionIoToken>>,
    present: RefCell<BTreeSet<(IggyNamespace, PartitionIncarnation)>>,
    waker: RefCell<Option<Waker>>,
}

#[derive(Default)]
struct IoCounters {
    active: Cell<usize>,
    queued: Cell<usize>,
    quarantined: Cell<usize>,
    fenced: Counter,
    timeouts: Counter,
}

impl<SB> PartitionIoSlot<SB> {
    fn interrupt(&self, interrupted: &Cell<bool>, counters: &IoCounters) -> bool {
        if self.state.get() != SlotState::Running {
            return false;
        }
        self.state.set(SlotState::Interrupted);
        interrupted.set(true);
        if let Some(quiescence) = self.quiescence.borrow().as_ref() {
            quiescence.interrupt();
        }
        counters.active.set(counters.active.get() - 1);
        counters.quarantined.set(counters.quarantined.get() + 1);
        counters.fenced.inc();
        true
    }
}

impl ReadyPartitions {
    fn notify(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        if !self.present.borrow_mut().insert((namespace, incarnation))
            && !self
                .retry_present
                .borrow_mut()
                .remove(&(namespace, incarnation))
        {
            return;
        }
        self.queue.borrow_mut().push_back((namespace, incarnation));
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Slots outlive mounted lookup. Tokens never carry local file handles or results.
pub struct PartitionIoLane<SB> {
    pub(crate) limits: PartitionIoLimits,
    slots: RefCell<Vec<Option<Rc<PartitionIoSlot<SB>>>>>,
    charged: Cell<usize>,
    ready: Rc<ReadyPartitions>,
    interrupted: Rc<Cell<bool>>,
    closed: Cell<bool>,
    timeout: Cell<Duration>,
    counters: Rc<IoCounters>,
    #[cfg(test)]
    execution_gate: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl<SB: SuperblockStore> PartitionIoLane<SB> {
    pub(crate) fn new(limits: PartitionIoLimits, metrics: &crate::metrics::ShardMetrics) -> Self {
        metrics.set_partition_io_limits(limits.capacity, limits.bytes_max);
        Self {
            limits,
            slots: RefCell::new((0..limits.capacity).map(|_| None).collect()),
            charged: Cell::new(0),
            ready: Rc::new(ReadyPartitions {
                completions: RefCell::new(VecDeque::with_capacity(limits.capacity)),
                ..ReadyPartitions::default()
            }),
            interrupted: Rc::new(Cell::new(false)),
            closed: Cell::new(false),
            timeout: Cell::new(Duration::ZERO),
            counters: Rc::new(IoCounters {
                fenced: metrics.partition_io_fenced_counter(),
                timeouts: metrics.partition_io_timeouts_counter(),
                ..IoCounters::default()
            }),
            #[cfg(test)]
            execution_gate: RefCell::new(None),
        }
    }

    pub(crate) fn notifier(&self) -> partitions::PartitionIoNotifier {
        let ready = Rc::clone(&self.ready);
        Rc::new(move |namespace, incarnation| ready.notify(namespace, incarnation))
    }

    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut current = self.ready.waker.borrow_mut();
        if current
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.queue.borrow().is_empty()
            || !self.ready.retry_present.borrow().is_empty()
            || !self.ready.completions.borrow().is_empty()
            || self.interrupted.get()
    }

    pub(crate) fn head(&self) -> Option<(IggyNamespace, PartitionIncarnation)> {
        if self.ready.queue.borrow().is_empty()
            || self.ready.continuations.get() >= CONTINUATIONS_PER_RETRY
        {
            while let Some(owner) = self.ready.retries.borrow_mut().pop_front() {
                if self.ready.retry_present.borrow_mut().remove(&owner) {
                    self.ready.queue.borrow_mut().push_front(owner);
                    self.ready.continuations.set(0);
                    break;
                }
            }
        }
        self.ready.queue.borrow().front().copied()
    }

    pub(crate) fn retry(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        let owner = (namespace, incarnation);
        if self.ready.present.borrow_mut().insert(owner) {
            self.ready.retry_present.borrow_mut().insert(owner);
            self.ready.retries.borrow_mut().push_back(owner);
        }
    }

    pub(crate) fn pop_ready(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        let mut queue = self.ready.queue.borrow_mut();
        if queue.front() == Some(&(namespace, incarnation)) {
            queue.pop_front();
        } else {
            queue.retain(|queued| *queued != (namespace, incarnation));
        }
        self.ready
            .present
            .borrow_mut()
            .remove(&(namespace, incarnation));
        self.ready
            .retry_present
            .borrow_mut()
            .remove(&(namespace, incarnation));
        self.ready
            .continuations
            .set(self.ready.continuations.get().saturating_add(1));
        drop(queue);
        let mut waiting = self.ready.waiting.borrow_mut();
        if waiting.front() == Some(&(namespace, incarnation)) {
            waiting.pop_front();
            self.ready.waiting_ready.set(false);
            drop(waiting);
            self.wake_capacity_waiter();
        }
    }

    fn wait_for_capacity(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        let owner = (namespace, incarnation);
        debug_assert_eq!(self.ready.queue.borrow().front(), Some(&owner));
        self.ready.queue.borrow_mut().pop_front();
        self.ready
            .continuations
            .set(self.ready.continuations.get().saturating_add(1));
        let mut waiting = self.ready.waiting.borrow_mut();
        if waiting.front() == Some(&owner) {
            self.ready.waiting_ready.set(false);
        } else {
            waiting.push_back(owner);
        }
    }

    fn wake_capacity_waiter(&self) {
        if self.ready.waiting_ready.get() {
            return;
        }
        if let Some(owner) = self.ready.waiting.borrow().front().copied() {
            let mut queue = self.ready.queue.borrow_mut();
            // Release can run while its caller still owns the queue head.
            let position = usize::from(!queue.is_empty());
            queue.insert(position, owner);
            self.ready.waiting_ready.set(true);
        }
    }

    pub(crate) fn reschedule(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        self.ready.notify(namespace, incarnation);
    }

    fn continue_ready(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        let owner = (namespace, incarnation);
        if self.ready.waiting_ready.get() && self.ready.waiting.borrow().front() == Some(&owner) {
            let mut queue = self.ready.queue.borrow_mut();
            let previous = queue.pop_front();
            debug_assert_eq!(previous, Some(owner));
            queue.push_back(owner);
            self.ready
                .continuations
                .set(self.ready.continuations.get().saturating_add(1));
        } else {
            self.pop_ready(namespace, incarnation);
            self.reschedule(namespace, incarnation);
        }
    }

    pub(crate) fn try_reserve(
        &self,
        namespace: IggyNamespace,
        incarnation: PartitionIncarnation,
        charge: usize,
    ) -> Option<usize> {
        if self.closed.get() {
            return None;
        }
        let mut slots = self.slots.borrow_mut();
        if let Some((index, existing)) = slots.iter().enumerate().find_map(|(index, slot)| {
            slot.as_ref()
                .filter(|slot| slot.namespace == namespace && slot.incarnation == incarnation)
                .map(|slot| (index, slot))
        }) {
            return (existing.state.get() == SlotState::Settled && charge <= existing.charge)
                .then_some(index);
        }
        if self
            .ready
            .waiting
            .borrow()
            .front()
            .is_some_and(|owner| *owner != (namespace, incarnation))
        {
            return None;
        }
        let charged = self.charged.get().checked_add(charge)?;
        if charged > self.limits.bytes_max {
            return None;
        }
        let index = slots.iter().position(Option::is_none)?;
        slots[index] = Some(Rc::new(PartitionIoSlot {
            namespace,
            incarnation,
            identity: Cell::new(None),
            state: Cell::new(SlotState::Reserved),
            elapsed: Cell::new(Duration::ZERO),
            charge,
            result: RefCell::new(None),
            resources: RefCell::new(None),
            gate: RefCell::new(None),
            quiescence: RefCell::new(None),
        }));
        self.charged.set(charged);
        Some(index)
    }

    pub(crate) fn dispatch(
        &self,
        index: usize,
        captured: CapturedPartitionIo<SB>,
        bus: &impl MessageBus,
    ) where
        SB: 'static,
    {
        let slot = Rc::clone(
            self.slots.borrow()[index]
                .as_ref()
                .expect("reserved partition I/O slot"),
        );
        let CapturedPartitionIo {
            identity,
            job,
            gate,
            quiescence,
        } = captured;
        slot.identity.set(Some(identity));
        *slot.resources.borrow_mut() = Some(job.retain_resources());
        *slot.gate.borrow_mut() = gate;
        *slot.quiescence.borrow_mut() = Some(Rc::clone(&quiescence));
        slot.state.set(SlotState::Running);
        slot.elapsed.set(Duration::ZERO);
        self.counters.active.set(self.counters.active.get() + 1);
        let marker = InterruptionMarker {
            slot: Rc::clone(&slot),
            interrupted: Rc::clone(&self.interrupted),
            counters: Rc::clone(&self.counters),
        };
        let ready = Rc::clone(&self.ready);
        let counters = Rc::clone(&self.counters);
        #[cfg(test)]
        let execution_gate = self.execution_gate.borrow_mut().take();
        bus.spawn(async move {
            #[cfg(test)]
            if let Some(execution_gate) = execution_gate {
                execution_gate
                    .await
                    .expect("test releases captured file job");
            }
            let result = job.execute().await;
            if slot.state.get() == SlotState::Interrupted {
                return;
            }
            *slot.result.borrow_mut() = Some(result);
            slot.state.set(SlotState::Queued);
            counters.active.set(counters.active.get() - 1);
            counters.queued.set(counters.queued.get() + 1);
            ready.completions.borrow_mut().push_back(PartitionIoToken {
                slot: index,
                identity,
            });
            if let Some(waker) = ready.waker.borrow().as_ref() {
                waker.wake_by_ref();
            }
            drop(marker);
        });
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn recv(&self) -> PartitionIoToken {
        poll_fn(|context| {
            self.register_waker(context.waker());
            self.try_recv().map_or(Poll::Pending, Poll::Ready)
        })
        .await
    }

    pub(crate) fn try_recv(&self) -> Option<PartitionIoToken> {
        self.ready.completions.borrow_mut().pop_front()
    }

    pub(crate) fn take_result(&self, token: PartitionIoToken) -> Option<PartitionIoResult> {
        let slots = self.slots.borrow();
        let slot = slots.get(token.slot)?.as_ref()?;
        if slot.identity.get() != Some(token.identity) || slot.state.get() != SlotState::Queued {
            return None;
        }
        let result = slot.result.borrow_mut().take()?;
        slot.state.set(SlotState::Settled);
        self.counters.queued.set(self.counters.queued.get() - 1);
        Some(result)
    }

    pub(crate) fn settle(&self, token: PartitionIoToken, retain: bool) {
        let slot = self.slots.borrow()[token.slot].clone();
        let Some(slot) = slot.filter(|slot| {
            slot.identity.get() == Some(token.identity) && slot.state.get() == SlotState::Settled
        }) else {
            return;
        };
        if let Some(guard) = slot.gate.borrow_mut().take() {
            guard.release();
        }
        slot.resources.borrow_mut().take();
        if let Some(quiescence) = slot.quiescence.borrow_mut().take() {
            quiescence.settle(token.identity);
        }
        if !retain {
            self.release(token.slot);
        }
    }

    pub(crate) fn release(&self, index: usize) {
        let mut slots = self.slots.borrow_mut();
        if slots[index].as_ref().is_some_and(|slot| {
            matches!(slot.state.get(), SlotState::Reserved | SlotState::Settled)
        }) {
            let slot = slots[index].take().expect("settled slot exists");
            self.charged.set(self.charged.get() - slot.charge);
            self.wake_capacity_waiter();
        }
    }

    pub(crate) fn retained(
        &self,
        namespace: IggyNamespace,
        incarnation: PartitionIncarnation,
    ) -> Option<PartitionIoToken> {
        self.slots
            .borrow()
            .iter()
            .enumerate()
            .find_map(|(index, slot)| {
                let slot = slot.as_ref()?;
                (slot.namespace == namespace
                    && slot.incarnation == incarnation
                    && slot.state.get() == SlotState::Settled)
                    .then(|| {
                        slot.identity.get().map(|identity| PartitionIoToken {
                            slot: index,
                            identity,
                        })
                    })
                    .flatten()
            })
    }

    pub(crate) fn set_timeout(&self, timeout: Duration) {
        self.timeout.set(timeout);
    }

    pub(crate) fn tick(&self) {
        let timeout = self.timeout.get();
        if timeout.is_zero() {
            return;
        }
        // Count injected pump ticks, not wall time, so tests drive the deadline
        // deterministically. The simulator never sets it, so it stays disabled there.
        // A timed-out writer keeps its lease and allocation charge until shutdown.
        for slot in self.slots.borrow().iter().flatten() {
            if slot.state.get() != SlotState::Running {
                continue;
            }
            let elapsed = slot
                .elapsed
                .get()
                .saturating_add(crate::CONSENSUS_TICK_INTERVAL);
            slot.elapsed.set(elapsed);
            if elapsed >= timeout {
                tracing::error!(
                    namespace_raw = slot.namespace.inner(),
                    "partition file job timed out; fencing its owner"
                );
                if slot.interrupt(&self.interrupted, &self.counters) {
                    self.counters.timeouts.inc();
                }
            }
        }
    }

    pub(crate) fn interrupted(&self) -> Vec<PartitionIoIdentity> {
        if !self.interrupted.replace(false) {
            return Vec::new();
        }
        self.slots
            .borrow()
            .iter()
            .flatten()
            .filter(|slot| slot.state.get() == SlotState::Interrupted)
            .filter_map(|slot| slot.identity.get())
            .collect()
    }

    pub(crate) fn outstanding(&self) -> usize {
        self.slots
            .borrow()
            .iter()
            .flatten()
            .filter(|slot| slot.state.get() != SlotState::Interrupted)
            .count()
    }

    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.ready.waker.borrow_mut().take();
    }

    pub(crate) fn record_metrics(&self, metrics: &crate::metrics::ShardMetrics) {
        metrics.set_partition_io(
            self.counters.active.get(),
            self.counters.queued.get(),
            self.charged.get(),
            self.ready.present.borrow().len(),
            self.counters.quarantined.get(),
        );
    }
}

impl<B: MessageBus + 'static, MJ, S, M, T, SB: SuperblockStore + 'static>
    IggyShard<B, MJ, S, M, T, SB>
where
    MJ: crate::JournalHandle,
    MJ::Target: journal::Journal<
            Entry = server_common::Message<iggy_binary_protocol::PrepareHeader>,
            Header = iggy_binary_protocol::PrepareHeader,
        >,
    M: crate::RestorableMetadataStm,
    T: crate::ShardsTable,
{
    pub(crate) fn accept_partition_io_completion(&self, token: PartitionIoToken) {
        let Some(result) = self.partition_io.take_result(token) else {
            return;
        };
        let retained = if let Some(partition) = self
            .plane
            .partitions()
            .get_io_owner(&token.identity.namespace)
            .filter(|partition| partition.incarnation() == token.identity.incarnation)
        {
            if let Err(error) = partition.accept_io(token.identity, result) {
                tracing::error!(namespace_raw = token.identity.namespace.inner(), %error, "partition I/O acceptance failed");
            }
            if token.identity.continuation == partitions::PartitionIoContinuation::Retention {
                self.drop_partition_transfer_state(token.identity.namespace, partition);
            }
            partition.retains_io_reservation(token.identity)
        } else {
            drop(result);
            false
        };
        self.partition_io.settle(token, retained);
    }

    /// Bounded completion and continuation service after each ordinary pump event.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn service_partition_io(&self) {
        if let Some(token) = self.partition_io.try_recv() {
            self.accept_partition_io_completion(token);
            self.cooperate().await;
        }
        for identity in self.partition_io.interrupted() {
            if let Some(partition) = self.plane.partitions().get_io_owner(&identity.namespace) {
                partition.interrupt_io(identity);
            }
            self.cooperate().await;
        }
        let Some((namespace, incarnation)) = self.partition_io.head() else {
            return;
        };
        let partitions = self.plane.partitions();
        let Some(partition) = partitions
            .get_io_owner(&namespace)
            .filter(|partition| partition.incarnation() == incarnation)
        else {
            self.partition_io.pop_ready(namespace, incarnation);
            if let Some(token) = self.partition_io.retained(namespace, incarnation) {
                self.partition_io.release(token.slot);
            }
            return;
        };
        let step = partition.resume_io(partitions.config()).await;
        if let Some(token) = self.partition_io.retained(namespace, incarnation)
            && !partition.retains_io_reservation(token.identity)
        {
            self.partition_io.release(token.slot);
        }
        match step {
            partitions::PartitionIoStep::WireActions(actions) => {
                crate::dispatch_partition_wire_actions::<B, _, MJ, _>(
                    partition.consensus(),
                    partition,
                    actions,
                )
                .await;
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::TransferReady => {
                self.partition_io.pop_ready(namespace, incarnation);
                self.on_partition_transfer_progress(namespace.inner()).await;
            }
            partitions::PartitionIoStep::InstallFinished { peer, outcome } => {
                self.partition_io.pop_ready(namespace, incarnation);
                self.drop_partition_transfer_state(namespace, partition);
                self.finish_partition_install(namespace.inner(), peer, outcome)
                    .await;
                self.partition_io.reschedule(namespace, incarnation);
            }
            partitions::PartitionIoStep::QuarantineFinished(outcome) => {
                self.partition_io.pop_ready(namespace, incarnation);
                match outcome {
                    Ok(directory) => {
                        tracing::warn!(
                            namespace_raw = namespace.inner(),
                            ?directory,
                            "fenced partition writers settled and quarantine completed"
                        );
                        self.enqueue_reconcile_op(crate::ReconcileOp::ConfirmRemove { namespace });
                        self.signal_reconcile_wake();
                    }
                    Err(error) => {
                        tracing::error!(namespace_raw = namespace.inner(), %error, "quarantine failed; retaining tombstone and files");
                    }
                }
            }
            partitions::PartitionIoStep::ViewApplied { actions, peer } => {
                let pending = partition.consensus().pending_view_log();
                crate::reconcile_partition_view_divergence(self.id, partition, pending.as_ref())
                    .await;
                if let Some(peer) = peer {
                    if partition.consensus().view_log_unproven() {
                        self.maybe_request_partition_repair(partition, peer).await;
                    } else {
                        self.finish_partition_view_adoption(namespace, peer, actions)
                            .await;
                    }
                } else {
                    self.advance_pending_partition_view(namespace).await;
                }
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Transition(message) => {
                self.on_message(message).await;
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::PrimaryTransfer(request) => {
                self.apply_primary_transfer(namespace, request).await;
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Progress => {
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Pending => {
                self.partition_io.pop_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Ready(plan) => {
                let Some(index) =
                    self.partition_io
                        .try_reserve(namespace, incarnation, plan.allocation_charge)
                else {
                    self.partition_io.wait_for_capacity(namespace, incarnation);
                    return;
                };
                self.partition_io.pop_ready(namespace, incarnation);
                match partition.capture_io(plan, partitions.config()) {
                    Ok(Some(captured)) => {
                        if matches!(
                            plan.continuation,
                            partitions::PartitionIoContinuation::Retention
                                | partitions::PartitionIoContinuation::Install
                        ) {
                            self.drop_partition_transfer_state(namespace, partition);
                        }
                        self.partition_io.dispatch(index, captured, &self.bus);
                    }
                    Ok(None) => {
                        self.partition_io.release(index);
                        self.partition_io.reschedule(namespace, incarnation);
                    }
                    Err(error) => {
                        self.partition_io.release(index);
                        tracing::error!(namespace_raw = namespace.inner(), %error, "partition I/O capture failed");
                        partition.reject_io_capture(plan.continuation, error);
                    }
                }
            }
        }
        self.cooperate().await;
    }
}

struct InterruptionMarker<SB> {
    slot: Rc<PartitionIoSlot<SB>>,
    interrupted: Rc<Cell<bool>>,
    counters: Rc<IoCounters>,
}

impl<SB> Drop for InterruptionMarker<SB> {
    fn drop(&mut self) {
        self.slot.interrupt(&self.interrupted, &self.counters);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, Ref, RefCell};
    use std::collections::BTreeMap;
    use std::io;
    use std::ops::RangeInclusive;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    use std::time::Duration;

    use consensus::{
        ArtifactProgress, Consensus, DvcSuffix, LocalPipeline, PartitionsHandle, Pipeline, Plane,
        Sequencer, StateArtifact, StateTransferStage, TimeoutManager, VsrAction, VsrConsensus,
        artifact_kind,
    };
    use futures::FutureExt;
    use futures::channel::oneshot;
    use iggy_binary_protocol::namespace::METADATA_GROUP;
    use iggy_binary_protocol::primitives::consumer::WireConsumer;
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::users::CreateUserRequest;
    use iggy_binary_protocol::requests::users::login_register::RegisterRequest;
    use iggy_binary_protocol::{
        AckLevel, Command, CommitHeader, ConsensusHeader, DoViewChangeHeader, GenericHeader,
        Operation, PrepareHeader, PrepareOkHeader, RepairRangeReplyHeader, ReplyHeader,
        RequestPreparesHeader, RequestStartViewHeader, RequestStateChunkHeader,
        RequestStateTransferHeader, RoutedRequestHeader, StartViewChangeHeader, StartViewHeader,
        StateChunkHeader, StateTransferTargetHeader, WireEncode, WireIdentifier, WireName,
        WireOptions,
    };
    use iggy_common::{
        ConsumerGroupOffsets, ConsumerKind, ConsumerOffsets, Durability, IggyByteSize,
        IggyTimestamp, PartitionStats, TopicRuntimeOptions, variadic,
    };
    use journal::Journal;
    use journal::prepare_journal::PrepareJournal;
    use journal::superblock::{SuperblockContents, SuperblockStore};
    use message_bus::{IggyMessageBus, MessageBus};
    use metadata::stm::stream::{Partition, Stream, Streams, StreamsInner, Topic};
    use metadata::stm::user::{Users, UsersInner};
    use metadata::{IggyMetadata, MuxStateMachine};
    use partitions::state_transfer::{
        PartitionArtifactSource, PartitionTransferSession, TransferArtifact,
    };
    use partitions::{
        IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionPathLayout,
        PartitionsConfig, REPAIR_MAX_STALL_RETRIES, RepairConclusion, RepairSession, Segment,
    };
    use prometheus_client::registry::Registry;
    use server_common::iobuf::Owned;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
    use server_common::{Message, MessageBag, SegmentStorage};

    use crate::host::NoopHost;
    use crate::metrics::ShardMetrics;
    use crate::shards_table::{PapayaShardsTable, ShardsTable};
    use crate::{
        IggyShard, LifecycleFrame, PARTITION_REPAIRS_INFLIGHT_MAX, PartitionConsensusConfig,
        REPAIR_GAP_DEBOUNCE_TICKS_MIN, Receiver, ReconcileOp, ReplicaTopology, ShardFrame,
        ShardIdentity, TaggedSender, channel, shard_channel,
    };

    const SEGMENT_BYTES: u64 = 1024 * 1024;
    const TEST_PARTITIONS: usize = 3;

    #[compio::test]
    async fn given_a_partition_incarnation_when_foreign_view_frames_arrive_should_preserve_consensus()
     {
        const INCARNATION: u64 = 7;
        const NEXT_COMMIT: u64 = 14;
        let namespace = IggyNamespace::new(0, 0, 0);
        for command in [
            Command::Commit,
            Command::StartViewChange,
            Command::DoViewChange,
            Command::StartView,
        ] {
            for incoming in [0, INCARNATION - 1, INCARNATION + 1, INCARNATION] {
                let bus = Rc::new(IggyMessageBus::new(0));
                let (owner, _sender) = test_owner(&bus, None);
                let partitions = owner.plane.partitions();
                partitions.remove(&namespace).unwrap();
                let mut partition =
                    repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
                partition.set_created_revision(INCARNATION);
                partition.repair = None;
                partitions.insert(namespace, partition);
                let message = if command == Command::Commit {
                    MessageBag::Commit(
                        Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
                            |_, header: &mut CommitHeader| {
                                header.command = Command::Commit;
                                header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
                                header.cluster = 1;
                                header.group = namespace.inner();
                                header.replica = REPAIR_PRIMARY;
                                header.view = REPAIR_VIEW;
                                header.commit = NEXT_COMMIT;
                                header.seal();
                            },
                        ),
                    )
                } else {
                    view_control_message(namespace, command, REPAIR_VIEW + 3, REPAIR_PRIMARY)
                };
                let message = match message {
                    MessageBag::Commit(message) => MessageBag::Commit(message.transmute_header(
                        |old, header: &mut CommitHeader| {
                            *header = old;
                            header.partition_incarnation = incoming;
                            header.seal();
                        },
                    )),
                    MessageBag::StartViewChange(message) => MessageBag::StartViewChange(
                        message.transmute_header(|old, header: &mut StartViewChangeHeader| {
                            *header = old;
                            header.partition_incarnation = incoming;
                            header.seal();
                        }),
                    ),
                    MessageBag::DoViewChange(message) => MessageBag::DoViewChange(
                        message.transmute_header(|old, header: &mut DoViewChangeHeader| {
                            *header = old;
                            header.partition_incarnation = incoming;
                            header.seal();
                        }),
                    ),
                    MessageBag::StartView(message) => MessageBag::StartView(
                        message.transmute_header(|old, header: &mut StartViewHeader| {
                            *header = old;
                            header.partition_incarnation = incoming;
                            header.seal();
                        }),
                    ),
                    _ => unreachable!(),
                };
                owner.on_message(message).await;
                let consensus = partitions.get_by_ns(&namespace).unwrap().consensus();
                let drops = owner.metrics.partition_incarnation_mismatch_value(command);
                if incoming == INCARNATION {
                    assert!(
                        consensus.view() > REPAIR_VIEW || consensus.commit_max() == NEXT_COMMIT,
                        "matching {command:?} did not reach consensus"
                    );
                    assert_eq!(drops, 0, "matching {command:?} counted as a drop");
                } else {
                    assert_eq!(drops, 1, "foreign {command:?} not counted once");
                    assert_eq!(
                        consensus.view(),
                        REPAIR_VIEW,
                        "foreign {command:?} changed the view"
                    );
                    assert_eq!(
                        consensus.commit_max(),
                        0,
                        "foreign {command:?} advanced commit_max"
                    );
                    assert!(consensus.is_normal(), "foreign {command:?} changed status");
                }
            }
        }
    }

    #[compio::test]
    async fn given_control_actions_when_sent_should_stamp_the_consensus_incarnation() {
        const INCARNATION: u64 = 7;
        const PEER: u8 = 1;
        for incarnation in [0, INCARNATION] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let sent = Rc::new(RefCell::new(Vec::new()));
            let captured = Rc::clone(&sent);
            bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
                captured.borrow_mut().push(frame);
                Ok(())
            }));
            assert!(bus.owner_table().try_claim(PEER, 1));
            let group = if incarnation == 0 {
                METADATA_GROUP
            } else {
                IggyNamespace::new(0, 0, 0).inner()
            };
            let consensus =
                VsrConsensus::new(1, 0, 2, group, Rc::clone(&bus), LocalPipeline::new());
            consensus.set_partition_incarnation(incarnation);
            consensus.init();
            let group = consensus.group();
            let view = consensus.view();
            let actions = [
                VsrAction::SendStartViewChange { view, group },
                VsrAction::SendDoViewChange {
                    view,
                    target: PEER,
                    log_view: view,
                    op: 0,
                    commit: 0,
                    group,
                    suffix: DvcSuffix::empty(),
                    untrusted_log: false,
                },
                VsrAction::SendRequestStartView { view, group },
                VsrAction::SendStartView {
                    view,
                    op: 0,
                    commit: 0,
                    incarnation: 0,
                    target: Some(PEER),
                    group,
                    suffix: Vec::new(),
                },
                VsrAction::SendCommit {
                    view,
                    commit: 0,
                    group,
                    timestamp_monotonic: 1,
                    target: None,
                },
            ];
            crate::dispatch_vsr_actions::<_, _, PrepareJournal>(&consensus, None, &actions).await;
            assert_eq!(sent.borrow().len(), actions.len());
            let (owner, _sender) = test_owner(&bus, None);
            owner.send_request_state_transfer(&consensus, PEER, 1).await;
            owner
                .send_request_prepares(
                    &consensus,
                    PEER,
                    1,
                    1,
                    1,
                    group,
                    consensus.partition_incarnation(),
                )
                .await;
            owner
                .send_request_state_chunk(
                    consensus.cluster(),
                    consensus.replica(),
                    PEER,
                    1,
                    group,
                    consensus.partition_incarnation(),
                    0,
                    0,
                    1,
                )
                .await;
            assert_eq!(sent.borrow().len(), actions.len() + 3);
            for frame in sent.borrow().iter() {
                assert_eq!(control_frame_incarnation(frame.as_slice()), incarnation);
            }
        }
    }

    #[compio::test]
    async fn given_a_partition_transfer_offer_when_serving_a_chunk_should_stamp_the_hosted_incarnation()
     {
        const INCARNATION: u64 = 7;
        const REQUESTER: u8 = 1;
        const NONCE: u128 = 1;
        const CHUNK_LEN: u32 = 1;
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(REQUESTER, 1));
        let (mut owner, _sender) = test_owner(&bus, None);
        owner.partition_consensus.replica_count = 2;
        let namespace = IggyNamespace::new(0, 0, 0);
        let directory = tempfile::tempdir().unwrap();
        let partitions = owner.plane.partitions();
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        partition.set_created_revision(INCARNATION);
        attach_segment_files(partition, directory.path(), false).await;
        let request =
            send_request(namespace, 1).transmute_header(|old, header: &mut RoutedRequestHeader| {
                *header = old;
                header.partition_incarnation = INCARNATION;
            });
        let (reply, replied) = consensus::oneshot_channel();
        partitions.on_request_with_reply(request, Some(reply)).await;
        owner
            .process_loopback(&mut crate::router::LoopbackRound::default())
            .await;
        assert_eq!(replied.await.unwrap().header().status, 0);
        let request =
            Message::<RequestStateTransferHeader>::new(size_of::<RequestStateTransferHeader>())
                .transmute_header(|_, header: &mut RequestStateTransferHeader| {
                    header.command = Command::RequestStateTransfer;
                    header.size = u32::try_from(size_of::<RequestStateTransferHeader>()).unwrap();
                    header.cluster = 1;
                    header.group = namespace.inner();
                    header.replica = REQUESTER;
                    header.partition_incarnation = INCARNATION;
                    header.nonce = NONCE;
                    header.seal();
                });
        owner
            .on_message(MessageBag::RequestStateTransfer(request))
            .await;
        let artifact = {
            let offers = owner.state_transfer_offers.borrow();
            let crate::ServedOffer::Partition(offer) =
                &offers[&(namespace.inner(), REQUESTER)].offer
            else {
                panic!("the partition must serve its committed offer");
            };
            u32::try_from(offer.artifact_count() - 1).unwrap()
        };
        sent.borrow_mut().clear();
        let request = Message::<RequestStateChunkHeader>::new(size_of::<RequestStateChunkHeader>())
            .transmute_header(|_, header: &mut RequestStateChunkHeader| {
                header.command = Command::RequestStateChunk;
                header.size = u32::try_from(size_of::<RequestStateChunkHeader>()).unwrap();
                header.cluster = 1;
                header.group = namespace.inner();
                header.replica = REQUESTER;
                header.partition_incarnation = INCARNATION;
                header.nonce = NONCE;
                header.artifact = artifact;
                header.len = CHUNK_LEN;
                header.seal();
            });
        owner
            .on_message(MessageBag::RequestStateChunk(request))
            .await;
        assert_eq!(sent.borrow().len(), 1);
        let frame = &sent.borrow()[0];
        let message =
            Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice())).unwrap();
        assert_eq!(message.header().command, Command::StateChunk);
        assert_eq!(control_frame_incarnation(frame.as_slice()), INCARNATION);
    }

    #[compio::test]
    async fn given_a_partition_incarnation_when_foreign_repair_or_transfer_requests_arrive_should_not_reply()
     {
        const INCARNATION: u64 = 7;
        const REQUESTER: u8 = 0;
        const NONCE: u128 = 1;
        let namespace = IggyNamespace::new(0, 0, 0);
        for command in [
            Command::RequestStartView,
            Command::RequestPrepares,
            Command::RequestStateTransfer,
            Command::RequestStateChunk,
        ] {
            for incoming in [0, INCARNATION - 1, INCARNATION + 1, INCARNATION] {
                let bus = Rc::new(IggyMessageBus::new(0));
                let sent = Rc::new(RefCell::new(Vec::new()));
                let captured = Rc::clone(&sent);
                bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
                    captured.borrow_mut().push(frame);
                    Ok(())
                }));
                assert!(bus.owner_table().try_claim(REQUESTER, 1));
                let (mut owner, _sender) = test_owner(&bus, None);
                owner.partition_consensus.replica_count = 3;
                let partitions = owner.plane.partitions();
                partitions.remove(&namespace).unwrap();
                let mut partition =
                    repairing_replica(&bus, namespace, (REPAIR_PRIMARY, 3), REQUESTER, 0, 0);
                partition.set_created_revision(INCARNATION);
                partition.repair = None;
                partitions.insert(namespace, partition);
                macro_rules! request {
                    ($header:ty, $variant:ident $(, $field:ident = $value:expr)*) => {
                        MessageBag::$variant(Message::<$header>::new(size_of::<$header>()).transmute_header(|_, header: &mut $header| {
                            header.command = command;
                            header.size = u32::try_from(size_of::<$header>()).unwrap();
                            header.cluster = 1;
                            header.group = namespace.inner();
                            header.replica = REQUESTER;
                            header.partition_incarnation = incoming;
                            $(header.$field = $value;)*
                            header.seal();
                        }))
                    };
                }
                let request = match command {
                    Command::RequestStartView => {
                        request!(RequestStartViewHeader, RequestStartView, view = REPAIR_VIEW)
                    }
                    Command::RequestPrepares => request!(
                        RequestPreparesHeader,
                        RequestPrepares,
                        nonce = NONCE,
                        from_op = 1,
                        to_op = 1
                    ),
                    Command::RequestStateTransfer => request!(
                        RequestStateTransferHeader,
                        RequestStateTransfer,
                        nonce = NONCE
                    ),
                    Command::RequestStateChunk => request!(
                        RequestStateChunkHeader,
                        RequestStateChunk,
                        nonce = NONCE,
                        len = 1
                    ),
                    _ => unreachable!(),
                };
                owner.on_message(request).await;
                let sent = sent.borrow();
                if incoming == INCARNATION {
                    assert!(
                        !sent.is_empty(),
                        "matching {command:?} never reached the handler"
                    );
                    for frame in sent.iter() {
                        assert_eq!(control_frame_incarnation(frame.as_slice()), INCARNATION);
                    }
                } else {
                    assert!(sent.is_empty(), "foreign {command:?} received a reply");
                    assert!(owner.state_transfer_offers.borrow().is_empty());
                    assert!(owner.partition_offer_builds.borrow().is_empty());
                }
            }
        }
    }

    #[compio::test]
    async fn given_live_recovery_when_foreign_replies_arrive_should_preserve_the_session() {
        const INCARNATION: u64 = 7;
        const NONCE: u128 = 1;
        const IDLE_TICKS: u32 = 3;
        const PAYLOAD: &[u8] = b"ab";
        let namespace = IggyNamespace::new(0, 0, 0);
        for command in [
            Command::RepairDone,
            Command::RangeEvicted,
            Command::StateTransferTarget,
            Command::StateChunk,
        ] {
            for incoming in [0, INCARNATION - 1, INCARNATION + 1, INCARNATION] {
                let bus = Rc::new(IggyMessageBus::new(0));
                let sent = Rc::new(RefCell::new(Vec::new()));
                let captured = Rc::clone(&sent);
                bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
                    captured.borrow_mut().push(frame);
                    Ok(())
                }));
                assert!(bus.owner_table().try_claim(REPAIR_PRIMARY, 1));
                let (mut owner, _sender) = test_owner(&bus, None);
                owner.partition_consensus.replica_count = 3;
                let partitions = owner.plane.partitions();
                partitions.remove(&namespace).unwrap();
                let mut partition =
                    repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
                partition.set_created_revision(INCARNATION);
                if matches!(command, Command::StateTransferTarget | Command::StateChunk) {
                    partition.repair = None;
                    partition
                        .consensus()
                        .set_state_transfer_stage(StateTransferStage::AwaitingTarget);
                    if command == Command::StateChunk {
                        partition
                            .consensus()
                            .set_state_transfer_stage(StateTransferStage::Fetching);
                    }
                    partition.transfer = Some(PartitionTransferSession {
                        nonce: NONCE,
                        peer: REPAIR_PRIMARY,
                        commit_op: 0,
                        artifacts: vec![TransferArtifact::Pending(ArtifactProgress {
                            entry: StateArtifact::for_bytes(
                                artifact_kind::CONSUMER_OFFSETS,
                                0,
                                PAYLOAD,
                            ),
                            buf: Vec::new(),
                        })],
                        target_accepted: command == Command::StateChunk,
                        idle_ticks: IDLE_TICKS,
                    });
                }
                partitions.insert(namespace, partition);
                macro_rules! response {
                    ($header:ty, $variant:ident $(, $field:ident = $value:expr)*) => {
                        MessageBag::$variant(Message::<$header>::new(size_of::<$header>()).transmute_header(|_, header: &mut $header| {
                            header.command = command;
                            header.size = u32::try_from(size_of::<$header>()).unwrap();
                            header.cluster = 1;
                            header.group = namespace.inner();
                            header.replica = REPAIR_PRIMARY;
                            header.partition_incarnation = incoming;
                            header.nonce = NONCE;
                            $(header.$field = $value;)*
                            header.seal();
                        }))
                    };
                }
                let response = match command {
                    Command::RepairDone | Command::RangeEvicted => {
                        response!(RepairRangeReplyHeader, RepairRangeReply, op = 2)
                    }
                    Command::StateTransferTarget => response!(
                        StateTransferTargetHeader,
                        StateTransferTarget,
                        unavailable_transient = 1
                    ),
                    Command::StateChunk => {
                        let size = size_of::<StateChunkHeader>() + 1;
                        let mut message = Message::<StateChunkHeader>::new(size);
                        message.as_mut_slice()[size_of::<StateChunkHeader>()] = PAYLOAD[0];
                        MessageBag::StateChunk(message.transmute_header(
                            |_, header: &mut StateChunkHeader| {
                                header.command = command;
                                header.size = u32::try_from(size).unwrap();
                                header.cluster = 1;
                                header.group = namespace.inner();
                                header.replica = REPAIR_PRIMARY;
                                header.partition_incarnation = incoming;
                                header.nonce = NONCE;
                                header.seal();
                            },
                        ))
                    }
                    _ => unreachable!(),
                };
                owner.on_message(response).await;
                let partition = partitions.get_by_ns(&namespace).unwrap();
                if incoming == INCARNATION {
                    match command {
                        Command::RepairDone => assert!(partition.repair.is_none()),
                        Command::RangeEvicted => {
                            assert_eq!(partition.repair.as_ref().unwrap().floor, Some(1));
                        }
                        Command::StateTransferTarget => assert!(partition.transfer.is_none()),
                        Command::StateChunk => assert_eq!(
                            partition.transfer.as_ref().unwrap().artifacts[0]
                                .pending()
                                .unwrap()
                                .buf,
                            &PAYLOAD[..1]
                        ),
                        _ => unreachable!(),
                    }
                    for frame in sent.borrow().iter() {
                        assert_eq!(control_frame_incarnation(frame.as_slice()), INCARNATION);
                    }
                } else {
                    assert!(
                        sent.borrow().is_empty(),
                        "foreign {command:?} sent a follow-up"
                    );
                    if matches!(command, Command::RepairDone | Command::RangeEvicted) {
                        assert_eq!(partition.repair.as_ref().unwrap().floor, None);
                    } else {
                        let transfer = partition.transfer.as_ref().unwrap();
                        assert_eq!(transfer.idle_ticks, IDLE_TICKS);
                        assert_eq!(transfer.artifacts[0].pending().unwrap().buf, [] as [u8; 0]);
                    }
                }
            }
        }
    }

    fn control_frame_incarnation(frame: &[u8]) -> u64 {
        let message = Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame)).unwrap();
        let offset = match message.header().command {
            Command::Commit => std::mem::offset_of!(CommitHeader, partition_incarnation),
            Command::StartViewChange => {
                std::mem::offset_of!(StartViewChangeHeader, partition_incarnation)
            }
            Command::DoViewChange => {
                std::mem::offset_of!(DoViewChangeHeader, partition_incarnation)
            }
            Command::StartView => std::mem::offset_of!(StartViewHeader, partition_incarnation),
            Command::RequestStartView => {
                std::mem::offset_of!(RequestStartViewHeader, partition_incarnation)
            }
            Command::RequestPrepares => {
                std::mem::offset_of!(RequestPreparesHeader, partition_incarnation)
            }
            Command::RepairDone | Command::RangeEvicted => {
                std::mem::offset_of!(RepairRangeReplyHeader, partition_incarnation)
            }
            Command::RequestStateTransfer => {
                std::mem::offset_of!(RequestStateTransferHeader, partition_incarnation)
            }
            Command::StateTransferTarget => {
                std::mem::offset_of!(StateTransferTargetHeader, partition_incarnation)
            }
            Command::RequestStateChunk => {
                std::mem::offset_of!(RequestStateChunkHeader, partition_incarnation)
            }
            Command::StateChunk => std::mem::offset_of!(StateChunkHeader, partition_incarnation),
            command => panic!("unexpected control frame {command:?}"),
        };
        u64::from_le_bytes(frame[offset..offset + size_of::<u64>()].try_into().unwrap())
    }
    /// The view of [`repairing_backup`], whose primary in a three-replica
    /// group is [`REPAIR_PRIMARY`].
    const REPAIR_VIEW: u32 = 1;
    const REPAIR_PRIMARY: u8 = 1;
    const REPAIR_BACKUP: u8 = 2;
    /// Where a spent session rotates: the ring's next replica after the
    /// primary, skipping the backup itself.
    const REPAIR_NEXT_PEER: u8 = 0;
    /// The backup of a two-replica group in [`REPAIR_VIEW`], whose primary is
    /// [`REPAIR_PRIMARY`] too.
    const PAIR_BACKUP: u8 = 0;
    /// A window of committed ops that a backup above a committed prefix
    /// misses, so the walk mark of the first stall round is above zero.
    const LAGGING_COMMIT_MIN: u64 = 5;
    const LAGGING_COMMIT_MAX: u64 = 8;
    const REPLY_DEADLINE: Duration = Duration::from_secs(1);
    const TASK_POLL_INTERVAL: Duration = Duration::from_millis(1);

    #[test]
    fn idle_partitions_do_not_enter_the_io_ready_queue_on_retry() {
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _) = test_owner(&bus, None);
        let partitions = owner.plane.partitions();
        partitions.set_io_notifier(
            owner.partition_io.notifier(),
            owner.partition_io.limits.bytes_max(),
        );
        while let Some((namespace, incarnation)) = owner.partition_io.head() {
            owner.partition_io.pop_ready(namespace, incarnation);
        }
        for namespace in partitions.namespaces() {
            assert!(!partitions.get_io_owner(namespace).unwrap().needs_io_retry());
        }
        assert!(!owner.partition_io.has_ready());
    }

    #[test]
    fn given_ready_notifications_when_pump_is_asleep_should_coalesce_wakes_until_rearmed() {
        let lane = super::PartitionIoLane::<HeldSuperblock>::new(
            super::PartitionIoLimits::new(1, None).unwrap(),
            &ShardMetrics::for_shard(),
        );
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(Arc::clone(&wake_count));
        let incarnation = partitions::PartitionIncarnation::default();
        let namespaces: [_; 3] = std::array::from_fn(|index| IggyNamespace::new(0, 0, index));

        lane.register_waker(&waker);
        lane.reschedule(namespaces[0], incarnation);
        lane.reschedule(namespaces[1], incarnation);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 1);

        for namespace in &namespaces[..2] {
            assert_eq!(lane.head(), Some((*namespace, incarnation)));
            lane.pop_ready(*namespace, incarnation);
        }
        assert!(!lane.has_ready());

        lane.register_waker(&waker);
        lane.reschedule(namespaces[2], incarnation);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
        assert_eq!(lane.head(), Some((namespaces[2], incarnation)));
    }

    #[test]
    fn given_configured_io_lane_when_scraped_before_tick_should_export_limits_and_counters() {
        const CAPACITY: usize = 2;
        let metrics = ShardMetrics::for_shard();
        let limits = super::PartitionIoLimits::new(CAPACITY, None).unwrap();
        let _lane = super::PartitionIoLane::<HeldSuperblock>::new(limits, &metrics);
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry).unwrap();

        for expected in [
            format!("partition_io_capacity {CAPACITY}"),
            format!("partition_io_bytes_max {}", limits.bytes_max()),
            "partition_io_fenced_total 0".to_owned(),
            "partition_io_timeouts_total 0".to_owned(),
        ] {
            assert!(buffer.lines().any(|line| line == expected), "{expected}");
        }
    }

    #[compio::test]
    async fn completed_commit_publishes_while_one_slot_is_held_and_another_job_waits() {
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut owner, _sender) = test_owner(&bus, Some(&store));
        owner.partition_io = super::PartitionIoLane::new(
            super::PartitionIoLimits::new(1, None).unwrap(),
            &owner.metrics,
        );
        let lane = &owner.partition_io;
        let partitions = owner.plane.partitions();
        partitions.set_io_notifier(lane.notifier(), lane.limits.bytes_max());
        let completed = IggyNamespace::new(0, 0, 1);
        let partition = partitions.get_mut_by_ns(&completed).unwrap();
        let dirs = ["consumers", "groups", "external_groups"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
        for dir in &dirs {
            std::fs::create_dir(dir).unwrap();
        }
        partition.configure_consumer_offset_storage(
            dirs,
            ConsumerOffsets::with_capacity(1),
            ConsumerGroupOffsets::with_capacity(1),
        );
        partition.stats.increment_messages_count(1);
        partition.set_runtime_options(TopicRuntimeOptions {
            consumer_offset_durability: Durability::Persisted,
            ..Default::default()
        });
        let completed_incarnation = partition.incarnation();
        let (reply, result) = consensus::oneshot_channel();
        partitions
            .on_request_with_reply(store_request(completed, AckLevel::Quorum), Some(reply))
            .await;
        let partition = partitions.get_mut_by_ns(&completed).unwrap();
        let mut acknowledgments = Vec::new();
        partition
            .consensus()
            .drain_loopback_into(&mut acknowledgments);
        for ack in acknowledgments {
            partition
                .on_ack(ack.try_into_typed().unwrap(), partitions.config())
                .await;
        }
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let mut files_complete = false;
        for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
            let partition = partitions.get_mut_by_ns(&completed).unwrap();
            match partition.resume_io(partitions.config()).await {
                partitions::PartitionIoStep::Ready(plan) => {
                    let slot = lane
                        .try_reserve(completed, completed_incarnation, plan.allocation_charge)
                        .unwrap();
                    let captured = partition
                        .capture_io(plan, partitions.config())
                        .unwrap()
                        .unwrap();
                    let last = matches!(
                        &captured.job,
                        partitions::PartitionIoJob::OffsetDirectories(_)
                    );
                    lane.dispatch(slot, captured, &captured_bus);
                    let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
                    task.await;
                    owner.accept_partition_io_completion(lane.try_recv().unwrap());
                    if last {
                        files_complete = true;
                        break;
                    }
                }
                partitions::PartitionIoStep::Progress => {}
                _ => panic!("offset commit must reach its directory sync"),
            }
        }
        assert!(files_complete);
        assert_eq!(
            partitions
                .get_by_ns(&completed)
                .unwrap()
                .consensus()
                .commit_min(),
            0
        );
        assert_eq!(lane.charged.get(), 0);
        let mut result = std::pin::pin!(result);
        assert!(futures::poll!(&mut result).is_pending());

        let (slot, captured) = capture_first_write(&owner).await;
        lane.dispatch(slot, captured, &captured_bus);
        let waiting = IggyNamespace::new(0, 0, 2);
        let partition = partitions.get_mut_by_ns(&waiting).unwrap();
        partition.set_superblock(Rc::clone(&store), None);
        assert!(!partition.reserve_offsets_through(0).await);
        let waiting_incarnation = partition.incarnation();
        while let Some((namespace, incarnation)) = lane.head() {
            lane.pop_ready(namespace, incarnation);
        }
        lane.reschedule(waiting, waiting_incarnation);
        lane.reschedule(completed, completed_incarnation);
        owner.service_partition_io().await;
        assert_eq!(
            lane.ready.waiting.borrow().front(),
            Some(&(waiting, waiting_incarnation))
        );
        for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
            owner.service_partition_io().await;
            if let std::task::Poll::Ready(reply) = futures::poll!(&mut result) {
                assert_eq!(reply.unwrap().header().status, 0);
                assert_eq!(
                    partitions
                        .get_by_ns(&completed)
                        .unwrap()
                        .consensus()
                        .commit_min(),
                    1
                );
                assert_eq!(
                    lane.counters.active.get(),
                    1,
                    "the unrelated file job must still be held"
                );
                let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
                task.await;
                owner.accept_partition_io_completion(lane.try_recv().unwrap());
                assert!(
                    lane.ready
                        .queue
                        .borrow()
                        .contains(&(waiting, waiting_incarnation)),
                    "the freed slot must wake the waiting file job"
                );
                return;
            }
        }
        panic!("a completed commit was blocked by unrelated file-job capacity");
    }

    #[compio::test]
    async fn given_held_superblock_when_other_partition_receives_send_should_ack_before_release() {
        let (entered, started) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(Some(entered)),
            held: RefCell::new(Some(held)),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, sender) = test_owner(&bus, Some(&store));
        let (stop, stopped) = channel(1);
        let pump = owner.run_message_pump(stopped, Arc::new(AtomicBool::new(false)));
        let exercise = async {
            let first = submit(&sender, IggyNamespace::new(0, 0, 0));
            started
                .await
                .expect("first send reached the held file write");
            let second = submit(&sender, IggyNamespace::new(0, 0, 1));
            let completed = second.recv().fuse();
            let deadline = bus.sleep(REPLY_DEADLINE).fuse();
            futures::pin_mut!(completed, deadline);
            let reply = futures::select_biased! {
                reply = completed => reply.expect("healthy partition reply channel"),
                () = deadline => panic!("partition B could not acknowledge while partition A's superblock write was held"),
            };
            let reply: Message<ReplyHeader> = reply
                .expect("partition B committed")
                .try_into_typed()
                .unwrap();
            assert_eq!(reply.header().status, 0);
            assert!(first.try_recv().is_err(), "held write cannot acknowledge");
            release.send(()).expect("file job still owns its wait");
            let reply: Message<ReplyHeader> = first
                .recv()
                .await
                .unwrap()
                .expect("partition A committed")
                .try_into_typed()
                .unwrap();
            assert_eq!(reply.header().status, 0);
            stop.try_send(()).unwrap();
        };
        let (fault, ()) = futures::join!(pump, exercise);
        assert!(
            fault.is_none(),
            "both partitions drained without a commit fault"
        );
    }

    #[compio::test]
    async fn held_materialization_and_offset_jobs_allow_other_partition_progress_and_shutdown() {
        for durability in [Durability::Replicated, Durability::Persisted] {
            for operation in [Operation::SendMessages, Operation::StoreConsumerOffset] {
                Box::pin(held_file_job_allows_progress(operation, durability)).await;
            }
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn held_file_job_allows_progress(operation: Operation, durability: Durability) {
        let directory = tempfile::tempdir().unwrap();
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, sender) = test_owner(&bus, None);
        let (release, held) = oneshot::channel();
        *owner.partition_io.execution_gate.borrow_mut() = Some(held);
        let namespace = IggyNamespace::new(0, 0, 0);
        let partitions = owner.plane.partitions();
        let late_namespace = IggyNamespace::new(0, 0, TEST_PARTITIONS - 1);
        let late_partition = Box::new(partitions.remove(&late_namespace).unwrap());
        owner.shards_table.remove(&late_namespace);
        partitions.set_io_notifier(
            owner.partition_io.notifier(),
            owner.partition_io.limits.bytes_max(),
        );
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        partition.set_runtime_options(TopicRuntimeOptions {
            messages_required_to_save: Some(1),
            durability,
            consumer_offset_durability: durability,
            ..Default::default()
        });
        let (log_path, index_path) =
            attach_segment_files(partition, directory.path(), durability.is_persisted()).await;
        let request = if operation == Operation::StoreConsumerOffset {
            let dirs = [
                "consumer_offsets",
                "consumer_group_offsets",
                "external_group_offsets",
            ]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
            for dir in &dirs {
                std::fs::create_dir(dir).unwrap();
            }
            partition.configure_consumer_offset_storage(
                dirs,
                ConsumerOffsets::with_capacity(1),
                ConsumerGroupOffsets::with_capacity(1),
            );
            partition.stats.increment_messages_count(1);
            store_request(namespace, AckLevel::NoAck)
        } else {
            send_request(namespace, 1)
        };
        let (reply, first) = consensus::oneshot_channel();
        partitions.on_request_with_reply(request, Some(reply)).await;
        let mut first = std::pin::pin!(first);
        let (stop, stopped) = channel(1);
        let pump = owner.run_message_pump(stopped, Arc::new(AtomicBool::new(false)));
        let exercise = async {
            while owner.partition_io.counters.active.get() == 0 {
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
            assert_eq!(owner.partition_io.counters.active.get(), 1);
            let early = futures::poll!(first.as_mut());
            assert_eq!(
                early.is_ready(),
                durability == Durability::Replicated && operation == Operation::SendMessages,
                "only a Replicated send replies before its held flush: {operation:?}, {durability:?}"
            );
            let second = submit(&sender, IggyNamespace::new(0, 0, 1));
            let second_reply = loop {
                if let Ok(reply) = second.try_recv() {
                    break reply;
                }
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            };
            let reply: Message<ReplyHeader> = second_reply.unwrap().try_into_typed().unwrap();
            assert_eq!(reply.header().status, 0, "{operation:?}, {durability:?}");
            assert_eq!(owner.partition_io.counters.active.get(), 1);
            stop.try_send(()).unwrap();
            compio::time::sleep(TASK_POLL_INTERVAL).await;
            assert_eq!(owner.partition_io.outstanding(), 1);
            assert!(owner.shutting_down.get());
            owner.enqueue_reconcile_op(ReconcileOp::InsertOwned {
                namespace: late_namespace,
                partition: late_partition,
                epoch: 1,
            });
            owner.apply_reconcile_ops();
            assert!(!partitions.contains(&late_namespace));
            assert!(owner.shards_table.shard_for(late_namespace).is_none());
            release.send(()).unwrap();
            loop {
                if partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .shutdown_io_complete()
                {
                    break;
                }
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
            let reply = match early {
                std::task::Poll::Ready(reply) => reply,
                std::task::Poll::Pending => first.await,
            };
            let reply: Message<ReplyHeader> = reply.unwrap().try_into_typed().unwrap();
            assert_eq!(reply.header().status, 0);
        };
        let complete = async { futures::join!(pump, exercise) }.fuse();
        let deadline = compio::time::sleep(REPLY_DEADLINE).fuse();
        futures::pin_mut!(complete, deadline);
        futures::select_biased! {
            (fault, ()) = complete => assert!(fault.is_none(), "{operation:?}, {durability:?}: {fault:?}"),
            () = deadline => panic!("held {operation:?} in {durability:?} blocked healthy progress or shutdown"),
        }
        assert_eq!(owner.partition_io.charged.get(), 0);
        if operation == Operation::SendMessages {
            assert!(std::fs::metadata(log_path).unwrap().len() > 0);
            assert!(std::fs::metadata(index_path).unwrap().len() > 0);
        } else {
            assert!(directory.path().join("consumer_offsets/1").is_file());
        }
    }

    #[compio::test]
    async fn given_queued_loopbacks_when_pump_is_quiet_should_complete_every_round() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let namespace = IggyNamespace::new(0, 0, 1);
        let count = consensus::PIPELINE_PREPARE_QUEUE_MAX + consensus::PIPELINE_REQUEST_QUEUE_MAX;
        let mut results = Vec::with_capacity(count);
        for request in 1..=count {
            let (reply, result) = consensus::oneshot_channel();
            owner
                .plane
                .partitions()
                .on_request_with_reply(
                    send_request(namespace, u64::try_from(request).unwrap()),
                    Some(reply),
                )
                .await;
            results.push(result);
        }
        let (stop, stopped) = channel(1);
        let pump = owner.run_message_pump(stopped, Arc::new(AtomicBool::new(false)));
        let exercise = async {
            let completed = async {
                for result in results {
                    assert_eq!(result.await.unwrap().header().status, 0);
                }
            }
            .fuse();
            let deadline = bus.sleep(REPLY_DEADLINE).fuse();
            futures::pin_mut!(completed, deadline);
            futures::select_biased! {
                () = completed => (),
                () = deadline => panic!("quiet pump stranded a self-ack round"),
            }
            stop.try_send(()).unwrap();
        };
        let (fault, ()) = futures::join!(pump, exercise);
        assert!(fault.is_none());
    }

    #[compio::test]
    async fn loopback_queues_drain_while_backup_acks_commit_between_rounds() {
        const REQUESTS: usize = consensus::PIPELINE_PREPARE_QUEUE_MAX + 1;
        const EVENTS_PER_REQUEST: usize = 3;
        const PARTITIONS: usize =
            REQUESTS * EVENTS_PER_REQUEST * crate::router::COOPERATIVE_EVENT_BUDGET
                / consensus::PIPELINE_PREPARE_QUEUE_MAX
                + TEST_PARTITIONS;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let partitions = owner.plane.partitions();
        for partition_id in TEST_PARTITIONS..PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            let consensus = VsrConsensus::new(
                1,
                0,
                1,
                namespace.inner(),
                Rc::clone(&bus),
                LocalPipeline::new(),
            );
            consensus.init();
            partitions.insert(
                namespace,
                IggyPartition::with_in_memory_storage(
                    Arc::new(PartitionStats::default()),
                    consensus,
                    IggyByteSize::from(SEGMENT_BYTES),
                ),
            );
        }
        for partition_id in 0..PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            for request in 1..=consensus::PIPELINE_PREPARE_QUEUE_MAX {
                partitions
                    .on_request_with_reply(send_request(namespace, request as u64), None)
                    .await;
            }
        }
        let mut round = crate::router::LoopbackRound::default();
        owner.process_loopback(&mut round).await;
        let namespace = IggyNamespace::new(0, 0, 0);
        partitions.remove(&namespace).unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            namespace.inner(),
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.init();
        partitions.insert(
            namespace,
            IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(SEGMENT_BYTES),
            ),
        );
        for request in 1..=REQUESTS {
            partitions
                .on_request_with_reply(send_request(namespace, request as u64), None)
                .await;
            owner.process_loopback(&mut round).await;
            let prepare = partitions
                .get_by_ns(&namespace)
                .unwrap()
                .consensus()
                .with_pipeline(|pipeline| pipeline.entry_by_op(request as u64).unwrap().header);
            for replica in [1, 2] {
                let ack = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                    .transmute_header(|_, header: &mut PrepareOkHeader| {
                        header.command = Command::PrepareOk;
                        header.operation = prepare.operation;
                        header.cluster = prepare.cluster;
                        header.group = prepare.group;
                        header.replica = replica;
                        header.view = prepare.view;
                        header.op = prepare.op;
                        header.prepare_checksum = prepare.checksum;
                        header.size = u32::try_from(size_of::<PrepareOkHeader>()).unwrap();
                        header.seal();
                    });
                partitions
                    .get_mut_by_ns(&namespace)
                    .unwrap()
                    .on_ack(ack, partitions.config())
                    .await;
                owner.process_loopback(&mut round).await;
            }
            assert_eq!(
                partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                request as u64
            );
        }
        assert!(
            !round.entries.is_empty(),
            "the original snapshot still spans pump events"
        );
    }

    #[compio::test]
    async fn loopback_rounds_retain_namespace_order_and_reject_replaced_incarnations() {
        for replace_tail in [false, true] {
            let store = Rc::new(HeldSuperblock {
                entered: RefCell::new(None),
                held: RefCell::new(None),
            });
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, Some(&store));
            let mut results = Vec::new();
            for partition_id in (0..TEST_PARTITIONS).rev() {
                let namespace = IggyNamespace::new(0, 0, partition_id);
                for request in 1..=consensus::PIPELINE_PREPARE_QUEUE_MAX {
                    let (reply, result) = consensus::oneshot_channel();
                    owner
                        .plane
                        .partitions()
                        .on_request_with_reply(send_request(namespace, request as u64), Some(reply))
                        .await;
                    results.push(result);
                }
            }
            let mut round = crate::router::LoopbackRound::default();
            assert_eq!(
                owner.process_loopback(&mut round).await,
                crate::router::COOPERATIVE_EVENT_BUDGET
            );
            let tail_namespace = IggyNamespace::new(0, 0, TEST_PARTITIONS - 1);
            let partitions = owner.plane.partitions();
            assert_eq!(
                partitions
                    .get_by_ns(&tail_namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                0
            );
            let first_namespace = IggyNamespace::new(0, 0, 0);
            let next_request = consensus::PIPELINE_PREPARE_QUEUE_MAX as u64 + 1;
            let (reply, result) = consensus::oneshot_channel();
            partitions
                .on_request_with_reply(send_request(first_namespace, next_request), Some(reply))
                .await;
            results.push(result);

            if replace_tail {
                let retired = partitions.remove(&tail_namespace).unwrap();
                let replacement = IggyPartition::with_in_memory_storage(
                    Arc::new(PartitionStats::default()),
                    VsrConsensus::new(
                        1,
                        0,
                        1,
                        tail_namespace.inner(),
                        Rc::clone(&bus),
                        LocalPipeline::new(),
                    ),
                    IggyByteSize::from(SEGMENT_BYTES),
                );
                replacement.consensus().init();
                partitions.insert(tail_namespace, replacement);
                drop(retired);
            }
            assert_eq!(
                owner.process_loopback(&mut round).await,
                consensus::PIPELINE_PREPARE_QUEUE_MAX + 1
            );
            assert_eq!(
                partitions
                    .get_by_ns(&first_namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                next_request,
                "new self-acks follow the older snapshot entries"
            );
            assert_eq!(
                partitions
                    .get_by_ns(&tail_namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                if replace_tail {
                    0
                } else {
                    consensus::PIPELINE_PREPARE_QUEUE_MAX as u64
                },
                "snapshot messages belong only to their captured incarnation",
            );
            assert_eq!(owner.process_loopback(&mut round).await, 0);
            assert_eq!(
                partitions
                    .get_by_ns(&first_namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                next_request
            );
            assert!(round.entries.is_empty());
            drop(results);
        }
    }

    #[compio::test]
    async fn queued_file_result_keeps_capacity_until_tombstoned_owner_accepts_it() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut owner, _sender) = test_owner(&bus, Some(&store));
        owner.partition_io = super::PartitionIoLane::new(
            super::PartitionIoLimits::new(1, None).unwrap(),
            &owner.metrics,
        );
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let identity = captured.identity;
        let charge = owner.partition_io.charged.get();
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let partitions = owner.plane.partitions();
        let other = IggyNamespace::new(0, 0, 1);
        let other_incarnation = partitions.get_by_ns(&other).unwrap().incarnation();
        assert!(
            owner
                .partition_io
                .try_reserve(other, other_incarnation, charge)
                .is_none()
        );
        let teardown = partitions
            .get_mut_by_ns(&identity.namespace)
            .unwrap()
            .capture_teardown();
        partitions.tombstone(identity.namespace);
        let drain = teardown.drain().fuse();
        futures::pin_mut!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        task.await;
        assert_eq!(owner.partition_io.counters.queued.get(), 1);
        assert_eq!(owner.partition_io.charged.get(), charge);
        assert!(
            futures::poll!(&mut drain).is_pending(),
            "physical completion does not bypass owner settlement"
        );
        let token = owner.partition_io.try_recv().unwrap();
        owner.accept_partition_io_completion(token);
        drain.await.unwrap();
        assert_eq!(owner.partition_io.charged.get(), 0);
        assert!(
            owner
                .partition_io
                .try_reserve(other, other_incarnation, charge)
                .is_some()
        );
        assert!(
            owner.partition_io.take_result(token).is_none(),
            "duplicate completion cannot accept a reused slot"
        );
    }

    #[compio::test]
    async fn view_change_sends_held_actions_as_soon_as_the_superblock_completes() {
        const STEPS_MAX: usize = 8;
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        for replica in 1..3 {
            assert!(bus.owner_table().try_claim(replica, 1));
        }
        let (owner, _sender) = test_owner(&bus, None);
        let partitions = owner.plane.partitions();
        let namespace = IggyNamespace::new(0, 0, 0);
        partitions.remove(&namespace).unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            namespace.inner(),
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(SEGMENT_BYTES),
        );
        partition.set_superblock(store, None);
        partitions.insert(namespace, partition);
        partitions.set_io_notifier(
            owner.partition_io.notifier(),
            owner.partition_io.limits.bytes_max(),
        );
        let message = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
            .transmute_header(|_, header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                header.cluster = 1;
                header.group = namespace.inner();
                header.replica = 1;
                header.view = 1;
                header.seal();
            });
        owner.on_start_view_change(message).await;
        assert!(sent.borrow().is_empty());
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        let partitions::PartitionIoStep::Ready(plan) =
            partition.resume_io(partitions.config()).await
        else {
            panic!("view change must queue its superblock write");
        };
        let captured = partition
            .capture_io(plan, partitions.config())
            .unwrap()
            .unwrap();
        let result = captured.job.execute().await;
        partition.accept_io(captured.identity, result).unwrap();
        captured.gate.unwrap().release();
        for _ in 0..STEPS_MAX {
            owner.service_partition_io().await;
            if !sent.borrow().is_empty() {
                break;
            }
        }
        let sent = sent.borrow();
        assert!(
            sent.iter().any(|frame| {
                let message =
                    Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                        .unwrap();
                message.header().command == Command::StartViewChange
            }),
            "the completion must send StartViewChange without a retransmission tick"
        );
    }

    #[compio::test]
    async fn view_change_waits_for_held_writer_and_queued_result_acceptance() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let namespace = captured.identity.namespace;
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let partitions = owner.plane.partitions();
        let old_view = partitions.get_by_ns(&namespace).unwrap().consensus().view();
        let message = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
            .transmute_header(|_, header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                header.cluster = partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .consensus()
                    .cluster();
                header.group = namespace.inner();
                header.view = old_view + 1;
                header.seal();
            });
        owner.on_start_view_change(message).await;
        owner.service_partition_io().await;
        assert_eq!(
            partitions.get_by_ns(&namespace).unwrap().consensus().view(),
            old_view,
            "view adoption must wait for the captured writer"
        );
        assert_eq!(owner.partition_io.outstanding(), 1);

        let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        task.await;
        assert_eq!(owner.partition_io.counters.queued.get(), 1);
        assert_eq!(
            partitions.get_by_ns(&namespace).unwrap().consensus().view(),
            old_view,
            "a completed but unaccepted writer still owns the old history"
        );
        for _ in 0..TEST_PARTITIONS {
            owner.service_partition_io().await;
            if partitions.get_by_ns(&namespace).unwrap().consensus().view() != old_view {
                break;
            }
        }
        assert_eq!(
            partitions.get_by_ns(&namespace).unwrap().consensus().view(),
            old_view + 1
        );
        assert_eq!(owner.partition_io.charged.get(), 0);
    }

    #[compio::test]
    async fn given_held_writer_when_view_frames_arrive_should_defer_only_current_work() {
        const CURRENT_VIEW: u32 = 3;
        for (command, view, peer, deferred) in [
            (Command::StartViewChange, CURRENT_VIEW - 1, 1, false),
            (Command::DoViewChange, CURRENT_VIEW - 1, 1, false),
            (Command::StartView, CURRENT_VIEW - 1, 2, false),
            (Command::StartViewChange, CURRENT_VIEW, 1, false),
            (Command::StartViewChange, CURRENT_VIEW + 1, 1, true),
            (Command::DoViewChange, CURRENT_VIEW * 2, 1, true),
            (Command::StartView, CURRENT_VIEW + 1, 1, true),
        ] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            let partitions = owner.plane.partitions();
            let namespace = IggyNamespace::new(0, 0, 0);
            drop(Box::new(partitions.remove(&namespace).unwrap()));
            let mut consensus = VsrConsensus::new(
                1,
                0,
                3,
                namespace.inner(),
                Rc::clone(&bus),
                LocalPipeline::new(),
            );
            consensus.set_view(CURRENT_VIEW);
            consensus.init();
            let mut partition = IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(SEGMENT_BYTES),
            );
            partition.set_superblock(
                Rc::new(HeldSuperblock {
                    entered: RefCell::new(None),
                    held: RefCell::new(None),
                }),
                None,
            );
            partitions.insert(namespace, partition);
            partitions.set_io_notifier(
                owner.partition_io.notifier(),
                owner.partition_io.limits.bytes_max(),
            );
            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            assert!(!partition.persist_superblock_if_needed().await);
            let partitions::PartitionIoStep::Ready(plan) =
                partition.resume_io(partitions.config()).await
            else {
                panic!("the current view must persist before sending its actions");
            };
            let slot = owner
                .partition_io
                .try_reserve(namespace, partition.incarnation(), plan.allocation_charge)
                .unwrap();
            let captured = partition
                .capture_io(plan, partitions.config())
                .unwrap()
                .unwrap();
            let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
            owner.partition_io.dispatch(slot, captured, &captured_bus);
            owner
                .on_message(view_control_message(namespace, command, view, peer))
                .await;
            assert_eq!(
                partitions.get_by_ns(&namespace).unwrap().consensus().view(),
                CURRENT_VIEW
            );

            let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
            task.await;
            owner.accept_partition_io_completion(owner.partition_io.try_recv().unwrap());
            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            let step = partition.resume_io(partitions.config()).await;
            assert_eq!(
                matches!(step, partitions::PartitionIoStep::Transition(_)),
                deferred,
                "{command:?} from view {view} must not add an obsolete WAL drain"
            );
            assert!(partition.fatal().is_none());
        }
    }

    #[compio::test]
    async fn given_stopping_owner_when_transfer_work_arrives_should_preserve_pending_artifacts() {
        const NONCE: u128 = 7;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (source_owner, _) = test_owner(&bus, None);
        let source_directory = tempfile::tempdir().unwrap();
        let namespace = IggyNamespace::new(0, 0, 0);
        let sources = source_owner.plane.partitions();
        attach_segment_files(
            sources.get_mut_by_ns(&namespace).unwrap(),
            source_directory.path(),
            false,
        )
        .await;
        let (reply, replied) = consensus::oneshot_channel();
        sources
            .on_request_with_reply(send_request(namespace, 1), Some(reply))
            .await;
        source_owner
            .process_loopback(&mut crate::router::LoopbackRound::default())
            .await;
        replied.await.unwrap();
        let source = sources.get_mut_by_ns(&namespace).unwrap();
        let offer = source.state_transfer_offer(sources.config()).await.unwrap();
        let offsets_index = offer.artifact_count() - 1;
        let PartitionArtifactSource::Offsets(offsets) = offer.artifact_at(offsets_index).unwrap()
        else {
            panic!("the final offer artifact contains the consumer offsets");
        };
        let offsets_entry = offer.manifest()[offsets_index];
        let segment = send_request(namespace, 1).body().to_vec();
        let segment_entry = StateArtifact::for_bytes(artifact_kind::SEGMENT_LOG, 0, &segment);

        for shard_stop in [false, true] {
            for (entry, bytes) in [(segment_entry, &segment), (offsets_entry, offsets.as_ref())] {
                let directory = tempfile::tempdir().unwrap();
                let (owner, _) = test_owner(&bus, None);
                let partitions = owner.plane.partitions();
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
                partition
                    .consensus()
                    .set_state_transfer_stage(StateTransferStage::AwaitingTarget);
                if shard_stop {
                    owner.shutting_down.set(true);
                } else {
                    partition.begin_shutdown_io();
                }
                assert!(!owner.arm_partition_transfer(partition, 0, 0).await);
                assert!(partition.transfer.is_none());
                assert!(partition.transfer_rearm.is_none());
                partition
                    .consensus()
                    .set_state_transfer_stage(StateTransferStage::Fetching);
                partition.transfer = Some(PartitionTransferSession {
                    nonce: NONCE,
                    peer: 0,
                    commit_op: offer.commit_op,
                    artifacts: vec![TransferArtifact::Pending(ArtifactProgress {
                        entry,
                        buf: bytes.clone(),
                    })],
                    target_accepted: true,
                    idle_ticks: 0,
                });

                owner
                    .on_partition_transfer_progress(namespace.inner())
                    .await;

                let partition = partitions.get_by_ns(&namespace).unwrap();
                assert_eq!(
                    partition.consensus().state_transfer_stage(),
                    StateTransferStage::Fetching
                );
                let transfer = partition.transfer.as_ref().unwrap();
                assert_eq!(transfer.nonce, NONCE);
                assert_eq!(transfer.artifacts[0].pending().unwrap().buf, *bytes);
                assert!(
                    std::fs::read_dir(directory.path())
                        .unwrap()
                        .next()
                        .is_none()
                );
                assert!(!partition.read_history_is_changing());
            }
        }
    }

    #[compio::test]
    async fn repair_replies_during_held_io_do_not_discard_live_prepares() {
        for command in [Command::RangeEvicted, Command::RepairDone] {
            for nonce in [1, 2] {
                let store = Rc::new(HeldSuperblock {
                    entered: RefCell::new(None),
                    held: RefCell::new(None),
                });
                let bus = Rc::new(IggyMessageBus::new(0));
                let (owner, _sender) = test_owner(&bus, None);
                let partitions = owner.plane.partitions();
                let namespace = IggyNamespace::new(0, 0, 0);
                let mut primary = VsrConsensus::new(
                    1,
                    1,
                    3,
                    namespace.inner(),
                    bus.clone(),
                    LocalPipeline::new(),
                );
                primary.set_view(1);
                primary.set_log_view(1);
                primary.init();
                let mut origin: IggyPartition<_, HeldSuperblock> =
                    IggyPartition::with_in_memory_storage(
                        Arc::new(PartitionStats::default()),
                        primary,
                        IggyByteSize::from(SEGMENT_BYTES),
                    );
                origin.on_request(send_request(namespace, 1), None).await;
                let frozen = origin.log.journal().inner.repair_entry(1).unwrap();
                let mut prepare = Message::<PrepareHeader>::new(frozen.len());
                prepare.as_mut_slice().copy_from_slice(frozen.as_slice());

                let mut backup = VsrConsensus::new(
                    1,
                    2,
                    3,
                    namespace.inner(),
                    bus.clone(),
                    LocalPipeline::new(),
                );
                backup.set_view(1);
                backup.set_log_view(1);
                backup.init();
                let mut partition = IggyPartition::with_in_memory_storage(
                    Arc::new(PartitionStats::default()),
                    backup,
                    IggyByteSize::from(SEGMENT_BYTES),
                );
                partition.set_superblock(Rc::clone(&store), None);
                partitions.insert(namespace, partition);
                partitions.set_io_notifier(
                    owner.partition_io.notifier(),
                    owner.partition_io.limits.bytes_max(),
                );
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                assert!(!partition.persist_superblock_if_needed().await);
                let partitions::PartitionIoStep::Ready(plan) =
                    partition.resume_io(partitions.config()).await
                else {
                    panic!("the backup must persist its view");
                };
                let slot = owner
                    .partition_io
                    .try_reserve(namespace, partition.incarnation(), plan.allocation_charge)
                    .unwrap();
                let captured = partition
                    .capture_io(plan, partitions.config())
                    .unwrap()
                    .unwrap();
                let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
                owner.partition_io.dispatch(slot, captured, &captured_bus);
                partition.repair = Some(RepairSession {
                    nonce: 1,
                    view: 1,
                    from_op: 1,
                    commit_to_op: 0,
                    fetch_to_op: 0,
                    floor: None,
                    peer: 1,
                    idle_ticks: 0,
                });
                let reply =
                    Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
                        .transmute_header(|_, header: &mut RepairRangeReplyHeader| {
                            header.command = command;
                            header.size =
                                u32::try_from(size_of::<RepairRangeReplyHeader>()).unwrap();
                            header.group = namespace.inner();
                            header.cluster = 1;
                            header.replica = 1;
                            header.nonce = nonce;
                            header.op = 1;
                            header.seal();
                        });
                owner.on_repair_range_reply(&reply).await;
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                partition.on_replicate(prepare).await;
                assert_eq!(
                    partition.consensus().sequencer().current_sequence(),
                    1,
                    "{command:?} with nonce {nonce} must not fence a live prepare"
                );
                assert!(partition.log.journal().inner.holds_op(1));
                assert_eq!(partition.take_prepare_gap_drops(), 0);
                assert_eq!(
                    partition.complete_repair(partitions.config()).await,
                    RepairConclusion::InProgress
                );
                let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
                task.await;
                assert_eq!(
                    partition.complete_repair(partitions.config()).await,
                    RepairConclusion::InProgress,
                    "completed file results still require acceptance"
                );
                owner.accept_partition_io_completion(owner.partition_io.try_recv().unwrap());
                for _ in 0..TEST_PARTITIONS {
                    owner.service_partition_io().await;
                }
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                assert_eq!(
                    partition.complete_repair(partitions.config()).await,
                    RepairConclusion::Done
                );
                assert_eq!(partition.consensus().commit_min(), 0);
            }
        }
    }

    #[compio::test]
    async fn repair_completion_waits_for_held_writer_and_queued_result_acceptance() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let namespace = captured.identity.namespace;
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let partitions = owner.plane.partitions();
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        partition.repair = Some(RepairSession {
            nonce: 1,
            view: partition.consensus().view(),
            from_op: 1,
            commit_to_op: 0,
            fetch_to_op: 0,
            floor: None,
            peer: 0,
            idle_ticks: 0,
        });
        assert_eq!(
            partition.complete_repair(partitions.config()).await,
            RepairConclusion::InProgress
        );
        assert!(partition.repair.is_some());
        let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        task.await;
        assert_eq!(
            partition.complete_repair(partitions.config()).await,
            RepairConclusion::InProgress,
            "repair must retain its session until the old result is accepted"
        );
        let token = owner.partition_io.try_recv().unwrap();
        owner.accept_partition_io_completion(token);
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        assert_eq!(
            partition.complete_repair(partitions.config()).await,
            RepairConclusion::Done
        );
        assert!(partition.repair.is_none());
        assert_eq!(partition.consensus().commit_min(), 0);
        assert_eq!(owner.partition_io.charged.get(), 0);
    }

    #[compio::test]
    async fn quarantine_waits_for_held_writer_and_confirms_only_after_file_success() {
        for missing_directory in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let partition_path = directory.path().join("partition");
            let segment_path = partition_path.join("00000000000000000000.log");
            let contents = b"retained partition evidence";
            if !missing_directory {
                std::fs::create_dir(&partition_path).unwrap();
                std::fs::write(&segment_path, contents).unwrap();
            }
            let store = Rc::new(HeldSuperblock {
                entered: RefCell::new(None),
                held: RefCell::new(None),
            });
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, Some(&store));
            let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
            let (slot, captured) = capture_first_write(&owner).await;
            let namespace = captured.identity.namespace;
            owner.partition_io.dispatch(slot, captured, &captured_bus);
            let partitions = owner.plane.partitions();
            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            partition.set_partition_dir(partition_path.to_str().unwrap().to_owned());
            owner.fence_partition_for_rebuild(namespace, partition, partition.offset_frontier());
            owner.service_partition_io().await;
            owner.apply_reconcile_ops();
            assert!(partitions.is_tombstoned(&namespace));
            assert!(partitions.get_io_owner(&namespace).is_some());
            assert!(!directory.path().join("partition.fenced.0").exists());
            assert_eq!(segment_path.exists(), !missing_directory);

            let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
            task.await;
            owner.apply_reconcile_ops();
            assert!(partitions.get_io_owner(&namespace).is_some());
            assert!(!directory.path().join("partition.fenced.0").exists());
            let token = owner.partition_io.try_recv().unwrap();
            owner.accept_partition_io_completion(token);
            partitions.get_io_owner(&namespace).unwrap().notify_io();
            let finish = async {
                let mut namespaces = Vec::new();
                while !partitions
                    .get_io_owner(&namespace)
                    .unwrap()
                    .shutdown_io_complete()
                {
                    assert!(owner.tick_partitions(&mut namespaces).await.is_none());
                    owner.service_partition_io().await;
                    compio::time::sleep(TASK_POLL_INTERVAL).await;
                }
            };
            // Heap-pinned: on macOS `finish` outgrows clippy's `large_futures` cap.
            compio::time::timeout(REPLY_DEADLINE, Box::pin(finish))
                .await
                .unwrap();
            owner.apply_reconcile_ops();
            assert_eq!(
                partitions.get_io_owner(&namespace).is_some(),
                missing_directory,
                "a quarantine failure must withhold removal confirmation"
            );
            assert_eq!(owner.partition_io.charged.get(), 0);
            if !missing_directory {
                assert!(!segment_path.exists());
                assert_eq!(
                    std::fs::read(
                        directory
                            .path()
                            .join("partition.fenced.0/00000000000000000000.log")
                    )
                    .unwrap(),
                    contents
                );
            }
        }
    }

    #[compio::test]
    async fn stale_completion_settles_old_writer_without_publishing_into_replacement() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let identity = captured.identity;
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        task.await;
        let partitions = owner.plane.partitions();
        let old = partitions.remove(&identity.namespace).unwrap();
        let teardown = old.capture_teardown();
        let drain = teardown.drain().fuse();
        futures::pin_mut!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            identity.namespace.inner(),
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.init();
        partitions.insert(
            identity.namespace,
            IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(SEGMENT_BYTES),
            ),
        );
        assert_ne!(
            partitions
                .get_by_ns(&identity.namespace)
                .unwrap()
                .incarnation(),
            identity.incarnation
        );
        let token = owner.partition_io.try_recv().unwrap();
        owner.accept_partition_io_completion(token);
        drain.await.unwrap();
        let replacement = partitions.get_by_ns(&identity.namespace).unwrap();
        assert_eq!(replacement.consensus().commit_min(), 0);
        assert_eq!(replacement.offset_frontier(), 0);
        assert!(replacement.fatal().is_none());
        assert_eq!(owner.partition_io.charged.get(), 0);
    }

    #[compio::test]
    async fn dropped_unpolled_job_fences_writer_and_retains_its_reservation() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let identity = captured.identity;
        let charge = owner.partition_io.charged.get();
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        captured_bus.spawned_tasks.borrow_mut().clear();
        owner.service_partition_io().await;
        let partition = owner
            .plane
            .partitions()
            .get_io_owner(&identity.namespace)
            .unwrap();
        assert!(partition.fatal().is_some());
        assert!(!partition.needs_persistence_checkpoint());
        assert!(owner.tick_partitions(&mut Vec::new()).await.is_some());
        assert!(partition.capture_teardown().drain().await.is_err());
        assert_eq!(owner.partition_io.counters.active.get(), 0);
        assert_eq!(owner.partition_io.counters.quarantined.get(), 1);
        assert_eq!(owner.metrics.partition_io_fenced_counter().get(), 1);
        assert_eq!(owner.metrics.partition_io_timeouts_counter().get(), 0);
        assert_eq!(owner.partition_io.charged.get(), charge);
        owner.partition_io.release(slot);
        assert_eq!(owner.partition_io.charged.get(), charge);
        assert!(
            owner
                .partition_io
                .try_reserve(identity.namespace, identity.incarnation, charge)
                .is_none()
        );
    }

    #[compio::test]
    async fn given_stuck_file_job_when_timeout_expires_should_fence_and_ignore_late_completion() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        owner.set_partition_io_timeout(partitions::PARTITION_IO_DRAIN_TIMEOUT);
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let identity = captured.identity;
        let charge = owner.partition_io.charged.get();
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let mut elapsed = Duration::ZERO;
        while elapsed + crate::CONSENSUS_TICK_INTERVAL < partitions::PARTITION_IO_DRAIN_TIMEOUT {
            owner.partition_io.tick();
            elapsed += crate::CONSENSUS_TICK_INTERVAL;
        }
        assert_eq!(owner.partition_io.counters.active.get(), 1);
        assert_eq!(
            owner.partition_io.interrupted(),
            [] as [partitions::PartitionIoIdentity; 0]
        );
        let mut scratch = Vec::new();
        assert!(owner.tick_partitions(&mut scratch).await.is_some());
        let partition = owner
            .plane
            .partitions()
            .get_io_owner(&identity.namespace)
            .unwrap();
        assert!(partition.fatal().is_some());
        assert_eq!(owner.partition_io.counters.quarantined.get(), 1);
        assert_eq!(owner.metrics.partition_io_fenced_counter().get(), 1);
        assert_eq!(owner.metrics.partition_io_timeouts_counter().get(), 1);
        let completion = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        completion.await;
        assert!(owner.partition_io.try_recv().is_none());
        assert!(partition.capture_teardown().drain().await.is_err());
        owner.partition_io.release(slot);
        assert_eq!(owner.partition_io.charged.get(), charge);
        assert_eq!(owner.partition_io.counters.active.get(), 0);
        assert_eq!(owner.partition_io.counters.quarantined.get(), 1);
        assert_eq!(owner.metrics.partition_io_fenced_counter().get(), 1);
        assert_eq!(owner.metrics.partition_io_timeouts_counter().get(), 1);
        assert!(
            owner
                .partition_io
                .try_reserve(identity.namespace, identity.incarnation, charge)
                .is_none()
        );
    }

    #[compio::test]
    async fn closing_admission_preserves_local_result_for_acceptance() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        owner.partition_io.close();
        let task = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        task.await;
        assert!(owner.partition_io.has_ready());
        assert!(owner.partition_io.charged.get() > 0);
        let token = owner.partition_io.try_recv().unwrap();
        owner.accept_partition_io_completion(token);
        assert_eq!(owner.partition_io.charged.get(), 0);
        assert_eq!(owner.partition_io.counters.queued.get(), 0);
    }

    #[test]
    fn replacing_a_ready_owner_keeps_its_settled_reservation_serviceable() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _) = test_owner(&bus, Some(&store));
        let partitions = owner.plane.partitions();
        let namespace = IggyNamespace::new(0, 0, 0);
        let old = partitions.get_by_ns(&namespace).unwrap().incarnation();
        let replacement = partitions
            .get_by_ns(&IggyNamespace::new(0, 0, 1))
            .unwrap()
            .incarnation();
        let lane = &owner.partition_io;
        let slot = lane.try_reserve(namespace, old, 1).unwrap();
        lane.slots.borrow()[slot]
            .as_ref()
            .unwrap()
            .state
            .set(super::SlotState::Settled);
        lane.reschedule(namespace, old);
        lane.reschedule(namespace, replacement);
        assert_eq!(
            lane.head(),
            Some((namespace, old)),
            "replacement readiness must not strand a settled old reservation"
        );
        lane.pop_ready(namespace, old);
        lane.release(slot);
        assert_eq!(lane.head(), Some((namespace, replacement)));
    }

    #[test]
    fn given_many_retries_when_completion_arrives_should_prioritize_it_without_starving_retries() {
        const RETRY_OWNERS: usize = 1000;
        let lane = super::PartitionIoLane::<HeldSuperblock>::new(
            super::PartitionIoLimits::new(2, None).unwrap(),
            &ShardMetrics::for_shard(),
        );
        let incarnation = partitions::PartitionIncarnation::default();
        for index in 0..RETRY_OWNERS {
            lane.retry(IggyNamespace::new(0, 0, index), incarnation);
        }
        let completed = IggyNamespace::new(0, 0, RETRY_OWNERS - 1);
        lane.reschedule(completed, incarnation);
        for _ in 0..super::CONTINUATIONS_PER_RETRY {
            assert_eq!(lane.head(), Some((completed, incarnation)));
            lane.continue_ready(completed, incarnation);
        }
        assert_eq!(
            lane.head(),
            Some((IggyNamespace::new(0, 0, 0), incarnation))
        );
        lane.pop_ready(IggyNamespace::new(0, 0, 0), incarnation);
        lane.pop_ready(completed, incarnation);
        for index in 1..RETRY_OWNERS - 1 {
            let namespace = IggyNamespace::new(0, 0, index);
            assert_eq!(lane.head(), Some((namespace, incarnation)));
            lane.pop_ready(namespace, incarnation);
        }
        assert!(!lane.has_ready());
        assert!(lane.head().is_none(), "a promoted retry must not run twice");
    }

    #[test]
    fn given_woken_capacity_waiter_when_cpu_work_progresses_should_keep_its_fifo_place() {
        let limits = super::PartitionIoLimits::new(1, None).unwrap();
        let lane =
            super::PartitionIoLane::<HeldSuperblock>::new(limits, &ShardMetrics::for_shard());
        let incarnation = partitions::PartitionIncarnation::default();
        let owners: [_; 3] = std::array::from_fn(|index| IggyNamespace::new(0, 0, index));
        let occupied = lane.try_reserve(owners[0], incarnation, 1).unwrap();
        for owner in &owners[1..] {
            lane.reschedule(*owner, incarnation);
            lane.wait_for_capacity(*owner, incarnation);
        }
        lane.release(occupied);
        assert_eq!(lane.head(), Some((owners[1], incarnation)));
        lane.continue_ready(owners[1], incarnation);
        assert!(lane.try_reserve(owners[2], incarnation, 1).is_none());
        assert_eq!(lane.head(), Some((owners[1], incarnation)));
        assert!(lane.try_reserve(owners[1], incarnation, 1).is_some());
        lane.pop_ready(owners[1], incarnation);
        assert_eq!(lane.head(), Some((owners[2], incarnation)));
    }

    #[test]
    fn released_capacity_retries_the_oldest_waiter_behind_the_active_head() {
        let limits = super::PartitionIoLimits::new(2, None).unwrap();
        let lane =
            super::PartitionIoLane::<HeldSuperblock>::new(limits, &ShardMetrics::for_shard());
        let owners: [_; 5] = std::array::from_fn(|index| {
            (
                IggyNamespace::new(0, 0, index),
                partitions::PartitionIncarnation::default(),
            )
        });
        let occupied = lane
            .try_reserve(owners[0].0, owners[0].1, limits.bytes_max())
            .unwrap();
        for owner in &owners[1..=2] {
            lane.reschedule(owner.0, owner.1);
            assert!(lane.try_reserve(owner.0, owner.1, 1).is_none());
            lane.wait_for_capacity(owner.0, owner.1);
        }
        for owner in &owners[3..] {
            lane.reschedule(owner.0, owner.1);
        }

        lane.release(occupied);
        assert_eq!(
            lane.head(),
            Some(owners[3]),
            "release must preserve the head held by its caller"
        );
        lane.pop_ready(owners[3].0, owners[3].1);
        assert_eq!(
            lane.head(),
            Some(owners[1]),
            "a capacity waiter must not wait for every runnable owner"
        );
        assert!(lane.try_reserve(owners[1].0, owners[1].1, 1).is_some());
        lane.pop_ready(owners[1].0, owners[1].1);
        assert_eq!(lane.head(), Some(owners[4]));
        lane.pop_ready(owners[4].0, owners[4].1);
        assert_eq!(lane.head(), Some(owners[2]));
        assert!(lane.try_reserve(owners[2].0, owners[2].1, 1).is_some());
    }

    #[test]
    fn oldest_large_reservation_waits_without_allowing_smaller_jobs_to_overtake() {
        let limits = super::PartitionIoLimits::new(TEST_PARTITIONS, None).unwrap();
        let lane =
            super::PartitionIoLane::<HeldSuperblock>::new(limits, &ShardMetrics::for_shard());
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, Some(&store));
        let identities: Vec<_> = (0..TEST_PARTITIONS)
            .map(|index| {
                let namespace = IggyNamespace::new(0, 0, index);
                (
                    namespace,
                    owner
                        .plane
                        .partitions()
                        .get_by_ns(&namespace)
                        .unwrap()
                        .incarnation(),
                )
            })
            .collect();
        let half = limits.bytes_max() / 2;
        let first = lane
            .try_reserve(identities[0].0, identities[0].1, half)
            .unwrap();
        lane.reschedule(identities[1].0, identities[1].1);
        lane.reschedule(identities[2].0, identities[2].1);
        assert!(
            lane.try_reserve(identities[1].0, identities[1].1, limits.bytes_max())
                .is_none()
        );
        lane.wait_for_capacity(identities[1].0, identities[1].1);
        assert_eq!(
            lane.head(),
            Some(identities[2]),
            "CPU continuations remain runnable"
        );
        assert!(
            lane.try_reserve(identities[2].0, identities[2].1, half)
                .is_none()
        );
        lane.wait_for_capacity(identities[2].0, identities[2].1);
        assert!(lane.head().is_none(), "file jobs wait without spinning");
        lane.release(first);
        assert_eq!(lane.head(), Some(identities[1]));
        assert_eq!(
            lane.ready.queue.borrow().len(),
            1,
            "one capacity release retries only the oldest file job"
        );
        let large = lane
            .try_reserve(identities[1].0, identities[1].1, limits.bytes_max())
            .unwrap();
        lane.pop_ready(identities[1].0, identities[1].1);
        assert!(
            lane.try_reserve(identities[2].0, identities[2].1, half)
                .is_none()
        );
        lane.release(large);
        assert_eq!(lane.head(), Some(identities[2]));
        assert!(
            lane.try_reserve(identities[2].0, identities[2].1, half)
                .is_some()
        );
    }

    #[test]
    fn configured_limits_reject_unserviceable_single_records_and_invalid_capacities() {
        let minimum = partitions::largest_legal_job_charge().unwrap();
        assert!(super::PartitionIoLimits::new(0, None).is_err());
        assert!(super::PartitionIoLimits::new(super::PARTITION_IO_CAPACITY_MAX + 1, None).is_err());
        assert!(super::PartitionIoLimits::new(1, Some(minimum - 1)).is_err());
        assert!(super::PartitionIoLimits::new(1, Some(usize::MAX)).is_err());
        assert_eq!(
            super::PartitionIoLimits::new(1, Some(minimum))
                .unwrap()
                .bytes_max(),
            minimum
        );
        assert!(
            super::PartitionIoLimits::new(super::DEFAULT_PARTITION_IO_CAPACITY, None)
                .unwrap()
                .bytes_max()
                >= minimum
        );
    }

    #[compio::test]
    async fn given_configured_wedge_window_when_file_job_is_slow_should_keep_running() {
        // Default `[cluster] superblock_wedged_fatal_timeout` in core/server/config.toml.
        const WEDGE_WINDOW_DEFAULT: Duration = Duration::from_secs(120);
        const SLOW_JOB: Duration = Duration::from_secs(31);
        for timeout in [Duration::ZERO, WEDGE_WINDOW_DEFAULT] {
            let store = Rc::new(HeldSuperblock {
                entered: RefCell::new(None),
                held: RefCell::new(None),
            });
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, Some(&store));
            owner.set_partition_io_timeout(timeout);
            let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
            let (slot, captured) = capture_first_write(&owner).await;
            let identity = captured.identity;
            owner.partition_io.dispatch(slot, captured, &captured_bus);
            let mut elapsed = Duration::ZERO;
            while elapsed < SLOW_JOB {
                owner.partition_io.tick();
                elapsed += crate::CONSENSUS_TICK_INTERVAL;
            }
            let mut scratch = Vec::new();
            assert!(
                owner.tick_partitions(&mut scratch).await.is_none(),
                "a {SLOW_JOB:?} file job stopped the server with timeout {timeout:?}"
            );
            let completion = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
            completion.await;
            let token = owner
                .partition_io
                .try_recv()
                .expect("the slow job result must reach its owner");
            owner.accept_partition_io_completion(token);
            let partition = owner
                .plane
                .partitions()
                .get_io_owner(&identity.namespace)
                .unwrap();
            assert!(partition.fatal().is_none());
        }
    }

    #[compio::test]
    async fn given_deleted_owner_when_file_job_times_out_should_stop_the_node() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut owner, _sender) = test_owner(&bus, Some(&store));
        owner.partition_io = super::PartitionIoLane::new(
            super::PartitionIoLimits::new(1, None).unwrap(),
            &owner.metrics,
        );
        owner.set_partition_io_timeout(partitions::PARTITION_IO_DRAIN_TIMEOUT);
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        let identity = captured.identity;
        let charge = owner.partition_io.charged.get();
        owner.partition_io.dispatch(slot, captured, &captured_bus);
        let partitions = owner.plane.partitions();
        let other = IggyNamespace::new(0, 0, 1);
        let other_incarnation = partitions.get_by_ns(&other).unwrap().incarnation();

        // `tear_down_owned_partition` tombstones before draining the writer.
        let teardown = partitions.capture_teardown(&identity.namespace).unwrap();
        partitions.tombstone(identity.namespace);
        owner.shards_table.remove(&identity.namespace);
        let drain = teardown.drain().fuse();
        futures::pin_mut!(drain);
        assert!(futures::poll!(&mut drain).is_pending());

        let mut elapsed = Duration::ZERO;
        while elapsed + crate::CONSENSUS_TICK_INTERVAL < partitions::PARTITION_IO_DRAIN_TIMEOUT {
            owner.partition_io.tick();
            elapsed += crate::CONSENSUS_TICK_INTERVAL;
        }
        let mut scratch = Vec::new();
        let fault = owner.tick_partitions(&mut scratch).await;

        assert!(
            partitions
                .get_io_owner(&identity.namespace)
                .unwrap()
                .fatal()
                .is_some(),
            "the timeout fences the deleted owner"
        );
        assert!(
            matches!(futures::poll!(&mut drain), std::task::Poll::Ready(Err(_))),
            "the pending delete fails once the writer is interrupted"
        );
        assert!(
            partitions
                .capture_teardown(&identity.namespace)
                .unwrap()
                .drain()
                .await
                .is_err(),
            "a retried delete fails at once and keeps the files"
        );
        let late = captured_bus.spawned_tasks.borrow_mut().pop().unwrap();
        late.await;
        assert!(owner.partition_io.try_recv().is_none());
        owner.partition_io.release(slot);
        assert_eq!(owner.partition_io.charged.get(), charge);
        assert!(
            owner
                .partition_io
                .try_reserve(other, other_incarnation, charge)
                .is_none(),
            "the interrupted slot stays held while the node runs on"
        );
        assert!(
            fault.is_some(),
            "a timed-out writer on a deleted owner must stop the node"
        );
    }

    #[compio::test]
    async fn given_scheduled_transfer_when_repair_catches_up_should_cancel_only_caught_up_rearms() {
        for (commit_max, repair_finished) in [(0, false), (0, true), (1, false)] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            let namespace = IggyNamespace::new(0, 0, 0);
            let consensus = VsrConsensus::new(
                1,
                1,
                3,
                namespace.inner(),
                bus.clone(),
                LocalPipeline::new(),
            );
            consensus.init();
            consensus.advance_commit_max(commit_max);
            let mut partition = IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(SEGMENT_BYTES),
            );
            partition.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
                peer: 0,
                after_ticks: u32::from(repair_finished),
            });
            partition.repair = repair_finished.then_some(partitions::RepairSession {
                nonce: 1,
                view: 0,
                from_op: 1,
                commit_to_op: commit_max,
                fetch_to_op: commit_max,
                floor: None,
                peer: 0,
                idle_ticks: 0,
            });
            owner.plane.partitions().insert(namespace, partition);

            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());

            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            assert_eq!(partition.consensus().is_transferring(), commit_max > 0);
            assert_eq!(partition.transfer.is_some(), commit_max > 0);
            assert!(partition.transfer_rearm.is_none());
        }
    }

    /// Above a committed prefix, the walk mark that the first stall round
    /// records is no progress for the rounds after it.
    #[compio::test]
    async fn given_silent_peer_above_committed_prefix_when_budget_is_spent_should_rotate_session() {
        const COMMIT_MIN: u64 = 5;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let namespace = IggyNamespace::new(0, 0, 0);
        owner.plane.partitions().insert(
            namespace,
            repairing_backup(&bus, namespace, COMMIT_MIN, COMMIT_MIN + 3),
        );

        let mut peers = Vec::new();
        for _ in 0..=REPAIR_MAX_STALL_RETRIES {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            peers.push(partition.repair.map(|session| session.peer));
        }

        let mut expected = vec![Some(REPAIR_PRIMARY); REPAIR_MAX_STALL_RETRIES as usize];
        expected.push(Some(REPAIR_NEXT_PEER));
        assert_eq!(
            peers, expected,
            "only the round past the budget re-arms from the next replica"
        );
    }

    /// A deferred view change pauses the journal until history writes drain,
    /// so the walk cannot commit the op this backup holds next. The primary
    /// answers every re-request with one chunk from that op, here one op long,
    /// and the journal holds all of it. The walk owns progress then, and a
    /// rotation cannot help.
    #[compio::test]
    async fn given_paused_walk_with_resident_next_op_when_stall_rounds_pass_should_keep_peer() {
        const COMMIT_MAX: u64 = 2;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let partitions = owner.plane.partitions();
        let namespace = IggyNamespace::new(0, 0, 0);
        let mut primary = VsrConsensus::new(
            1,
            REPAIR_PRIMARY,
            3,
            namespace.inner(),
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        primary.set_view(REPAIR_VIEW);
        primary.set_log_view(REPAIR_VIEW);
        primary.init();
        let mut origin: IggyPartition<_, HeldSuperblock> = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            primary,
            IggyByteSize::from(SEGMENT_BYTES),
        );
        origin.on_request(send_request(namespace, 1), None).await;
        let frozen = origin.log.journal().inner.repair_entry(1).unwrap();
        let served = || {
            let mut prepare = Message::<PrepareHeader>::new(frozen.len());
            prepare.as_mut_slice().copy_from_slice(frozen.as_slice());
            prepare
        };
        let mut backup = repairing_backup(&bus, namespace, 0, COMMIT_MAX);
        backup.apply_repaired_prepare(served()).await;
        assert!(backup.log.journal().inner.holds_op(1));
        backup.defer_view_transition(view_control_message(
            namespace,
            Command::StartViewChange,
            REPAIR_VIEW + 1,
            REPAIR_NEXT_PEER,
        ));
        partitions.insert(namespace, backup);

        for round in 0..=REPAIR_MAX_STALL_RETRIES + 1 {
            partitions
                .get_mut_by_ns(&namespace)
                .unwrap()
                .apply_repaired_prepare(served())
                .await;
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let partition = partitions.get_by_ns(&namespace).unwrap();
            assert_eq!(partition.consensus().commit_min(), 0);
            assert_eq!(
                partition.repair.map(|session| session.peer),
                Some(REPAIR_PRIMARY),
                "round {round} rotated a served session away while its own walk was paused"
            );
        }
    }

    /// At its commit point the backup has no walk to wait for, even when it
    /// holds the next op as the head of an uncommitted suffix. A silent
    /// primary that never sends the rest must still spend the budget.
    #[compio::test]
    async fn given_held_suffix_head_above_commit_point_when_peer_stays_silent_should_leave_peer() {
        const FETCH_TO_OP: u64 = 2;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let partitions = owner.plane.partitions();
        let namespace = IggyNamespace::new(0, 0, 0);
        let mut primary = VsrConsensus::new(
            1,
            REPAIR_PRIMARY,
            3,
            namespace.inner(),
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        primary.set_view(REPAIR_VIEW);
        primary.set_log_view(REPAIR_VIEW);
        primary.init();
        let mut origin: IggyPartition<_, HeldSuperblock> = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            primary,
            IggyByteSize::from(SEGMENT_BYTES),
        );
        origin.on_request(send_request(namespace, 1), None).await;
        let frozen = origin.log.journal().inner.repair_entry(1).unwrap();
        let mut served = Message::<PrepareHeader>::new(frozen.len());
        served.as_mut_slice().copy_from_slice(frozen.as_slice());
        let mut backup = repairing_backup(&bus, namespace, 0, 0);
        backup.repair = Some(RepairSession {
            fetch_to_op: FETCH_TO_OP,
            ..backup.repair.unwrap()
        });
        backup.on_replicate(served).await;
        assert!(backup.log.journal().inner.holds_op(1));
        assert_eq!(backup.consensus().commit_max(), 0);
        partitions.insert(namespace, backup);

        let mut peers = Vec::new();
        for _ in 0..=REPAIR_MAX_STALL_RETRIES {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let partition = partitions.get_by_ns(&namespace).unwrap();
            peers.push(partition.repair.map(|session| session.peer));
        }

        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        assert!(
            peers[..budget]
                .iter()
                .all(|peer| *peer == Some(REPAIR_PRIMARY)),
            "only the round past the budget may leave the primary: {peers:?}"
        );
        assert_ne!(
            peers[budget],
            Some(REPAIR_PRIMARY),
            "a held suffix head above the commit point must still spend the budget: {peers:?}"
        );
    }

    /// Each peer that the ring reaches gets the whole budget, and the
    /// rotations add up until the ring is spent. The walk steps around the
    /// ring from the primary, so in a group of five every other replica gets
    /// one session before the fourth spends the ring. Stepping back to the
    /// primary after each backup alternated between two of them.
    #[compio::test]
    async fn given_rotated_session_when_new_peer_spends_its_budget_should_rotate_again() {
        const GROUP_OF_FIVE: u8 = 5;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let namespace = IggyNamespace::new(0, 0, 0);
        owner.plane.partitions().insert(
            namespace,
            repairing_replica(
                &bus,
                namespace,
                (REPAIR_BACKUP, GROUP_OF_FIVE),
                REPAIR_PRIMARY,
                LAGGING_COMMIT_MIN,
                LAGGING_COMMIT_MAX,
            ),
        );

        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let mut expected = vec![(Some(REPAIR_PRIMARY), None); budget];
        let ring_after_primary = (1..GROUP_OF_FIVE)
            .map(|step| (REPAIR_PRIMARY + step) % GROUP_OF_FIVE)
            .filter(|&peer| peer != REPAIR_BACKUP);
        for peer in ring_after_primary {
            expected.extend(vec![(Some(peer), None); budget + 1]);
        }
        expected.push((None, Some(REPAIR_PRIMARY)));
        let observed = stall_rounds(&owner, namespace, expected.len()).await;

        assert_eq!(
            observed, expected,
            "each other replica must spend a whole budget until the fourth session spends the ring"
        );
    }

    /// Once the sessions that spent their budget reach the number of other
    /// replicas, no peer is left to try. A backup that still misses committed
    /// ops then transfers from the primary, the only replica that serves one,
    /// and not one round earlier. A group of two spends its ring at the first
    /// exhaustion.
    #[compio::test]
    async fn given_silent_peers_when_backup_spends_the_ring_should_transfer_from_primary() {
        let transfer = (None, Some(REPAIR_PRIMARY));
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let mut next_peer_rounds = vec![(Some(REPAIR_NEXT_PEER), None); budget + 1];
        next_peer_rounds.push(transfer);
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for (backup, replica_count, after_budget) in [
            (PAIR_BACKUP, 2, vec![transfer]),
            (REPAIR_BACKUP, 3, next_peer_rounds),
        ] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            owner.set_repair_retry_ticks(1);
            let namespace = IggyNamespace::new(0, 0, 0);
            owner.plane.partitions().insert(
                namespace,
                repairing_replica(
                    &bus,
                    namespace,
                    (backup, replica_count),
                    REPAIR_PRIMARY,
                    LAGGING_COMMIT_MIN,
                    LAGGING_COMMIT_MAX,
                ),
            );

            let mut rounds = vec![(Some(REPAIR_PRIMARY), None); budget];
            rounds.extend(after_budget);
            let stalls = stall_rounds(&owner, namespace, rounds.len()).await;
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            observed.push((
                replica_count,
                stalls,
                partition.consensus().state_transfer_stage(),
            ));
            expected.push((replica_count, rounds, StateTransferStage::AwaitingTarget));
        }

        assert_eq!(
            observed, expected,
            "per group size: (repair peer, transfer peer) per round, then the stage"
        );
    }

    /// Progress in the middle of the ring restarts the rotations with the
    /// budget, so the ring is not spent: once the peer the ring reached spends
    /// its budget again, the session rotates instead of converting. A new
    /// frame above a hole is progress, so is a frame that a pause keeps out
    /// of the journal, and so is a walk over an op that arrived on the live
    /// path. The walk lands in the round after it, so that round charges
    /// nothing.
    #[compio::test]
    async fn given_progress_in_the_middle_of_the_ring_when_rounds_resume_should_restart_budget() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Progress {
            Frame,
            PausedFrame,
            Walk,
        }
        // Op 3 never arrives, so no kind of progress closes the window.
        const COMMIT_MAX: u64 = 3;
        for progress in [Progress::Frame, Progress::PausedFrame, Progress::Walk] {
            let walk = progress == Progress::Walk;
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            owner.set_repair_retry_ticks(1);
            let partitions = owner.plane.partitions();
            let namespace = IggyNamespace::new(0, 0, 0);
            let mut prepares = primary_prepares(&bus, namespace, 2).await;
            let above_hole = prepares.pop().unwrap();
            let next_op = prepares.pop().unwrap();
            partitions.insert(namespace, repairing_backup(&bus, namespace, 0, COMMIT_MAX));
            let budget = REPAIR_MAX_STALL_RETRIES as usize;

            let mut observed = stall_rounds(&owner, namespace, budget + 1).await;
            let backup = partitions.get_mut_by_ns(&namespace).unwrap();
            match progress {
                Progress::Frame => {
                    backup.apply_repaired_prepare(above_hole).await;
                }
                // A transfer stage is one of the pauses.
                Progress::PausedFrame => {
                    let op = above_hole.header().op;
                    backup.consensus().begin_state_transfer_await();
                    backup.apply_repaired_prepare(above_hole).await;
                    backup
                        .consensus()
                        .set_state_transfer_stage(StateTransferStage::Idle);
                    assert!(!backup.log.journal().inner.holds_op(op));
                }
                Progress::Walk => backup.on_replicate(next_op).await,
            }
            let rounds_to_exhaustion = budget + 1 + usize::from(walk);
            observed.extend(stall_rounds(&owner, namespace, rounds_to_exhaustion).await);

            let mut expected = vec![(Some(REPAIR_PRIMARY), None); budget];
            expected.extend(vec![(Some(REPAIR_NEXT_PEER), None); rounds_to_exhaustion]);
            expected.push((Some(REPAIR_PRIMARY), None));
            assert_eq!(
                observed, expected,
                "{progress:?} in the middle of the ring must restart the budget and the rotations"
            );
            assert_eq!(
                partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                u64::from(walk)
            );
        }
    }

    /// A spent ring that cannot convert keeps rotating at the pace of the
    /// budget, one rotation per spent budget. A backup with a scheduled re-arm
    /// waits for it, and a primary has no primary to transfer from.
    #[compio::test]
    async fn given_ring_that_cannot_convert_when_ring_is_spent_should_rotate_once_per_budget() {
        const REARM_BACKOFF_TICKS: u32 = 1024;
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        for (case, replica, peer) in [
            ("a backup waiting to re-arm", REPAIR_BACKUP, REPAIR_PRIMARY),
            ("a primary", REPAIR_PRIMARY, REPAIR_BACKUP),
        ] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            owner.set_repair_retry_ticks(1);
            let namespace = IggyNamespace::new(0, 0, 0);
            let mut partition = repairing_replica(
                &bus,
                namespace,
                (replica, 3),
                peer,
                LAGGING_COMMIT_MIN,
                LAGGING_COMMIT_MAX,
            );
            if replica != REPAIR_PRIMARY {
                partition.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
                    peer: REPAIR_PRIMARY,
                    after_ticks: REARM_BACKOFF_TICKS,
                });
            }
            owner.plane.partitions().insert(namespace, partition);

            let mut expected = vec![(Some(peer), None); budget];
            expected.extend(vec![(Some(REPAIR_NEXT_PEER), None); budget + 1]);
            expected.push((Some(peer), None));
            let observed = stall_rounds(&owner, namespace, expected.len()).await;

            assert_eq!(
                observed, expected,
                "{case} must rotate once per spent budget and never arm a transfer"
            );
        }
    }

    /// A spent ring rotates as before, here dropping the session of a group
    /// of two, when a transfer cannot take over. A scheduled re-arm keeps its
    /// backoff, a busy stage belongs to the transfer in flight, a session can
    /// outlive its stage when an election clears the stage, and a transfer
    /// carries no uncommitted suffix.
    #[compio::test]
    async fn given_transfer_that_cannot_take_over_when_ring_is_spent_should_rotate_instead_of_arming()
     {
        const REARM_BACKOFF_TICKS: u32 = 1024;
        const TRANSFER_NONCE: u128 = 7;
        const SUFFIX_OPS: u64 = 2;
        type Setup = fn(&mut IggyPartition<Rc<IggyMessageBus>, HeldSuperblock>);
        let cases: [(&str, u64, Setup); 4] = [
            ("an uncommitted suffix", LAGGING_COMMIT_MIN, |partition| {
                partition.repair = partition.repair.map(|session| RepairSession {
                    fetch_to_op: session.commit_to_op + SUFFIX_OPS,
                    ..session
                });
            }),
            ("a scheduled re-arm", LAGGING_COMMIT_MAX, |partition| {
                partition.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
                    peer: REPAIR_PRIMARY,
                    after_ticks: REARM_BACKOFF_TICKS,
                });
            }),
            ("a transfer stage", LAGGING_COMMIT_MAX, |partition| {
                partition.consensus().begin_state_transfer_await();
            }),
            ("a transfer session", LAGGING_COMMIT_MAX, |partition| {
                partition.transfer = Some(PartitionTransferSession {
                    nonce: TRANSFER_NONCE,
                    peer: REPAIR_PRIMARY,
                    commit_op: 0,
                    artifacts: Vec::new(),
                    target_accepted: false,
                    idle_ticks: 0,
                });
            }),
        ];
        let transfer_state = |partition: &IggyPartition<Rc<IggyMessageBus>, HeldSuperblock>| {
            (
                partition.transfer.as_ref().map(|session| session.nonce),
                partition.consensus().state_transfer_stage(),
                partition.transfer_rearm.is_some(),
            )
        };
        for (case, commit_max, setup) in cases {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            owner.set_repair_retry_ticks(1);
            let namespace = IggyNamespace::new(0, 0, 0);
            let mut backup = repairing_replica(
                &bus,
                namespace,
                (PAIR_BACKUP, 2),
                REPAIR_PRIMARY,
                LAGGING_COMMIT_MIN,
                commit_max,
            );
            setup(&mut backup);
            let before = transfer_state(&backup);
            owner.plane.partitions().insert(namespace, backup);

            let budget = REPAIR_MAX_STALL_RETRIES as usize;
            let observed = stall_rounds(&owner, namespace, budget + 1).await;

            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            let mut expected = vec![Some(REPAIR_PRIMARY); budget];
            expected.push(None);
            assert_eq!(
                (
                    observed
                        .iter()
                        .map(|(repair, _)| *repair)
                        .collect::<Vec<_>>(),
                    transfer_state(partition),
                ),
                (expected, before),
                "{case} must keep the spent ring from arming a transfer"
            );
        }
    }

    /// One sweep spends the ring of more groups than the shard has transfer
    /// slots. The first groups take the slots, and every other group defers:
    /// it keeps its repair session, schedules a re-arm and leaves the stage
    /// idle. The next round re-requests from the same peer.
    #[compio::test]
    async fn given_more_spent_rings_than_transfer_slots_when_one_sweep_spends_them_should_defer_the_rest()
     {
        const LAGGING_GROUPS: usize = 5;
        let cap = <IoTestShard>::PARTITION_TRANSFERS_INFLIGHT_MAX;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let partitions = owner.plane.partitions();
        let namespaces: Vec<_> = (0..LAGGING_GROUPS)
            .map(|partition_id| IggyNamespace::new(0, 0, partition_id))
            .collect();
        for &namespace in &namespaces {
            partitions.insert(
                namespace,
                repairing_replica(
                    &bus,
                    namespace,
                    (PAIR_BACKUP, 2),
                    REPAIR_PRIMARY,
                    LAGGING_COMMIT_MIN,
                    LAGGING_COMMIT_MAX,
                ),
            );
        }
        let recovery = |namespace: &IggyNamespace| {
            let partition = partitions.get_by_ns(namespace).unwrap();
            (
                partition.transfer.as_ref().map(|session| session.peer),
                partition.transfer_rearm.is_some(),
                partition.repair.map(|session| session.peer),
                partition.consensus().state_transfer_stage(),
            )
        };
        let armed = (
            Some(REPAIR_PRIMARY),
            false,
            None,
            StateTransferStage::AwaitingTarget,
        );
        let deferred = (None, true, Some(REPAIR_PRIMARY), StateTransferStage::Idle);

        for _ in 0..=REPAIR_MAX_STALL_RETRIES {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
        }
        let spent: Vec<_> = namespaces.iter().map(recovery).collect();
        let deferred_groups: Vec<_> = namespaces
            .iter()
            .filter(|namespace| recovery(namespace) == deferred)
            .collect();
        assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());

        assert_eq!(
            (
                spent.iter().filter(|&&state| state == armed).count(),
                deferred_groups.len(),
                deferred_groups
                    .iter()
                    .all(|namespace| recovery(namespace) == deferred),
            ),
            (cap, LAGGING_GROUPS - cap, true),
            "(armed, deferred, deferred groups unchanged a round later) per group: {spent:?}"
        );
    }

    /// A session that the ring drops without a successor leaves the gap to
    /// the detector, which re-arms the same peer after its debounce. That
    /// session gets the whole budget too, so a peer that answers slower than
    /// one retry interval still gets its answer in.
    #[compio::test]
    async fn given_session_dropped_without_successor_when_detector_re_arms_should_keep_whole_budget()
     {
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let namespace = IggyNamespace::new(0, 0, 0);
        owner.plane.partitions().insert(
            namespace,
            repairing_replica(
                &bus,
                namespace,
                (REPAIR_PRIMARY, 2),
                PAIR_BACKUP,
                LAGGING_COMMIT_MIN,
                LAGGING_COMMIT_MAX,
            ),
        );

        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let session = (Some(PAIR_BACKUP), None);
        let mut expected = vec![session; budget];
        expected.extend(vec![(None, None); REPAIR_GAP_DEBOUNCE_TICKS_MIN as usize]);
        expected.extend(vec![session; budget + 1]);
        expected.push((None, None));
        let observed = stall_rounds(&owner, namespace, expected.len()).await;

        assert_eq!(
            observed, expected,
            "the session that the detector re-arms must get the whole budget"
        );
    }

    /// A scheduled re-arm counts down in every round that does not rotate.
    /// With a retry interval of one tick, a spent ring that cannot convert
    /// still rotates only once per budget, so the re-arm fires and arms the
    /// transfer.
    #[compio::test]
    async fn given_scheduled_re_arm_when_stall_rounds_run_every_tick_should_arm_transfer() {
        const REARM_BACKOFF_TICKS: u32 = 20;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let namespace = IggyNamespace::new(0, 0, 0);
        let mut backup = repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
        backup.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
            peer: REPAIR_PRIMARY,
            after_ticks: REARM_BACKOFF_TICKS,
        });
        owner.plane.partitions().insert(namespace, backup);

        let observed = stall_rounds(&owner, namespace, 2 * REARM_BACKOFF_TICKS as usize).await;

        assert!(
            observed.contains(&(None, Some(REPAIR_PRIMARY))),
            "the re-arm must fire within twice its backoff: {observed:?}"
        );
    }

    /// A rotation that the live-session ceiling refuses opens no session, so
    /// it does not count toward the ring. The detector re-arms the primary
    /// later, and the ring still reaches the other backup before it converts.
    #[compio::test]
    async fn given_rotation_refused_at_session_ceiling_when_ring_resumes_should_ask_next_peer_before_transfer()
     {
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let namespace = IggyNamespace::new(0, 0, 0);
        owner.plane.partitions().insert(
            namespace,
            repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX),
        );
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let mut expected = vec![(Some(REPAIR_PRIMARY), None); budget];
        expected.extend(vec![(None, None); REPAIR_GAP_DEBOUNCE_TICKS_MIN as usize]);
        expected.extend(vec![(Some(REPAIR_PRIMARY), None); budget + 1]);
        expected.extend(vec![(Some(REPAIR_NEXT_PEER), None); budget + 1]);
        expected.push((None, Some(REPAIR_PRIMARY)));

        let mut observed = stall_rounds(&owner, namespace, budget).await;
        // Other sessions fill the ceiling, for the sweep of the rotation only:
        // the rotation hands the slot of its dropped session to the successor,
        // and each sweep republishes the tally.
        owner
            .partition_repairs_inflight
            .set(PARTITION_REPAIRS_INFLIGHT_MAX + 1);
        observed.extend(stall_rounds(&owner, namespace, expected.len() - budget).await);

        assert_eq!(
            observed, expected,
            "a rotation refused at the ceiling must not spend the ring"
        );
    }

    /// Other groups hold every other session slot across sweeps, so the
    /// shard stays at its live-session ceiling while this backup holds a
    /// session. Each rotation hands the slot of the session it drops to its
    /// successor, so the ring reaches the other backup and converts at the
    /// pace of a shard below its ceiling.
    #[compio::test]
    async fn given_sustained_session_ceiling_when_backup_spends_the_ring_should_ask_next_peer_then_transfer()
     {
        const OTHER_SESSIONS: usize = PARTITION_REPAIRS_INFLIGHT_MAX - 1;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let partitions = owner.plane.partitions();
        let namespace = IggyNamespace::new(0, 0, 0);
        partitions.insert(
            namespace,
            repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX),
        );
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let mut expected = vec![(Some(REPAIR_PRIMARY), None); budget];
        expected.extend(vec![(Some(REPAIR_NEXT_PEER), None); budget + 1]);
        expected.push((None, Some(REPAIR_PRIMARY)));

        let mut observed = Vec::with_capacity(expected.len());
        for _ in 0..expected.len() {
            let holds_session = partitions.get_by_ns(&namespace).unwrap().repair.is_some();
            owner
                .partition_repairs_inflight
                .set(OTHER_SESSIONS + usize::from(holds_session));
            observed.extend(stall_rounds(&owner, namespace, 1).await);
        }

        assert_eq!(
            observed, expected,
            "a ceiling that holds across sweeps must not stop the ring"
        );
    }

    /// Every group that holds a session spends its budget in the same sweep,
    /// while one more group waits at the live-session ceiling. Each rotation
    /// keeps the slot of the session it drops, and the sweep counts the
    /// session it opens, so the waiting group does not arm past the ceiling
    /// in the next sweep.
    #[compio::test]
    async fn given_sessions_rotating_at_ceiling_when_waiting_group_is_due_should_not_arm_past_it() {
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let partitions = owner.plane.partitions();
        let namespaces: Vec<_> = (0..=PARTITION_REPAIRS_INFLIGHT_MAX)
            .map(|partition_id| IggyNamespace::new(0, 0, partition_id))
            .collect();
        for &namespace in &namespaces {
            partitions.insert(
                namespace,
                repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX),
            );
        }
        let waiting = partitions
            .get_mut_by_ns(&namespaces[PARTITION_REPAIRS_INFLIGHT_MAX])
            .unwrap();
        waiting.repair = None;
        // Past its debounce, so only the ceiling holds it back.
        waiting.gap_ticks.set(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        // The tally that arming the inserted sessions would have published.
        owner
            .partition_repairs_inflight
            .set(PARTITION_REPAIRS_INFLIGHT_MAX);
        let session_peers = || {
            namespaces
                .iter()
                .filter_map(|namespace| partitions.get_by_ns(namespace).unwrap().repair)
                .map(|session| session.peer)
                .collect::<Vec<_>>()
        };
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let mut expected = vec![vec![REPAIR_PRIMARY; PARTITION_REPAIRS_INFLIGHT_MAX]; budget];
        // The sweep of the rotations, then the sweep that reads its recount.
        expected.extend(vec![
            vec![REPAIR_NEXT_PEER; PARTITION_REPAIRS_INFLIGHT_MAX];
            2
        ]);

        let mut observed = Vec::with_capacity(expected.len());
        for _ in 0..expected.len() {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            observed.push(session_peers());
        }

        assert_eq!(
            observed, expected,
            "the peer of each live session per round, the waiting group last"
        );
    }

    #[compio::test]
    async fn given_spent_rings_at_ceiling_when_transfers_cannot_arm_should_admit_waiting_group() {
        const REARM_BACKOFF_TICKS: u32 = 1024;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let partitions = owner.plane.partitions();
        let budget = REPAIR_MAX_STALL_RETRIES as usize + 1;
        let namespaces: Vec<_> = (0..=PARTITION_REPAIRS_INFLIGHT_MAX)
            .map(|partition_id| IggyNamespace::new(0, 0, partition_id))
            .collect();
        for (index, &namespace) in namespaces.iter().enumerate() {
            let mut backup =
                repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
            if index < PARTITION_REPAIRS_INFLIGHT_MAX {
                for _ in 0..index % budget {
                    assert!(!backup.burn_repair_attempt());
                }
                backup.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
                    peer: REPAIR_PRIMARY,
                    after_ticks: REARM_BACKOFF_TICKS,
                });
            } else {
                backup.repair = None;
                backup.gap_ticks.set(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
            }
            partitions.insert(namespace, backup);
        }
        owner
            .partition_repairs_inflight
            .set(PARTITION_REPAIRS_INFLIGHT_MAX);
        let waiting = namespaces[PARTITION_REPAIRS_INFLIGHT_MAX];
        let mut waiting_armed = false;
        for sweep in 1..=2 * budget + 1 {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let live = namespaces
                .iter()
                .filter(|namespace| partitions.get_by_ns(namespace).unwrap().repair.is_some())
                .count();
            assert_eq!(
                owner.partition_repairs_inflight.get(),
                live,
                "sweep {sweep}"
            );
            assert!(
                live <= PARTITION_REPAIRS_INFLIGHT_MAX,
                "sweep {sweep}: {live}"
            );
            waiting_armed |= partitions.get_by_ns(&waiting).unwrap().repair.is_some();
        }
        assert!(
            waiting_armed,
            "spent rings must release capacity for a waiting group"
        );
    }

    #[compio::test]
    async fn given_staggered_sessions_at_ceiling_when_rotating_should_preserve_each_slot() {
        const WAITING_GROUPS: usize = 4;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let partitions = owner.plane.partitions();
        let budget = REPAIR_MAX_STALL_RETRIES as usize + 1;
        let namespaces: Vec<_> = (0..PARTITION_REPAIRS_INFLIGHT_MAX + WAITING_GROUPS)
            .map(|partition_id| IggyNamespace::new(0, 0, partition_id))
            .collect();
        for (index, &namespace) in namespaces.iter().enumerate() {
            let mut backup =
                repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
            if index < PARTITION_REPAIRS_INFLIGHT_MAX {
                for _ in 0..index % budget {
                    assert!(!backup.burn_repair_attempt());
                }
            } else {
                backup.repair = None;
                backup.gap_ticks.set(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
            }
            partitions.insert(namespace, backup);
        }
        owner
            .partition_repairs_inflight
            .set(PARTITION_REPAIRS_INFLIGHT_MAX);
        for sweep in 1..=budget {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let live = namespaces
                .iter()
                .filter(|namespace| partitions.get_by_ns(namespace).unwrap().repair.is_some())
                .count();
            assert_eq!(live, PARTITION_REPAIRS_INFLIGHT_MAX, "sweep {sweep}");
            assert_eq!(
                owner.partition_repairs_inflight.get(),
                live,
                "sweep {sweep}"
            );
            for (index, namespace) in namespaces.iter().enumerate() {
                let expected = if index >= PARTITION_REPAIRS_INFLIGHT_MAX {
                    None
                } else if sweep >= budget - index % budget {
                    Some(REPAIR_NEXT_PEER)
                } else {
                    Some(REPAIR_PRIMARY)
                };
                let peer = partitions
                    .get_by_ns(namespace)
                    .unwrap()
                    .repair
                    .map(|session| session.peer);
                assert_eq!(peer, expected, "sweep {sweep}, group {index}");
            }
        }
    }

    #[compio::test]
    async fn given_transfer_abandoned_at_ceiling_when_repair_rearms_should_count_new_session() {
        const OTHER_SESSIONS: usize = PARTITION_REPAIRS_INFLIGHT_MAX - 1;
        const SWEEPS: usize = 2;
        for retry_ticks in [1, partitions::REPAIR_RETRY_TICKS] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            owner.set_repair_retry_ticks(retry_ticks);
            owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
            let partitions = owner.plane.partitions();
            let namespaces: Vec<_> = (0..=PARTITION_REPAIRS_INFLIGHT_MAX)
                .map(|partition_id| IggyNamespace::new(0, 0, partition_id))
                .collect();
            for &namespace in &namespaces[..OTHER_SESSIONS] {
                partitions.insert(
                    namespace,
                    repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX),
                );
            }
            let transferring = namespaces[OTHER_SESSIONS];
            let mut backup =
                repairing_backup(&bus, transferring, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
            backup.consensus().begin_state_transfer_await();
            assert!(
                owner
                    .arm_partition_transfer(&mut backup, REPAIR_PRIMARY, 0)
                    .await
            );
            for _ in 0..consensus::STATE_TRANSFER_MAX_STALL_RETRIES {
                assert!(!backup.burn_transfer_attempt());
            }
            backup.transfer.as_mut().unwrap().idle_ticks = retry_ticks - 1;
            partitions.insert(transferring, backup);
            let waiting = namespaces[PARTITION_REPAIRS_INFLIGHT_MAX];
            let mut late = repairing_backup(&bus, waiting, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
            late.repair = None;
            late.gap_ticks.set(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
            partitions.insert(waiting, late);
            owner.partition_repairs_inflight.set(OTHER_SESSIONS);

            for sweep in 1..=SWEEPS {
                assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
                let live = namespaces
                    .iter()
                    .filter(|namespace| partitions.get_by_ns(namespace).unwrap().repair.is_some())
                    .count();
                assert_eq!(
                    live, PARTITION_REPAIRS_INFLIGHT_MAX,
                    "interval {retry_ticks}, sweep {sweep}"
                );
                assert_eq!(
                    owner.partition_repairs_inflight.get(),
                    live,
                    "interval {retry_ticks}, sweep {sweep}"
                );
                assert!(
                    partitions
                        .get_by_ns(&transferring)
                        .unwrap()
                        .repair
                        .is_some()
                );
                assert!(partitions.get_by_ns(&waiting).unwrap().repair.is_none());
            }
        }
    }

    #[compio::test]
    async fn given_partly_spent_repair_when_transfer_supersedes_it_should_restart_fallback_budget()
    {
        const REARM_BACKOFF_TICKS: u32 = 2 * (REPAIR_MAX_STALL_RETRIES + 1);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_retry_ticks(1);
        let namespace = IggyNamespace::new(0, 0, 0);
        let mut backup = repairing_backup(&bus, namespace, LAGGING_COMMIT_MIN, LAGGING_COMMIT_MAX);
        for _ in 0..REPAIR_MAX_STALL_RETRIES {
            assert!(!backup.burn_repair_attempt());
        }
        backup.note_repair_rotation();
        backup.repair.as_mut().unwrap().peer = REPAIR_NEXT_PEER;
        backup.consensus().begin_state_transfer_await();

        assert!(
            owner
                .arm_partition_transfer(&mut backup, REPAIR_PRIMARY, 0)
                .await
        );
        assert!(backup.repair.is_none());
        assert_eq!(backup.repair_rotations(), 0);
        owner
            .abandon_or_rearm_partition_transfer(&mut backup, REPAIR_PRIMARY)
            .await;
        backup.transfer_rearm.as_mut().unwrap().after_ticks = REARM_BACKOFF_TICKS;
        owner.plane.partitions().insert(namespace, backup);
        let budget = REPAIR_MAX_STALL_RETRIES as usize;
        let observed = stall_rounds(&owner, namespace, budget + 1).await;
        let mut expected = vec![(Some(REPAIR_PRIMARY), None); budget];
        expected.push((Some(REPAIR_NEXT_PEER), None));
        assert_eq!(
            observed, expected,
            "fallback repair must get the whole stall budget"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn capture_first_write(
        owner: &IoTestShard,
    ) -> (usize, partitions::CapturedPartitionIo<HeldSuperblock>) {
        let lane = &owner.partition_io;
        let partitions = owner.plane.partitions();
        partitions.set_io_notifier(lane.notifier(), lane.limits.bytes_max());
        let namespace = IggyNamespace::new(0, 0, 0);
        let (reply, result) = consensus::oneshot_channel();
        partitions
            .on_request_with_reply(send_request(namespace, 1), Some(reply))
            .await;
        drop(result);
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
            match partition.resume_io(partitions.config()).await {
                partitions::PartitionIoStep::Ready(plan) => {
                    let slot = lane
                        .try_reserve(namespace, partition.incarnation(), plan.allocation_charge)
                        .unwrap();
                    let captured = partition
                        .capture_io(plan, partitions.config())
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        captured.identity.continuation,
                        partitions::PartitionIoContinuation::Superblock
                    );
                    return (slot, captured);
                }
                partitions::PartitionIoStep::Progress => {}
                _ => panic!("first send must reach its reservation write"),
            }
        }
        panic!("reservation did not reach file execution");
    }

    type IoTestMetadata = MuxStateMachine<variadic!(Users, Streams)>;
    type IoTestShard<B = Rc<IggyMessageBus>> =
        IggyShard<B, PrepareJournal, (), IoTestMetadata, PapayaShardsTable, HeldSuperblock>;

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_delayed_old_repair_when_a_probe_announces_an_appending_head_should_not_commit_it()
     {
        const REPLICAS: u8 = 5;
        const OLD_PRIMARY: usize = 0;
        const PRIMARY: usize = 1;
        const BACKUP: usize = 4;
        const OLD_CLIENT: u128 = 11;
        const CLIENT: u128 = 22;
        const ROOT_OP: u64 = 1;
        const REGISTER_OP: u64 = ROOT_OP + 1;
        const STEP_LIMIT: usize = 10_000;
        let root_slot = usize::try_from(ROOT_OP).unwrap();
        let register_slot = usize::try_from(REGISTER_OP).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let frames: Vec<MetadataFrames> = (0..REPLICAS)
            .map(|_| Rc::new(RefCell::new(Vec::new())))
            .collect();
        let mut owners = Vec::new();
        for replica in 0..REPLICAS {
            let bus = Rc::new(IggyMessageBus::new(0));
            let captured = Rc::clone(&frames[usize::from(replica)]);
            bus.set_replica_forward_fn(Box::new(move |target, _, frame| {
                let message =
                    Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                        .unwrap();
                captured.borrow_mut().push((target, message));
                Ok(())
            }));
            for peer in 0..REPLICAS {
                if peer != replica {
                    assert!(bus.owner_table().try_claim(peer, 1));
                }
            }
            let (mut owner, inbox_owner) = test_owner(&bus, None);
            let consensus = VsrConsensus::new(
                1,
                replica,
                REPLICAS,
                METADATA_GROUP,
                Rc::clone(&bus),
                LocalPipeline::new(),
            );
            consensus.set_incarnation(u128::from(replica) + 1);
            consensus.init();
            consensus.mark_superblock_durable(0, 0);
            let journal =
                PrepareJournal::open(&directory.path().join(format!("metadata-{replica}.wal")), 0)
                    .await
                    .unwrap();
            owner.plane.inner_mut().0 = IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                IoTestMetadata::new((
                    UsersInner::new().into(),
                    (StreamsInner::default().into(), ()),
                )),
                None,
            );
            owners.push((owner, inbox_owner));
        }
        let (owners, _inbox_owners): (Vec<_>, Vec<_>) = owners.into_iter().unzip();
        let metadata = |replica: usize| &owners[replica].plane.inner().0;
        let consensus = |replica| metadata(replica).consensus.as_ref().unwrap();
        let journal = |replica| metadata(replica).journal.as_ref().unwrap();
        let take = |source: usize, target: usize, command: Command| {
            let mut pending = frames[source].borrow_mut();
            let index = pending
                .iter()
                .position(|(replica, message)| {
                    usize::from(*replica) == target && message.header().command == command
                })
                .unwrap_or_else(|| panic!("missing {command:?} from {source} to {target}"));
            pending.remove(index).1
        };
        let root_request = CreateUserRequest {
            username: WireName::new("root").unwrap(),
            password: "hash".to_owned(),
            status: iggy_common::UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        };
        let mut root = Box::pin(metadata(OLD_PRIMARY).submit_root_user_in_process(&root_request));
        let mut root_status = None;
        for _ in 0..STEP_LIMIT {
            if root_status.is_none()
                && let std::task::Poll::Ready(result) = futures::poll!(&mut root)
            {
                root_status = Some(result.unwrap());
            }
            deliver_metadata_frames(&owners, &frames, &[0, 1, 2, 3, 4]).await;
            for owner in &owners {
                owner.tick_metadata().await;
                owner
                    .process_loopback(&mut crate::router::LoopbackRound::default())
                    .await;
            }
            if root_status.is_some()
                && (0..usize::from(REPLICAS))
                    .all(|replica| consensus(replica).commit_min() == ROOT_OP)
            {
                break;
            }
            compio::runtime::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(root_status, Some(0));
        for replica in 0..usize::from(REPLICAS) {
            assert_eq!(consensus(replica).commit_min(), ROOT_OP);
            assert_eq!(
                journal(replica).header(root_slot).unwrap().operation,
                Operation::CreateUser
            );
        }
        for pending in &frames {
            pending.borrow_mut().clear();
        }
        let registration = RegisterRequest {
            user_created_at: journal(OLD_PRIMARY).header(root_slot).unwrap().timestamp,
            bind_verifier: [0x5a; 32],
        };
        let mut abandoned =
            Box::pin(metadata(OLD_PRIMARY).submit_register_in_process(OLD_CLIENT, 0, registration));
        for _ in 0..STEP_LIMIT {
            assert!(futures::poll!(&mut abandoned).is_pending());
            if journal(OLD_PRIMARY).header(register_slot).is_some() {
                break;
            }
            compio::runtime::time::sleep(Duration::from_millis(1)).await;
        }
        let old_header = *journal(OLD_PRIMARY).header(register_slot).unwrap();
        assert_eq!(old_header.client, OLD_CLIENT);
        assert_eq!(consensus(OLD_PRIMARY).commit_min(), ROOT_OP);
        frames[OLD_PRIMARY].borrow_mut().clear();

        consensus(BACKUP).init_as_backup();
        consensus(BACKUP).begin_view_probe();
        for _ in 0..STEP_LIMIT {
            owners[BACKUP].tick_metadata().await;
            if frames[BACKUP]
                .borrow()
                .iter()
                .any(|(_, frame)| frame.header().command == Command::RequestStartView)
            {
                break;
            }
        }
        let probe = take(BACKUP, OLD_PRIMARY, Command::RequestStartView);
        owners[OLD_PRIMARY]
            .on_message(MessageBag::try_from(probe).unwrap())
            .await;
        let announced = take(OLD_PRIMARY, BACKUP, Command::StartView);
        owners[BACKUP]
            .on_message(MessageBag::try_from(announced).unwrap())
            .await;
        let first_request = take(BACKUP, OLD_PRIMARY, Command::RequestPrepares);
        let request = first_request
            .clone()
            .try_into_typed::<RequestPreparesHeader>()
            .unwrap();
        assert_eq!(
            (request.header().view, request.header().to_op),
            (0, REGISTER_OP)
        );
        owners[OLD_PRIMARY]
            .on_message(MessageBag::try_from(first_request).unwrap())
            .await;
        let first_body = take(OLD_PRIMARY, BACKUP, Command::RepairPrepare);
        let first_done = take(OLD_PRIMARY, BACKUP, Command::RepairDone);

        // The first response is delayed long enough for the receiver to retry.
        for _ in 0..STEP_LIMIT {
            owners[BACKUP].tick_metadata().await;
            if frames[BACKUP].borrow().iter().any(|(replica, frame)| {
                usize::from(*replica) == OLD_PRIMARY
                    && frame.header().command == Command::RequestPrepares
            }) {
                break;
            }
        }
        let retry = take(BACKUP, OLD_PRIMARY, Command::RequestPrepares);
        owners[OLD_PRIMARY]
            .on_message(MessageBag::try_from(retry).unwrap())
            .await;
        let delayed_body = take(OLD_PRIMARY, BACKUP, Command::RepairPrepare);
        owners[BACKUP]
            .on_message(MessageBag::try_from(first_body).unwrap())
            .await;
        owners[BACKUP]
            .on_message(MessageBag::try_from(first_done).unwrap())
            .await;
        assert_eq!(
            journal(BACKUP).header(register_slot).unwrap().checksum,
            old_header.checksum
        );
        assert!(!consensus(BACKUP).view_log_unproven());
        for pending in &frames {
            pending.borrow_mut().clear();
        }

        // Only the three replicas without X communicate during this election.
        for _ in 0..STEP_LIMIT {
            for owner in &owners[1..=3] {
                owner.tick_metadata().await;
            }
            deliver_metadata_frames(&owners, &frames, &[1, 2, 3]).await;
            if (1..=3)
                .all(|replica| consensus(replica).view() == 1 && consensus(replica).is_normal())
            {
                break;
            }
        }
        assert_eq!(consensus(PRIMARY).view(), 1);
        assert!(consensus(PRIMARY).is_normal());
        assert_eq!(consensus(PRIMARY).sequencer().current_sequence(), ROOT_OP);
        for pending in &frames {
            pending.borrow_mut().clear();
        }

        for _ in 0..STEP_LIMIT {
            owners[PRIMARY].tick_metadata().await;
            if frames[PRIMARY].borrow().iter().any(|(replica, frame)| {
                usize::from(*replica) == BACKUP && frame.header().command == Command::Commit
            }) {
                break;
            }
        }
        let heartbeat = take(PRIMARY, BACKUP, Command::Commit);
        owners[BACKUP]
            .on_message(MessageBag::try_from(heartbeat).unwrap())
            .await;
        for _ in 0..STEP_LIMIT {
            owners[BACKUP].tick_metadata().await;
            if frames[BACKUP].borrow().iter().any(|(replica, frame)| {
                usize::from(*replica) == PRIMARY
                    && frame.header().command == Command::RequestStartView
            }) {
                break;
            }
        }
        let probe = take(BACKUP, PRIMARY, Command::RequestStartView);
        let mut replacement =
            Box::pin(metadata(PRIMARY).submit_register_in_process(CLIENT, 0, registration));
        assert!(futures::poll!(&mut replacement).is_pending());
        assert_eq!(
            consensus(PRIMARY).sequencer().current_sequence(),
            REGISTER_OP
        );
        assert!(
            journal(PRIMARY).header(register_slot).is_none(),
            "the real WAL append is still suspended"
        );
        owners[PRIMARY]
            .on_message(MessageBag::try_from(probe).unwrap())
            .await;
        let announced = take(PRIMARY, BACKUP, Command::StartView)
            .try_into_typed::<StartViewHeader>()
            .unwrap();
        assert_eq!(
            (announced.header().view, announced.header().op),
            (1, REGISTER_OP)
        );
        let suffix = consensus::dvc_suffix_decode(announced.body(), REGISTER_OP, 0, 0).unwrap();
        assert_eq!(
            consensus::dvc_header_kind(&suffix.headers()[0]),
            consensus::DvcHeaderKind::Blank
        );
        owners[BACKUP].on_start_view(announced).await;
        assert!(consensus(BACKUP).view_log_unproven());
        assert_eq!(
            journal(BACKUP).header(register_slot).unwrap().checksum,
            old_header.checksum
        );
        for _ in 0..STEP_LIMIT {
            owners[BACKUP].tick_metadata().await;
            if owners[BACKUP]
                .metadata_repair
                .borrow()
                .is_some_and(|session| session.view == 1)
            {
                break;
            }
        }
        assert!(
            owners[BACKUP]
                .metadata_repair
                .borrow()
                .is_some_and(|session| session.view == 1)
        );
        owners[BACKUP]
            .on_message(MessageBag::try_from(delayed_body).unwrap())
            .await;
        let proved_from_old_body = !consensus(BACKUP).view_log_unproven();

        for _ in 0..STEP_LIMIT {
            if let std::task::Poll::Ready(result) = futures::poll!(&mut replacement) {
                assert_eq!(result.unwrap().epoch, REGISTER_OP);
                break;
            }
            deliver_metadata_frames(&owners, &frames, &[1, 2, 3]).await;
            owners[PRIMARY]
                .process_loopback(&mut crate::router::LoopbackRound::default())
                .await;
            compio::runtime::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(consensus(PRIMARY).commit_min(), REGISTER_OP);
        let replacement_header = *journal(PRIMARY).header(register_slot).unwrap();
        assert_eq!(replacement_header.client, CLIENT);
        assert_ne!(replacement_header.checksum, old_header.checksum);
        frames[PRIMARY]
            .borrow_mut()
            .retain(|(_, message)| message.header().command != Command::Commit);
        for _ in 0..STEP_LIMIT {
            owners[PRIMARY].tick_metadata().await;
            if frames[PRIMARY].borrow().iter().any(|(replica, frame)| {
                usize::from(*replica) == BACKUP && frame.header().command == Command::Commit
            }) {
                break;
            }
        }
        let heartbeat = take(PRIMARY, BACKUP, Command::Commit);
        owners[BACKUP]
            .on_message(MessageBag::try_from(heartbeat).unwrap())
            .await;
        assert!(
            consensus(BACKUP).commit_min() == ROOT_OP
                || journal(BACKUP).header(register_slot).unwrap().checksum
                    == replacement_header.checksum,
            "the delayed old repair must not prove and commit X under Y's view: proved={proved_from_old_body}, backup_commit={}, old_client={OLD_CLIENT}, committed_client={CLIENT}",
            consensus(BACKUP).commit_min(),
        );
        assert!(!proved_from_old_body);
        assert_eq!(
            metadata(BACKUP).client_table.borrow().get_epoch(OLD_CLIENT),
            None
        );

        for _ in 0..STEP_LIMIT {
            for owner in &owners[1..=BACKUP] {
                owner.tick_metadata().await;
                owner
                    .process_loopback(&mut crate::router::LoopbackRound::default())
                    .await;
            }
            deliver_metadata_frames(&owners, &frames, &[1, 2, 3, BACKUP]).await;
            if consensus(BACKUP).commit_min() == REGISTER_OP
                && !consensus(BACKUP).view_log_unproven()
            {
                break;
            }
            compio::runtime::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            consensus(BACKUP).commit_min(),
            REGISTER_OP,
            "repair must finish after communication resumes"
        );
        assert_eq!(
            consensus(BACKUP).view(),
            1,
            "the accepted view must repair without another election"
        );
        assert!(!consensus(BACKUP).view_log_unproven());
        assert_eq!(
            journal(BACKUP).header(register_slot).unwrap().checksum,
            replacement_header.checksum
        );
        assert_eq!(
            metadata(BACKUP).client_table.borrow().get_epoch(CLIENT),
            Some(REGISTER_OP)
        );
        assert_eq!(
            metadata(BACKUP).client_table.borrow().get_epoch(OLD_CLIENT),
            None
        );
    }

    type MetadataFrames = Rc<RefCell<Vec<(u8, Message<GenericHeader>)>>>;

    #[allow(clippy::future_not_send)]
    async fn deliver_metadata_frames(
        owners: &[IoTestShard],
        frames: &[MetadataFrames],
        connected: &[usize],
    ) {
        const FRAME_LIMIT: usize = 10_000;
        for _ in 0..FRAME_LIMIT {
            let next = connected.iter().find_map(|source| {
                let mut pending = frames[*source].borrow_mut();
                let index = pending
                    .iter()
                    .position(|(target, _)| connected.contains(&usize::from(*target)))?;
                Some(pending.remove(index))
            });
            let Some((target, message)) = next else {
                return;
            };
            owners[usize::from(target)]
                .on_message(MessageBag::try_from(message).unwrap())
                .await;
        }
        panic!("metadata handlers did not drain their bounded message burst");
    }

    #[compio::test]
    async fn given_metadata_suffix_mutation_when_stale_commit_arrives_should_publish_fresh_headers()
    {
        const VIEW: u32 = 3;
        const HEAD: u64 = 2;
        let directory = tempfile::tempdir().unwrap();
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        for replica in [1, 2] {
            assert!(bus.owner_table().try_claim(replica, 1));
        }
        let (mut owner, _sender) = test_owner(&bus, None);
        let mut consensus = VsrConsensus::new(
            1,
            0,
            3,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.set_view(VIEW);
        consensus.set_log_view(VIEW);
        consensus.init();
        consensus.mark_superblock_durable(VIEW, VIEW);
        let journal = PrepareJournal::open(&directory.path().join("metadata.wal"), 0)
            .await
            .unwrap();
        owner.plane.inner_mut().0.consensus = Some(consensus);
        owner.plane.inner_mut().0.journal = Some(journal);
        let metadata = &owner.plane.inner().0;
        let consensus = metadata.consensus.as_ref().unwrap();
        crate::refresh_metadata_dvc_suffix(consensus, metadata.journal.as_ref());
        let prepares = metadata_prepares(HEAD, VIEW);
        for prepare in &prepares {
            metadata
                .journal
                .as_ref()
                .unwrap()
                .append(prepare.clone())
                .await
                .unwrap();
            consensus.note_journal_mutation();
        }
        consensus.sequencer().set_sequence(HEAD);
        assert!(consensus.local_dvc_suffix_stale());
        let heartbeat = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
            |_, header: &mut CommitHeader| {
                header.command = Command::Commit;
                header.cluster = 1;
                header.group = METADATA_GROUP;
                header.replica = 2;
                header.view = VIEW - 1;
                header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
                header.seal();
            },
        );

        owner.on_commit(&heartbeat).await;

        assert_eq!(sent.borrow().len(), 2);
        let expected: Vec<_> = prepares
            .iter()
            .rev()
            .map(|prepare| *prepare.header())
            .collect();
        for frame in sent.borrow().iter() {
            let reply =
                Message::<StartViewHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            let suffix = consensus::dvc_suffix_decode(
                &reply.as_slice()[size_of::<StartViewHeader>()..],
                HEAD,
                0,
                0,
            )
            .unwrap();
            assert_eq!(suffix.headers(), expected);
        }
    }

    #[compio::test]
    async fn given_metadata_retransmit_below_adopted_head_when_repairing_should_keep_a_valid_range()
    {
        const LOCAL_HEAD: u64 = 4;
        const HEAD: u64 = 8;
        const VIEW: u32 = 3;
        let directory = tempfile::tempdir().unwrap();
        let bus = Rc::new(IggyMessageBus::new(0));
        let requests = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&requests);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            let message =
                Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            if message.header().command == Command::RequestPrepares {
                captured.borrow_mut().push(
                    Message::<RequestPreparesHeader>::try_from(Owned::copy_from_slice(
                        frame.as_slice(),
                    ))
                    .unwrap(),
                );
            }
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(0, 1));
        let (mut owner, _sender) = test_owner(&bus, None);
        let mut consensus = VsrConsensus::new(
            1,
            2,
            3,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.set_view(1);
        consensus.set_log_view(1);
        consensus.init();
        let journal = PrepareJournal::open(&directory.path().join("metadata.wal"), 0)
            .await
            .unwrap();
        let prepares = metadata_prepares(HEAD, 1);
        for prepare in prepares.iter().take(usize::try_from(LOCAL_HEAD).unwrap()) {
            journal.append(prepare.clone()).await.unwrap();
        }
        consensus.sequencer().set_sequence(LOCAL_HEAD);
        owner.plane.inner_mut().0.consensus = Some(consensus);
        owner.plane.inner_mut().0.journal = Some(journal);
        owner
            .on_start_view(start_view_suffix(
                IggyNamespace::from_raw(METADATA_GROUP),
                VIEW,
                HEAD,
                0,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        let metadata = &owner.plane.inner().0;
        let consensus = metadata.consensus.as_ref().unwrap();
        assert!(consensus.view_log_unproven());
        assert_eq!(consensus.view_log_repair_op(), Some(HEAD - 1));
        let retransmit = prepares[usize::try_from(LOCAL_HEAD).unwrap()]
            .clone()
            .transmute_header(|original, header: &mut PrepareHeader| {
                *header = original;
                header.view = VIEW;
                header.replica = 0;
                header.checksum = header.identity_checksum();
            });
        metadata.on_replicate(retransmit).await;
        assert_eq!(consensus.sequencer().current_sequence(), LOCAL_HEAD + 1);
        assert_eq!(consensus.commit_min(), 0);
        *owner.metadata_repair.borrow_mut() = None;
        requests.borrow_mut().clear();

        owner.maybe_request_metadata_repair(consensus, 0).await;

        let requests = requests.borrow();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            (requests[0].header().from_op, requests[0].header().to_op),
            (1, HEAD - 1)
        );
        assert_eq!(owner.metadata_repair.borrow().unwrap().to_op, HEAD);
    }

    #[compio::test]
    async fn given_a_busy_divergent_metadata_removal_when_the_fenced_op_is_repaired_should_retry_it()
     {
        const HEAD: u64 = 6;
        const DIVERGENT: u64 = HEAD - 1;
        const VIEW: u32 = 3;
        let directory = tempfile::tempdir().unwrap();
        let bus = Rc::new(IggyMessageBus::new(0));
        bus.set_replica_forward_fn(Box::new(|_, _, _| Ok(())));
        assert!(bus.owner_table().try_claim(0, 1));
        let (mut owner, _sender) = test_owner(&bus, None);
        let mut consensus = VsrConsensus::new(
            1,
            2,
            3,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.set_view(1);
        consensus.set_log_view(1);
        consensus.init();
        let journal = PrepareJournal::open(&directory.path().join("metadata.wal"), 0)
            .await
            .unwrap();
        let prepares = metadata_prepares(HEAD, 1);
        let divergent = usize::try_from(DIVERGENT - 1).unwrap();
        let stale =
            prepares[divergent]
                .clone()
                .transmute_header(|original, header: &mut PrepareHeader| {
                    *header = original;
                    header.timestamp += 1;
                    header.checksum = header.identity_checksum();
                });
        let stale_checksum = stale.header().checksum;
        for (index, prepare) in prepares.iter().enumerate() {
            let entry = if index == divergent {
                stale.clone()
            } else {
                prepare.clone()
            };
            journal.append(entry).await.unwrap();
        }
        consensus.sequencer().set_sequence(HEAD);
        owner.plane.inner_mut().0.consensus = Some(consensus);
        owner.plane.inner_mut().0.journal = Some(journal);
        let metadata = &owner.plane.inner().0;
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        let resident = || {
            journal
                .header(usize::try_from(DIVERGENT).unwrap())
                .map(|header| header.checksum)
        };

        // A checkpoint drain in flight refuses the removal as busy.
        let mut checkpoint = Box::pin(journal.drain(1..=1));
        assert!(futures::poll!(&mut checkpoint).is_pending());
        owner
            .on_start_view(start_view_suffix(
                IggyNamespace::from_raw(METADATA_GROUP),
                VIEW,
                HEAD,
                0,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        drop(checkpoint);
        assert_eq!(consensus.view_log_repair_op(), Some(DIVERGENT));
        assert_eq!(resident(), Some(stale_checksum), "the busy WAL kept it");
        assert!(owner.metadata_repair.borrow().is_some());

        let repaired = prepares[divergent].clone().transmute_header(
            |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                repaired.0 = header;
                repaired.0.command = Command::RepairPrepare;
            },
        );
        owner.on_repair_prepare(repaired).await;
        assert_ne!(
            resident(),
            Some(stale_checksum),
            "the repaired op the fence waits on never retried the removal, so every \
             later repair of it is dropped against the divergent entry"
        );
    }

    /// Removals a [`CountingJournal`] accepts before it fails every further one,
    /// so a reconcile that keeps removing nothing fails its test instead of
    /// spinning in it: the double never yields.
    const REMOVALS_MAX: usize = 64;

    /// Counts WAL rewrites the way `PrepareJournal` makes them: one per removal
    /// that drops an entry.
    #[derive(Default)]
    struct CountingJournal {
        headers: RefCell<BTreeMap<u64, PrepareHeader>>,
        rewrites: Cell<usize>,
        removals: Cell<usize>,
        fail_next_rewrite: Cell<bool>,
    }

    impl Journal for CountingJournal {
        type Header = PrepareHeader;
        type Entry = Message<PrepareHeader>;
        type HeaderRef<'a> = Ref<'a, PrepareHeader>;

        fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
            let op = u64::try_from(idx).ok()?;
            Ref::filter_map(self.headers.borrow(), |headers| headers.get(&op)).ok()
        }

        fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
            self.header(usize::try_from(header.op.checked_sub(1)?).ok()?)
        }

        fn append(&self, entry: Self::Entry) -> impl Future<Output = io::Result<()>> {
            self.headers
                .borrow_mut()
                .insert(entry.header().op, *entry.header());
            std::future::ready(Ok(()))
        }

        fn entry(&self, _header: &Self::Header) -> impl Future<Output = Option<Self::Entry>> {
            std::future::ready(None)
        }

        fn remove_range(
            &self,
            ops: RangeInclusive<u64>,
        ) -> impl Future<Output = io::Result<usize>> {
            let removals = self.removals.get() + 1;
            self.removals.set(removals);
            let removed = if removals > REMOVALS_MAX {
                Err(io::Error::other("removals past the test bound"))
            } else if self.fail_next_rewrite.replace(false) {
                Err(io::Error::other("injected WAL rewrite failure"))
            } else {
                let mut headers = self.headers.borrow_mut();
                let held = headers.len();
                headers.retain(|op, _| !ops.contains(op));
                let removed = held - headers.len();
                if removed > 0 {
                    self.rewrites.set(self.rewrites.get() + 1);
                }
                Ok(removed)
            };
            std::future::ready(removed)
        }

        fn last_op(&self) -> Option<u64> {
            self.headers.borrow().keys().next_back().copied()
        }

        fn snapshot_op(&self) -> u64 {
            0
        }

        fn set_snapshot_op(&self, _op: u64) {}
    }

    #[compio::test]
    async fn given_a_divergent_metadata_run_when_reconciling_should_rewrite_the_wal_once() {
        const VIEW: u32 = 3;
        const FIRST: u64 = 3;
        const DIVERGENT: u64 = 32;
        const LAST: u64 = FIRST + DIVERGENT - 1;
        const HEAD: u64 = LAST + 4;
        const UNKNOWN: u64 = FIRST + 9;
        let canonical: Vec<_> = metadata_prepares(HEAD, VIEW)
            .iter()
            .map(|prepare| *prepare.header())
            .collect();
        let mut parent = canonical[usize::try_from(FIRST - 2).unwrap()].checksum;
        let held: BTreeMap<_, _> = canonical
            .iter()
            .map(|header| {
                let mut header = *header;
                if (FIRST..=LAST).contains(&header.op) {
                    header.client += 1;
                    header.parent = parent;
                    header.checksum = header.identity_checksum();
                    parent = header.checksum;
                }
                (header.op, header)
            })
            .collect();

        for (rewrite_fails, unknown) in [(false, false), (true, false), (false, true)] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let mut consensus = VsrConsensus::new(
                1,
                2,
                3,
                METADATA_GROUP,
                Rc::clone(&bus),
                LocalPipeline::new(),
            );
            consensus.set_view(1);
            consensus.set_log_view(1);
            consensus.init();
            consensus.sequencer().set_sequence(HEAD);
            let suffix: Vec<_> = canonical
                .iter()
                .rev()
                .map(|header| {
                    if unknown && header.op == UNKNOWN {
                        consensus::dvc_blank(UNKNOWN)
                    } else {
                        *header
                    }
                })
                .collect();
            let view = start_view_suffix(
                IggyNamespace::from_raw(METADATA_GROUP),
                VIEW,
                HEAD,
                0,
                &suffix,
            );
            consensus.handle_start_view(consensus::PlaneKind::Metadata, view.header(), view.body());
            assert!(consensus.view_log_unproven());
            let journal = CountingJournal {
                headers: RefCell::new(held.clone()),
                ..CountingJournal::default()
            };
            if rewrite_fails {
                journal.fail_next_rewrite.set(true);
                crate::reconcile_metadata_view_log(0, &consensus, &journal).await;
                assert_eq!(journal.rewrites.get(), 0);
                assert_eq!(*journal.headers.borrow(), held);
                assert!(consensus.view_log_unproven());
            }

            crate::reconcile_metadata_view_log(0, &consensus, &journal).await;

            assert_eq!(
                journal.rewrites.get(),
                1,
                "one rewrite removes the whole proven run"
            );
            let removed = if unknown { UNKNOWN } else { FIRST }..=LAST;
            let headers = journal.headers.borrow();
            for (op, header) in &held {
                assert_eq!(
                    headers.get(op),
                    (!removed.contains(op)).then_some(header),
                    "op {op}: the canonical tail and entries past an unknown op stay"
                );
            }
            assert!(consensus.view_log_unproven());
            assert_eq!(
                consensus.view_log_repair_op(),
                Some(*removed.start()),
                "repair opens at the lowest removed op, so one window refetches the run"
            );
        }
    }

    #[compio::test]
    async fn given_a_bridged_header_off_the_view_chain_when_reconciling_metadata_should_fetch_its_op()
     {
        const VIEW: u32 = 3;
        const HEAD: u64 = 8;
        const BRIDGED: u64 = 5;
        let canonical: Vec<_> = metadata_prepares(HEAD, VIEW)
            .iter()
            .rev()
            .map(|prepare| *prepare.header())
            .collect();
        let held: BTreeMap<_, _> = canonical
            .iter()
            .filter(|header| header.op != BRIDGED)
            .map(|header| (header.op, *header))
            .collect();
        let bus = Rc::new(IggyMessageBus::new(0));
        let mut consensus = VsrConsensus::new(
            1,
            2,
            3,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.set_view(1);
        consensus.set_log_view(1);
        consensus.init();
        consensus.sequencer().set_sequence(HEAD);
        let view = start_view_suffix(
            IggyNamespace::from_raw(METADATA_GROUP),
            VIEW,
            HEAD,
            0,
            &off_chain_at(&canonical, BRIDGED),
        );
        consensus.handle_start_view(consensus::PlaneKind::Metadata, view.header(), view.body());
        assert!(consensus.view_log_unproven());
        let journal = CountingJournal {
            headers: RefCell::new(held.clone()),
            ..CountingJournal::default()
        };

        crate::reconcile_metadata_view_log(0, &consensus, &journal).await;

        assert_eq!(
            journal.removals.get(),
            0,
            "op {BRIDGED} is not held, so a removal there removes nothing and the next \
             proof names it again"
        );
        assert_eq!(*journal.headers.borrow(), held);
        assert!(consensus.view_log_unproven());
        assert_eq!(
            consensus.view_log_repair_op(),
            Some(BRIDGED),
            "the op under the off-chain header is a gap to fetch"
        );
    }

    fn metadata_prepares(head: u64, view: u32) -> Vec<Message<PrepareHeader>> {
        let mut parent = 0;
        (1..=head)
            .map(|op| {
                let body = CreateStreamRequest {
                    name: WireName::new(format!("repaired-stream-{op}")).unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes();
                let size = size_of::<PrepareHeader>() + body.len();
                let mut prepare = Message::<PrepareHeader>::new(size);
                prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
                let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
                    header.command = Command::Prepare;
                    header.operation = Operation::CreateStream;
                    header.cluster = 1;
                    header.group = METADATA_GROUP;
                    header.view = view;
                    header.replica = (view % 3) as u8;
                    header.op = op;
                    header.parent = parent;
                    header.timestamp = op;
                    header.size = u32::try_from(size).unwrap();
                    header.checksum_body = u128::from(iggy_common::calculate_checksum(&body));
                    header.checksum = header.identity_checksum();
                });
                parent = prepare.header().checksum;
                prepare
            })
            .collect()
    }

    #[compio::test]
    async fn given_a_stale_primary_heartbeat_when_the_suffix_cache_is_stale_should_reply_with_fresh_headers()
     {
        const HEAD: u64 = 2;
        const INCARNATION: u64 = 1;
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        for replica in [0, 2] {
            assert!(bus.owner_table().try_claim(replica, 1));
        }
        let (mut owner, _sender) = test_owner(&bus, None);
        owner.partition_consensus.replica_count = 3;
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let mut partition = repairing_replica(&bus, namespace, (REPAIR_PRIMARY, 3), 0, 0, 0);
        partition.repair = None;
        partition.set_created_revision(INCARNATION);
        crate::refresh_partition_dvc_suffix(&partition);
        for prepare in &prepares {
            partition
                .log
                .journal()
                .inner
                .append(prepare.clone().into_frozen())
                .await
                .unwrap();
            partition.consensus().note_journal_mutation();
        }
        partition.consensus().sequencer().set_sequence(HEAD);
        assert!(partition.consensus().local_dvc_suffix_stale());
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        sent.borrow_mut().clear();
        let heartbeat = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
            |_, header: &mut CommitHeader| {
                header.command = Command::Commit;
                header.cluster = 1;
                header.group = namespace.inner();
                header.partition_incarnation = INCARNATION;
                header.replica = 0;
                header.view = REPAIR_VIEW - 1;
                header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
                header.seal();
            },
        );

        owner.on_commit(&heartbeat).await;

        assert_eq!(sent.borrow().len(), 2);
        let expected: Vec<_> = prepares
            .iter()
            .rev()
            .map(|prepare| *prepare.header())
            .collect();
        for frame in sent.borrow().iter() {
            let reply =
                Message::<StartViewHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            let suffix = consensus::dvc_suffix_decode(
                &reply.as_slice()[size_of::<StartViewHeader>()..],
                HEAD,
                0,
                0,
            )
            .unwrap();
            assert_eq!(suffix.headers(), expected);
        }
    }

    #[compio::test]
    async fn given_unproven_suffix_when_adopting_start_view_should_repair_before_applying() {
        const APPLIED: u64 = 2;
        const LOCAL_HEAD: u64 = 4;
        const NEW_HEAD: u64 = 2050;
        const CHUNK: u64 = crate::REPAIR_CHUNK_MAX;
        const NEW_VIEW: u32 = 3;
        const NEW_PRIMARY: u8 = 0;
        let bus = Rc::new(IggyMessageBus::new(0));
        let requests = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&requests);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            let message =
                Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            if message.header().command == Command::RequestPrepares {
                captured.borrow_mut().push(
                    Message::<RequestPreparesHeader>::try_from(Owned::copy_from_slice(
                        frame.as_slice(),
                    ))
                    .unwrap(),
                );
            }
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(NEW_PRIMARY, 1));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, NEW_HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in prepares.iter().take(usize::try_from(LOCAL_HEAD).unwrap()) {
            partition.on_replicate(prepare.clone()).await;
        }
        partition.consensus().advance_commit_max(APPLIED);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(partition.consensus().commit_min(), APPLIED);
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                NEW_HEAD,
                NEW_HEAD,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert_eq!(partition.consensus().commit_min(), APPLIED);
        assert!(partition.log.journal().inner.holds_op(APPLIED + 1));
        assert!(partition.consensus().view_log_unproven());
        assert!(!partition.consensus().view_log_entry_proven(APPLIED + 1));
        let mut rounds = 0;
        let mut bodies = 0;
        while partition.consensus().view_log_unproven() {
            let request = requests.borrow_mut().pop().unwrap_or_else(|| {
                panic!("missing repair request after {rounds} rounds; hole={:?}, session={:?}, transfer={:?}",
                    partition.consensus().view_log_repair_op(), partition.repair,
                    partition.consensus().state_transfer_stage());
            });
            let request = request.header();
            let missing = partition.consensus().view_log_repair_op().unwrap();
            let expected_from = if partition.consensus().view_log_entry_proven(missing) {
                missing
            } else {
                missing.saturating_sub(CHUNK - 1).max(APPLIED + 1)
            };
            assert_eq!(request.from_op, expected_from);
            assert_eq!(request.to_op, missing);
            rounds += 1;
            assert!(rounds <= (NEW_HEAD - LOCAL_HEAD).div_ceil(CHUNK) + 1);
            let reply = Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
                .transmute_header(|_, header: &mut RepairRangeReplyHeader| {
                    header.command = Command::RepairDone;
                    header.size = u32::try_from(size_of::<RepairRangeReplyHeader>()).unwrap();
                    header.cluster = request.cluster;
                    header.group = request.group;
                    header.replica = NEW_PRIMARY;
                    header.nonce = request.nonce;
                    header.op = request.to_op;
                    header.seal();
                });
            owner.on_repair_range_reply(&reply).await;
            assert!(
                requests.borrow().is_empty(),
                "a terminator without bodies must wait for the retry timer"
            );
            for op in request.from_op..=request.to_op {
                bodies += 1;
                let repaired = prepares[usize::try_from(op - 1).unwrap()]
                    .clone()
                    .transmute_header(
                        |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                            repaired.0 = header;
                            repaired.0.command = Command::RepairPrepare;
                        },
                    );
                owner.on_repair_prepare(repaired).await;
            }
            let before = partition.consensus().commit_min();
            for command in [Command::RangeEvicted, Command::RepairDone] {
                let foreign_reply = reply.clone().transmute_header(
                    |header, foreign: &mut RepairRangeReplyHeader| {
                        *foreign = header;
                        foreign.command = command;
                        foreign.replica = REPAIR_PRIMARY;
                        foreign.seal();
                    },
                );
                owner.on_repair_range_reply(&foreign_reply).await;
                assert_eq!(partition.consensus().commit_min(), before);
                assert_eq!(partition.repair.unwrap().floor, None);
                assert!(
                    requests.borrow().is_empty(),
                    "a different peer cannot complete or advance this repair session"
                );
            }
            if !partition.consensus().view_log_unproven() {
                let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
                while partition.consensus().commit_min() < NEW_HEAD {
                    let before = partition.consensus().commit_min();
                    partition
                        .commit_journal(owner.plane.partitions().config())
                        .await;
                    assert!(partition.consensus().commit_min() > before);
                }
            }
            owner.on_repair_range_reply(&reply).await;
        }
        assert_eq!(rounds, (NEW_HEAD - LOCAL_HEAD).div_ceil(CHUNK) + 1);
        assert_eq!(bodies, NEW_HEAD - APPLIED);
        assert!(requests.borrow().is_empty());
        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert!(!partition.consensus().view_log_unproven());
        assert_eq!(partition.consensus().commit_min(), NEW_HEAD);
    }

    /// The three-replica group of [`HeadlessReplica`]: the primary of
    /// [`HEADLESS_VIEW`] is down, and the candidate is the next view's primary.
    const HEADLESS_REPLICAS: u8 = 3;
    const HEADLESS_PRIMARY: u8 = REPAIR_PRIMARY;
    const HEADLESS_BACKUP: u8 = 0;
    const HEADLESS_CANDIDATE: u8 = REPAIR_BACKUP;
    const HEADLESS_VIEW: u32 = 4;
    const HEADLESS_NEXT_VIEW: u32 = HEADLESS_VIEW + 1;

    /// Every frame a replica sent, with the replica it targeted.
    type TargetedFrames = Rc<RefCell<Vec<(u8, Message<GenericHeader>)>>>;

    /// A live replica of the group whose [`HEADLESS_VIEW`] primary is down.
    struct HeadlessReplica {
        owner: IoTestShard,
        _sender: TaggedSender,
        sent: TargetedFrames,
    }

    impl HeadlessReplica {
        /// Replica `replica` in [`REPAIR_VIEW`] with `held` journaled. With
        /// `persisted`, its partition WAL lives in that directory and its views go
        /// to that superblock store.
        #[allow(clippy::future_not_send)]
        async fn journaled(
            replica: u8,
            namespace: IggyNamespace,
            held: &[Message<PrepareHeader>],
            persisted: Option<(&Path, &Rc<HeldSuperblock>)>,
        ) -> Self {
            assert_eq!(
                u32::from(HEADLESS_PRIMARY),
                HEADLESS_VIEW % u32::from(HEADLESS_REPLICAS)
            );
            assert_eq!(
                u32::from(HEADLESS_CANDIDATE),
                HEADLESS_NEXT_VIEW % u32::from(HEADLESS_REPLICAS)
            );
            let bus = Rc::new(IggyMessageBus::new(0));
            let sent = Rc::new(RefCell::new(Vec::new()));
            let captured = Rc::clone(&sent);
            bus.set_replica_forward_fn(Box::new(move |target, _, frame| {
                let message =
                    Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                        .unwrap();
                captured.borrow_mut().push((target, message));
                Ok(())
            }));
            for peer in (0..HEADLESS_REPLICAS).filter(|peer| *peer != replica) {
                assert!(bus.owner_table().try_claim(peer, 1));
            }
            let (owner, sender) = test_owner(&bus, None);
            let mut partition = repairing_replica(
                &bus,
                namespace,
                (replica, HEADLESS_REPLICAS),
                HEADLESS_PRIMARY,
                0,
                0,
            );
            partition.repair = None;
            if let Some((directory, store)) = persisted {
                partition.set_partition_dir(directory.to_str().unwrap().to_owned());
                Box::pin(partition.open_persistence()).await.unwrap();
                partition.set_superblock(Rc::clone(store), None);
            }
            for prepare in held {
                Box::pin(partition.on_replicate(prepare.clone())).await;
            }
            let partitions = owner.plane.partitions();
            partitions.remove(&namespace).unwrap();
            partitions.insert(namespace, partition);
            partitions.set_io_notifier(
                owner.partition_io.notifier(),
                owner.partition_io.limits.bytes_max(),
            );
            Self {
                owner,
                _sender: sender,
                sent,
            }
        }

        fn partition(
            &self,
            namespace: IggyNamespace,
        ) -> &IggyPartition<Rc<IggyMessageBus>, HeldSuperblock> {
            self.owner.plane.partitions().get_by_ns(&namespace).unwrap()
        }

        /// Removes the first `command` frame this replica sent to `target`.
        fn take(&self, target: u8, command: Command) -> MessageBag {
            let mut sent = self.sent.borrow_mut();
            let index = sent
                .iter()
                .position(|(replica, message)| {
                    *replica == target && message.header().command == command
                })
                .unwrap_or_else(|| panic!("missing {command:?} to replica {target}"));
            MessageBag::try_from(sent.remove(index).1).unwrap()
        }

        fn sent_to(&self, target: u8, command: Command, view: u32) -> bool {
            self.sent.borrow().iter().any(|(replica, message)| {
                *replica == target
                    && message.header().command == command
                    && message.header().view == view
            })
        }

        /// One tick sweep and one pass of the IO lane, then one tick of wall time
        /// for the WAL and superblock tasks that the lane spawned.
        #[allow(clippy::future_not_send)]
        async fn step(&self, scratch: &mut Vec<IggyNamespace>) {
            assert!(self.owner.tick_partitions(scratch).await.is_none());
            self.owner.service_partition_io().await;
            compio::time::sleep(crate::CONSENSUS_TICK_INTERVAL).await;
        }
    }

    /// The candidate starts [`HEADLESS_NEXT_VIEW`] while `backup` holds the proven
    /// log of [`HEADLESS_VIEW`] and has run nothing since its proof settled. The
    /// backup votes only after the held write of `store` persists the new view,
    /// and the candidate wins the view with that vote.
    #[allow(clippy::future_not_send)]
    async fn elect_beside_proven_backup(
        namespace: IggyNamespace,
        backup: &HeadlessReplica,
        candidate: &HeadlessReplica,
        store: &HeldSuperblock,
    ) {
        let backup_consensus = || backup.partition(namespace).consensus();
        let candidate_consensus = || candidate.partition(namespace).consensus();
        let backup_voted = || {
            [Command::StartViewChange, Command::DoViewChange]
                .into_iter()
                .all(|command| backup.sent_to(HEADLESS_CANDIDATE, command, HEADLESS_NEXT_VIEW))
        };
        assert!(backup_consensus().is_normal());
        assert!(!backup_consensus().view_log_unproven());

        // Only the candidate ticks: its heartbeat timeout starts the next view
        // while the backup runs nothing after its proof settled.
        let mut scratch = Vec::new();
        for _ in 0..2 * TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if candidate_consensus().view() == HEADLESS_NEXT_VIEW {
                break;
            }
            assert!(
                candidate
                    .owner
                    .tick_partitions(&mut scratch)
                    .await
                    .is_none()
            );
        }
        assert_eq!(candidate_consensus().view(), HEADLESS_NEXT_VIEW);
        let (entered, mut started) = oneshot::channel();
        let (release, held) = oneshot::channel();
        *store.entered.borrow_mut() = Some(entered);
        *store.held.borrow_mut() = Some(held);
        backup
            .owner
            .on_message(candidate.take(HEADLESS_BACKUP, Command::StartViewChange))
            .await;
        assert_eq!(backup_consensus().view(), HEADLESS_NEXT_VIEW);
        assert!(!backup_consensus().is_normal());

        let persisting = async {
            while started.try_recv().unwrap().is_none() {
                backup.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(persisting))
            .await
            .expect("the backup must persist the view once its WAL certifies the view it proved");
        assert!(
            !backup_voted(),
            "the votes must wait for the superblock write"
        );
        release.send(()).unwrap();
        let votes = async {
            while !backup_voted() {
                backup.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(votes))
            .await
            .expect("the backup must vote once its superblock write completes");

        candidate
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
            .await;
        candidate
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::DoViewChange))
            .await;
        for _ in 0..TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            if candidate_consensus().is_normal() {
                break;
            }
            assert!(
                candidate
                    .owner
                    .tick_partitions(&mut scratch)
                    .await
                    .is_none()
            );
        }
        assert!(candidate_consensus().is_normal());
        assert!(candidate_consensus().is_primary());
        assert_eq!(candidate_consensus().view(), HEADLESS_NEXT_VIEW);
        backup
            .owner
            .on_message(candidate.take(HEADLESS_BACKUP, Command::StartView))
            .await;
        assert!(backup_consensus().is_normal());
        assert_eq!(backup_consensus().view(), HEADLESS_NEXT_VIEW);
    }

    /// A backup proves its adopted log through a repair while its WAL still
    /// certifies the view it replicated in. The next view change reaches it before
    /// any persist sweep certifies the proven view, and that view's primary is down.
    #[compio::test]
    async fn given_settled_view_proof_when_start_view_change_precedes_wal_certification_should_elect_primary()
     {
        const LOCAL_HEAD: u64 = 1;
        const HEAD: u64 = 2;
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&Rc::new(IggyMessageBus::new(0)), namespace, HEAD).await;
        let local = usize::try_from(LOCAL_HEAD).unwrap();
        let backup = Box::pin(HeadlessReplica::journaled(
            HEADLESS_BACKUP,
            namespace,
            &prepares[..local],
            Some((directory.path(), &store)),
        ))
        .await;
        let candidate = Box::pin(HeadlessReplica::journaled(
            HEADLESS_CANDIDATE,
            namespace,
            &prepares,
            None,
        ))
        .await;
        let start_view = start_view_suffix(
            namespace,
            HEADLESS_VIEW,
            HEAD,
            0,
            &[*prepares.last().unwrap().header()],
        );
        for replica in [&backup, &candidate] {
            replica.owner.on_start_view(start_view.clone()).await;
        }
        assert!(
            !candidate
                .partition(namespace)
                .consensus()
                .view_log_unproven()
        );
        assert!(backup.partition(namespace).consensus().view_log_unproven());

        backup
            .owner
            .on_repair_prepare(as_repair_prepare(prepares[local].clone()))
            .await;
        elect_beside_proven_backup(namespace, &backup, &candidate, &store).await;
    }

    /// A backup adopts a view whose log ends below an op it held. Dropping that op
    /// proves the adopted log with no repair frame, so no frame moves the chain
    /// anchor off the dropped op. The next view change reaches the backup before
    /// any prepare of the adopted view.
    #[compio::test]
    async fn given_adopted_head_below_held_op_when_start_view_change_precedes_any_prepare_should_elect_primary()
     {
        Box::pin(elect_after_dropping_held_op(false)).await;
    }

    /// As above, but the adopted head is the commit point. A segment holds it, so
    /// the resident headers no longer do.
    #[compio::test]
    async fn given_evicted_commit_point_below_held_op_when_start_view_change_precedes_any_prepare_should_elect_primary()
     {
        Box::pin(elect_after_dropping_held_op(true)).await;
    }

    /// If `head_committed`, the adopted head is also the commit point, and the
    /// backup holds it only in the repair ring.
    #[allow(clippy::future_not_send)]
    async fn elect_after_dropping_held_op(head_committed: bool) {
        const ADOPTED_HEAD: u64 = 1;
        const HELD_HEAD: u64 = ADOPTED_HEAD + 1;
        let commit = if head_committed { ADOPTED_HEAD } else { 0 };
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares =
            primary_prepares(&Rc::new(IggyMessageBus::new(0)), namespace, HELD_HEAD).await;
        let adopted = usize::try_from(ADOPTED_HEAD).unwrap();
        let backup = Box::pin(HeadlessReplica::journaled(
            HEADLESS_BACKUP,
            namespace,
            &prepares,
            Some((directory.path(), &store)),
        ))
        .await;
        assert_eq!(
            backup.partition(namespace).log.journal().inner.last_op(),
            Some(HELD_HEAD)
        );
        let candidate = Box::pin(HeadlessReplica::journaled(
            HEADLESS_CANDIDATE,
            namespace,
            &prepares[..adopted],
            None,
        ))
        .await;
        if head_committed {
            for replica in [&backup, &candidate] {
                replica
                    .partition(namespace)
                    .consensus()
                    .restore_commit_state(commit, commit);
            }
            let journal = &backup.partition(namespace).log.journal().inner;
            assert_eq!(
                journal.evict_prefix(adopted).len(),
                prepares.len() - adopted
            );
            assert!(journal.header_by_op(ADOPTED_HEAD).is_none());
            assert!(journal.repair_header(ADOPTED_HEAD).is_some());
        }
        let start_view = start_view_suffix(
            namespace,
            HEADLESS_VIEW,
            ADOPTED_HEAD,
            commit,
            &[*prepares[adopted - 1].header()],
        );
        for replica in [&backup, &candidate] {
            replica.owner.on_start_view(start_view.clone()).await;
        }
        // The adoption leaves the drop of the held op to the IO lane, and the
        // backup runs nothing once its proof settles.
        let mut scratch = Vec::new();
        let settling = async {
            while backup.partition(namespace).consensus().view_log_unproven() {
                backup.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(settling))
            .await
            .expect("dropping the held op must prove the adopted log");
        let partition = backup.partition(namespace);
        assert!(partition.repair.is_none());
        assert_eq!(
            partition.log.journal().inner.last_op(),
            (!head_committed).then_some(ADOPTED_HEAD)
        );
        assert_eq!(
            partition.consensus().sequencer().current_sequence(),
            ADOPTED_HEAD
        );
        elect_beside_proven_backup(namespace, &backup, &candidate, &store).await;
    }

    /// The candidate becomes the [`HEADLESS_NEXT_VIEW`] primary-elect beside the
    /// backup. It truncates the op that the merged log replaces, which clears the
    /// WAL certificate of the view it replicated in, and proves the merge through
    /// a repair. Returns the backup and the elect, whose view has not started.
    #[allow(clippy::future_not_send)]
    async fn elect_with_settled_merge(
        namespace: IggyNamespace,
        directory: &Path,
        store: &Rc<HeldSuperblock>,
    ) -> (HeadlessReplica, HeadlessReplica) {
        const HEAD: u64 = 2;
        let prepares = primary_prepares(&Rc::new(IggyMessageBus::new(0)), namespace, HEAD).await;
        let prefix = usize::try_from(HEAD - 1).unwrap();
        // The primary of both views prepared op 2 in the elect's view, lost it,
        // and prepared another op 2 in the adopted view.
        let merged =
            prepares[prefix]
                .clone()
                .transmute_header(|previous, header: &mut PrepareHeader| {
                    *header = previous;
                    header.view = HEADLESS_VIEW;
                    header.request += 1;
                    header.checksum = header.identity_checksum();
                });
        let backup = Box::pin(HeadlessReplica::journaled(
            HEADLESS_BACKUP,
            namespace,
            &prepares[..prefix],
            None,
        ))
        .await;
        let elect = Box::pin(HeadlessReplica::journaled(
            HEADLESS_CANDIDATE,
            namespace,
            &prepares,
            Some((directory, store)),
        ))
        .await;
        backup
            .owner
            .on_start_view(start_view_suffix(
                namespace,
                HEADLESS_VIEW,
                HEAD,
                0,
                &[*merged.header()],
            ))
            .await;
        backup
            .owner
            .on_repair_prepare(as_repair_prepare(merged.clone()))
            .await;
        let backup_consensus = || backup.partition(namespace).consensus();
        let elect_partition = || elect.partition(namespace);
        assert!(backup_consensus().is_normal());
        assert!(!backup_consensus().view_log_unproven());
        assert_eq!(elect_partition().log.journal().inner.last_op(), Some(HEAD));

        let mut scratch = Vec::new();
        for _ in 0..2 * TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if backup_consensus().view() == HEADLESS_NEXT_VIEW {
                break;
            }
            assert!(backup.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert_eq!(backup_consensus().view(), HEADLESS_NEXT_VIEW);
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
            .await;
        let announced = async {
            while !elect.sent_to(
                HEADLESS_BACKUP,
                Command::StartViewChange,
                HEADLESS_NEXT_VIEW,
            ) {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(announced))
            .await
            .expect("the elect must announce the view change");
        backup
            .owner
            .on_message(elect.take(HEADLESS_BACKUP, Command::StartViewChange))
            .await;
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::DoViewChange))
            .await;
        assert!(elect_partition().consensus().is_primary_elect());
        // The IO lane truncates the replaced op, then the elect fetches the merged one.
        let truncating = async {
            while elect_partition().repair.is_none() {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(truncating))
            .await
            .expect("the elect must truncate the replaced op and fetch the merged one");
        assert_eq!(
            elect_partition().log.journal().inner.last_op(),
            Some(HEAD - 1)
        );
        elect
            .owner
            .on_repair_prepare(as_repair_prepare(merged))
            .await;
        assert!(!elect_partition().consensus().view_log_unproven());
        assert!(elect_partition().consensus().is_primary_elect());
        assert_eq!(elect_partition().consensus().log_view(), REPAIR_VIEW);
        (backup, elect)
    }

    /// A primary-elect truncates the op that the merged log replaces and proves the
    /// merge through a repair. Its `log_view` names the view it replicated in until
    /// the view starts, so its WAL must not certify that view over the merged
    /// journal in between.
    #[compio::test]
    async fn given_a_settled_merge_when_the_elect_persists_before_the_view_starts_should_not_certify_its_old_log_view()
     {
        // Persist calls between the settled proof and the view start, one tick
        // apart. The WAL task publishes a queued certificate within the first few.
        const WINDOW_ROUNDS: usize = 20;
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let namespace = IggyNamespace::new(0, 0, 0);
        let (backup, elect) = Box::pin(elect_with_settled_merge(
            namespace,
            directory.path(),
            &store,
        ))
        .await;
        let backup_consensus = || backup.partition(namespace).consensus();
        let elect_partition = || elect.partition(namespace);
        let mut scratch = Vec::new();

        // The next tick sweep starts the view. Until then the journal is the
        // merged log while `log_view` still names the elect's own view.
        for _ in 0..WINDOW_ROUNDS {
            assert!(
                !elect_partition().persist_superblock_if_needed().await,
                "the WAL must not certify view {REPAIR_VIEW} over the merged log"
            );
            compio::time::sleep(crate::CONSENSUS_TICK_INTERVAL).await;
        }
        let started = async {
            while !elect.sent_to(HEADLESS_BACKUP, Command::StartView, HEADLESS_NEXT_VIEW) {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(started))
            .await
            .expect("the elect must start the view once its WAL certifies it");
        assert!(elect_partition().consensus().is_primary());
        assert_eq!(elect_partition().consensus().log_view(), HEADLESS_NEXT_VIEW);
        backup
            .owner
            .on_message(elect.take(HEADLESS_BACKUP, Command::StartView))
            .await;
        assert!(backup_consensus().is_normal());
        assert_eq!(backup_consensus().view(), HEADLESS_NEXT_VIEW);
    }

    /// As above, but the backup's view change supersedes the elect before its view
    /// starts. The third replica is down, so the new view needs the elect's votes,
    /// and those wait for a superblock write.
    #[compio::test]
    async fn given_a_settled_merge_when_a_view_change_supersedes_the_elect_should_vote_and_elect_a_primary()
     {
        const SUPERSEDING_VIEW: u32 = HEADLESS_NEXT_VIEW + 1;
        const SUPERSEDING_PRIMARY: u8 = HEADLESS_BACKUP;
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let namespace = IggyNamespace::new(0, 0, 0);
        assert_eq!(
            u32::from(SUPERSEDING_PRIMARY),
            SUPERSEDING_VIEW % u32::from(HEADLESS_REPLICAS)
        );
        let (backup, elect) = Box::pin(elect_with_settled_merge(
            namespace,
            directory.path(),
            &store,
        ))
        .await;
        let backup_consensus = || backup.partition(namespace).consensus();
        let elect_consensus = || elect.partition(namespace).consensus();
        // What the first refresh after the settled proof snapshots.
        crate::refresh_partition_dvc_suffix(elect.partition(namespace));
        let settled = elect_consensus().local_dvc_suffix();

        // Only the backup ticks: its status timeout supersedes the elect, which runs
        // nothing after its proof settled.
        let mut scratch = Vec::new();
        for _ in 0..2 * TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            if backup_consensus().view() == SUPERSEDING_VIEW {
                break;
            }
            assert!(backup.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert_eq!(backup_consensus().view(), SUPERSEDING_VIEW);
        // `take` returns the first match, so drop the frames of earlier views, the
        // backup's retransmits among them.
        for replica in [&backup, &elect] {
            replica
                .sent
                .borrow_mut()
                .retain(|(_, frame)| frame.header().view == SUPERSEDING_VIEW);
        }
        assert!(elect_consensus().view_log_merged());
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
            .await;
        assert_eq!(elect_consensus().view(), SUPERSEDING_VIEW);

        let voted = async {
            while ![Command::StartViewChange, Command::DoViewChange]
                .into_iter()
                .all(|command| elect.sent_to(SUPERSEDING_PRIMARY, command, SUPERSEDING_VIEW))
            {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(voted))
            .await
            .expect("the superseded elect must persist the view and vote in it");
        let MessageBag::DoViewChange(vote) = elect.take(SUPERSEDING_PRIMARY, Command::DoViewChange)
        else {
            unreachable!("take returns the command it was asked for");
        };
        let header = *vote.header();
        assert_eq!(
            header.log_view, REPAIR_VIEW,
            "the merge never started, so the elect claims the view it replicated in"
        );
        assert_eq!(
            (header.commit, header.nack_bitset, header.present_bitset),
            (0, 0, 0),
            "nothing is committed, and an unproven journal neither offers nor nacks an uncommitted op"
        );
        let suffix = consensus::dvc_suffix_decode(
            vote.body(),
            header.op,
            header.nack_bitset,
            header.present_bitset,
        )
        .unwrap();
        assert!(
            !suffix.is_empty(),
            "the vote must carry the snapshot taken before the view change"
        );
        assert_eq!(
            (suffix.headers(), header.nack_bitset, header.present_bitset),
            (
                settled.headers(),
                settled.nack_bitset(),
                settled.present_bitset()
            ),
            "the superseding handler must vote with the snapshot of the settled proof"
        );

        backup
            .owner
            .on_message(elect.take(SUPERSEDING_PRIMARY, Command::StartViewChange))
            .await;
        backup
            .owner
            .on_message(MessageBag::DoViewChange(vote))
            .await;
        for _ in 0..TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            if backup_consensus().is_normal() {
                break;
            }
            assert!(backup.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert!(backup_consensus().is_normal());
        assert!(backup_consensus().is_primary());
        assert_eq!(backup_consensus().view(), SUPERSEDING_VIEW);
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartView))
            .await;
        assert!(elect_consensus().is_normal());
        assert_eq!(elect_consensus().view(), SUPERSEDING_VIEW);
    }

    /// As above, but the elect adopted the view of the third replica, which goes
    /// silent after it prepares one more op. The third replica still votes for the
    /// next view with that op, then goes down. The merge truncates nothing, so the
    /// elect's WAL still certifies its `log_view`, and the backup's `log_view` is
    /// below it. Only the elect's journal then holds every op, so it must stay
    /// proven and offer them.
    #[compio::test]
    async fn given_a_settled_merge_that_kept_the_wal_certificate_when_a_lagging_backup_supersedes_the_elect_should_elect_the_backup_from_the_merged_log()
     {
        const HEAD: u64 = 3;
        const SUPERSEDING_VIEW: u32 = HEADLESS_NEXT_VIEW + 1;
        const SUPERSEDING_PRIMARY: u8 = HEADLESS_BACKUP;
        let directory = tempfile::tempdir().unwrap();
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&Rc::new(IggyMessageBus::new(0)), namespace, HEAD).await;
        let adopted = usize::try_from(HEAD - 1).unwrap();
        // The primary prepared the last op after it started the adopted view.
        let lagged =
            prepares[adopted]
                .clone()
                .transmute_header(|previous, header: &mut PrepareHeader| {
                    *header = previous;
                    header.view = HEADLESS_VIEW;
                    header.checksum = header.identity_checksum();
                });
        let backup = Box::pin(HeadlessReplica::journaled(
            HEADLESS_BACKUP,
            namespace,
            &prepares[..1],
            None,
        ))
        .await;
        let elect = Box::pin(HeadlessReplica::journaled(
            HEADLESS_CANDIDATE,
            namespace,
            &prepares[..adopted],
            Some((directory.path(), &store)),
        ))
        .await;
        let backup_consensus = || backup.partition(namespace).consensus();
        let elect_partition = || elect.partition(namespace);
        let elect_consensus = || elect.partition(namespace).consensus();
        elect
            .owner
            .on_start_view(start_view_suffix(
                namespace,
                HEADLESS_VIEW,
                HEAD - 1,
                0,
                &[*prepares[adopted - 1].header()],
            ))
            .await;
        assert!(!elect_consensus().view_log_unproven());
        assert_eq!(elect_consensus().log_view(), HEADLESS_VIEW);

        let mut scratch = Vec::new();
        let certified = async {
            while !elect_partition().wal_keeps_log_view() {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(certified))
            .await
            .expect("the elect's WAL must certify the view it adopted");
        for _ in 0..2 * TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if elect_consensus().view() == HEADLESS_NEXT_VIEW {
                break;
            }
            assert!(elect.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert_eq!(elect_consensus().view(), HEADLESS_NEXT_VIEW);
        let announced = async {
            while !elect.sent_to(
                HEADLESS_BACKUP,
                Command::StartViewChange,
                HEADLESS_NEXT_VIEW,
            ) {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(announced))
            .await
            .expect("the elect must announce the view change");
        backup
            .owner
            .on_message(elect.take(HEADLESS_BACKUP, Command::StartViewChange))
            .await;
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
            .await;
        // The backup's own vote never arrives, so the quorum is the elect's and the
        // third replica's, which offers a body for every op.
        let third_replica = DvcSuffix::new(
            [&lagged, &prepares[1], &prepares[0]]
                .map(|prepare| *prepare.header())
                .to_vec(),
            0,
            (1 << HEAD) - 1,
        );
        elect
            .owner
            .on_message(MessageBag::DoViewChange(do_view_change_suffix(
                namespace,
                HEADLESS_PRIMARY,
                HEADLESS_NEXT_VIEW,
                HEADLESS_VIEW,
                HEAD,
                &third_replica,
            )))
            .await;
        assert!(elect_consensus().is_primary_elect());
        let fetching = async {
            while elect_partition().repair.is_none() {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(fetching))
            .await
            .expect("the elect must fetch the op it lacks");
        assert_eq!(
            elect_partition().log.journal().inner.last_op(),
            Some(HEAD - 1),
            "the elect holds a prefix of the merged log, so nothing truncates"
        );
        elect
            .owner
            .on_repair_prepare(as_repair_prepare(lagged.clone()))
            .await;
        assert!(!elect_consensus().view_log_unproven());
        assert!(elect_consensus().is_primary_elect());
        assert!(elect_partition().wal_keeps_log_view());

        // Only the backup ticks: its status timeout supersedes the elect, which runs
        // nothing after its proof settled.
        for _ in 0..2 * TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            if backup_consensus().view() == SUPERSEDING_VIEW {
                break;
            }
            assert!(backup.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert_eq!(backup_consensus().view(), SUPERSEDING_VIEW);
        for replica in [&backup, &elect] {
            replica
                .sent
                .borrow_mut()
                .retain(|(_, frame)| frame.header().view == SUPERSEDING_VIEW);
        }
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartViewChange))
            .await;
        let voted = async {
            while ![Command::StartViewChange, Command::DoViewChange]
                .into_iter()
                .all(|command| elect.sent_to(SUPERSEDING_PRIMARY, command, SUPERSEDING_VIEW))
            {
                elect.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(voted))
            .await
            .expect("the superseded elect must vote");
        let MessageBag::DoViewChange(vote) = elect.take(SUPERSEDING_PRIMARY, Command::DoViewChange)
        else {
            unreachable!("take returns the command it was asked for");
        };
        assert!(
            backup_consensus().log_view() < vote.header().log_view,
            "no copy the backup holds can stand in for the elect's"
        );
        backup
            .owner
            .on_message(elect.take(SUPERSEDING_PRIMARY, Command::StartViewChange))
            .await;
        backup
            .owner
            .on_message(MessageBag::DoViewChange(vote))
            .await;

        let merging = async {
            while backup.partition(namespace).repair.is_none() {
                backup.step(&mut scratch).await;
            }
        };
        compio::time::timeout(REPLY_DEADLINE, Box::pin(merging))
            .await
            .expect("the backup must merge the elect's log and fetch the ops it lacks");
        for prepare in [&prepares[1], &lagged] {
            backup
                .owner
                .on_repair_prepare(as_repair_prepare(prepare.clone()))
                .await;
        }
        for _ in 0..TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            if backup_consensus().is_normal() {
                break;
            }
            assert!(backup.owner.tick_partitions(&mut scratch).await.is_none());
        }
        assert!(backup_consensus().is_normal());
        assert!(backup_consensus().is_primary());
        assert_eq!(backup_consensus().view(), SUPERSEDING_VIEW);
        elect
            .owner
            .on_message(backup.take(HEADLESS_CANDIDATE, Command::StartView))
            .await;
        assert!(elect_consensus().is_normal());
        assert_eq!(elect_consensus().view(), SUPERSEDING_VIEW);
    }

    /// The walk refuses an unproven view log, so a resident op after
    /// `commit_min` cannot move a group whose proof waits on a missing body.
    /// When the session ceiling refuses the arm at `StartView` adoption, the
    /// gap detector is the only site left to fetch that body.
    #[compio::test]
    async fn given_a_proof_waiting_above_a_resident_next_op_when_the_edge_arm_is_refused_should_arm_from_the_tick()
     {
        const APPLIED: u64 = 2;
        const LOCAL_HEAD: u64 = 4;
        const NEW_HEAD: u64 = 8;
        const NEW_VIEW: u32 = 3;
        const NEW_PRIMARY: u8 = 0;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_gap_debounce_ticks(REPAIR_GAP_DEBOUNCE_TICKS_MIN);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, NEW_HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in prepares.iter().take(usize::try_from(LOCAL_HEAD).unwrap()) {
            partition.on_replicate(prepare.clone()).await;
        }
        partition.consensus().advance_commit_max(APPLIED);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        // Other groups hold every session slot when the view change lands.
        owner
            .partition_repairs_inflight
            .set(PARTITION_REPAIRS_INFLIGHT_MAX);

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                NEW_HEAD,
                NEW_HEAD,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        let proof_op = {
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            assert!(
                partition.repair.is_none(),
                "the ceiling must refuse the edge arm"
            );
            assert_eq!(partition.consensus().commit_min(), APPLIED);
            assert!(partition.log.journal().inner.holds_op(APPLIED + 1));
            let proof_op = partition.consensus().view_log_repair_op().unwrap();
            assert!(!partition.log.journal().inner.holds_op(proof_op));
            proof_op
        };

        let mut expected = vec![(None, None); REPAIR_GAP_DEBOUNCE_TICKS_MIN as usize - 1];
        expected.push((Some(NEW_PRIMARY), None));
        let observed = stall_rounds(&owner, namespace, expected.len()).await;

        assert_eq!(
            observed, expected,
            "the detector must arm once its debounce runs out"
        );
        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert_eq!(
            partition.repair.map(|session| session.from_op),
            Some(proof_op)
        );
    }

    /// Host at `namespace` a backup of [`REPAIR_VIEW`] that applied `prepares`.
    #[allow(clippy::future_not_send)]
    async fn host_applied_backup(
        owner: &IoTestShard,
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        prepares: &[Message<PrepareHeader>],
    ) {
        let mut partition =
            repairing_replica(bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in prepares {
            partition.on_replicate(prepare.clone()).await;
        }
        let applied = u64::try_from(prepares.len()).unwrap();
        partition.consensus().advance_commit_max(applied);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(partition.consensus().commit_min(), applied);
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
    }

    #[compio::test]
    async fn given_a_start_view_that_contradicts_an_applied_op_when_adopting_should_refuse_it() {
        const APPLIED: u64 = 2;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, APPLIED).await;
        host_applied_backup(&owner, &bus, namespace, &prepares).await;
        let mut forged = *prepares.last().unwrap().header();
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                APPLIED,
                APPLIED,
                &[forged],
            ))
            .await;

        // The refusal also fences the partition for resync, behind the tombstone.
        let partition = owner.plane.partitions().get_io_owner(&namespace).unwrap();
        assert_eq!(
            partition.consensus().view(),
            REPAIR_VIEW,
            "a log naming another prepare at applied op {APPLIED} must be refused, not adopted"
        );
        assert_eq!(partition.consensus().applied_log_contradictions(), 1);
    }

    #[compio::test]
    async fn given_a_refused_contradicting_start_view_when_ticking_should_export_it_once() {
        const APPLIED: u64 = 2;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, APPLIED).await;
        host_applied_backup(&owner, &bus, namespace, &prepares).await;
        let mut forged = *prepares.last().unwrap().header();
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();
        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                APPLIED,
                APPLIED,
                &[forged],
            ))
            .await;

        for _ in 0..2 {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
        }

        assert_eq!(
            owner.metrics.partition_applied_log_contradictions_value(),
            1
        );
        let partition = owner.plane.partitions().get_io_owner(&namespace).unwrap();
        assert_eq!(
            partition.consensus().applied_log_contradictions(),
            1,
            "the export must leave the total for the simulator invariants"
        );
    }

    #[compio::test]
    async fn given_a_start_view_contradicting_an_evicted_applied_op_when_refused_should_fence_the_partition_for_resync()
     {
        const APPLIED: u64 = 2;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, APPLIED).await;
        host_applied_backup(&owner, &bus, namespace, &prepares).await;
        {
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            let journal = &partition.log.journal().inner;
            journal.evict_prefix(usize::try_from(APPLIED).unwrap());
            assert!(journal.header_by_op(APPLIED).is_none());
            assert!(partition.wal_checksum(APPLIED).is_none());
            assert!(partition.applied_header(APPLIED).is_some());
        }
        let mut forged = *prepares.last().unwrap().header();
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                APPLIED,
                APPLIED,
                &[forged],
            ))
            .await;

        let partitions = owner.plane.partitions();
        assert!(
            partitions.is_tombstoned(&namespace),
            "an op only the repair ring still holds must fence the partition it refused on"
        );
        assert_eq!(owner.metrics.partition_applied_divergences_value(), 1);
        assert_eq!(
            partitions
                .get_io_owner(&namespace)
                .unwrap()
                .consensus()
                .view(),
            REPAIR_VIEW
        );
    }

    #[compio::test]
    async fn given_a_refused_contradicting_metadata_log_when_ticking_should_export_it_once() {
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut owner, _sender) = test_owner(&bus, None);
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.init();
        owner.plane.inner_mut().0 = IggyMetadata::new(
            Some(consensus),
            None,
            None,
            None,
            IoTestMetadata::new((
                UsersInner::new().into(),
                (StreamsInner::default().into(), ()),
            )),
            None,
        );
        let announced = PrepareHeader {
            checksum: 1,
            ..PrepareHeader::default()
        };
        let consensus = owner.plane.inner().0.consensus.as_ref().unwrap();
        assert!(consensus.contradicts_applied_log(
            consensus::PlaneKind::Metadata,
            consensus::ContradictionSource::StartView,
            1,
            [&announced],
            |_| Some(PrepareHeader {
                checksum: 2,
                ..announced
            }),
        ));

        for _ in 0..2 {
            owner.tick_metadata().await;
        }

        assert_eq!(owner.metrics.metadata_applied_log_contradictions_value(), 1);
        assert_eq!(
            owner
                .plane
                .inner()
                .0
                .consensus
                .as_ref()
                .unwrap()
                .applied_log_contradictions(),
            1
        );
    }

    #[compio::test]
    async fn given_a_start_view_that_matches_the_applied_ops_when_adopting_should_adopt_it() {
        const APPLIED: u64 = 2;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, APPLIED).await;
        host_applied_backup(&owner, &bus, namespace, &prepares).await;

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                APPLIED,
                APPLIED,
                &[*prepares.last().unwrap().header()],
            ))
            .await;

        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert_eq!(partition.consensus().view(), NEW_VIEW);
        assert!(!partition.consensus().view_log_unproven());
        assert_eq!(partition.consensus().applied_log_contradictions(), 0);
    }

    #[compio::test]
    async fn given_a_divergent_entry_when_removal_is_deferred_should_keep_the_canonical_tail() {
        const APPLIED: u64 = 2;
        const DIVERGENT: u64 = 3;
        const HEAD: u64 = 4;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in &prepares {
            partition.on_replicate(prepare.clone()).await;
        }
        partition.consensus().advance_commit_max(APPLIED);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        partition
            .log
            .journal()
            .inner
            .remove_range(DIVERGENT..=DIVERGENT)
            .await
            .unwrap();
        let divergent = prepares[usize::try_from(DIVERGENT - 1).unwrap()]
            .clone()
            .transmute_header(|previous, header: &mut PrepareHeader| {
                *header = previous;
                header.client += 1;
                header.checksum = header.identity_checksum();
            });
        partition
            .log
            .journal()
            .inner
            .append(divergent.into_frozen())
            .await
            .unwrap();
        let partitions = owner.plane.partitions();
        partitions.remove(&namespace).unwrap();
        partitions.insert(namespace, partition);
        partitions.set_io_notifier(
            owner.partition_io.notifier(),
            owner.partition_io.limits.bytes_max(),
        );

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                HEAD,
                HEAD,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        assert!(
            partitions
                .get_by_ns(&namespace)
                .unwrap()
                .view_truncation_pending()
        );
        for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
            owner.service_partition_io().await;
            if !partitions
                .get_by_ns(&namespace)
                .unwrap()
                .view_truncation_pending()
            {
                break;
            }
        }
        let partition = partitions.get_by_ns(&namespace).unwrap();
        assert!(!partition.view_truncation_pending());
        assert!(partition.consensus().view_log_unproven());
        assert_eq!(partition.consensus().commit_min(), APPLIED);
        assert!(!partition.log.journal().inner.holds_op(DIVERGENT));
        assert_eq!(
            partition.log.journal().inner.header_by_op(HEAD),
            Some(*prepares.last().unwrap().header())
        );

        let repaired = prepares[usize::try_from(DIVERGENT - 1).unwrap()]
            .clone()
            .transmute_header(
                |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                    repaired.0 = header;
                    repaired.0.command = Command::RepairPrepare;
                },
            );
        owner.on_repair_prepare(repaired).await;
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        assert!(!partition.consensus().view_log_unproven());
        partition.commit_journal(partitions.config()).await;
        assert_eq!(partition.consensus().commit_min(), HEAD);
    }

    #[compio::test]
    async fn given_a_divergent_partition_run_when_reconciling_should_remove_it_at_once() {
        const APPLIED: u64 = 2;
        const FIRST: u64 = APPLIED + 1;
        const DIVERGENT: u64 = 32;
        const LAST: u64 = FIRST + DIVERGENT - 1;
        const HEAD: u64 = LAST + 3;
        const NEW_VIEW: u32 = 3;
        for deferred in [true, false] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            let namespace = IggyNamespace::new(0, 0, 0);
            let prepares = primary_prepares(&bus, namespace, HEAD).await;
            let canonical: Vec<_> = prepares
                .iter()
                .rev()
                .take(usize::try_from(HEAD - APPLIED).unwrap())
                .map(|prepare| *prepare.header())
                .collect();
            let view = || start_view_suffix(namespace, NEW_VIEW, HEAD, HEAD, &canonical);
            install_divergent_backup(&owner, &bus, namespace, &prepares, APPLIED, FIRST..=LAST)
                .await;
            let partitions = owner.plane.partitions();
            if deferred {
                partitions.set_io_notifier(
                    owner.partition_io.notifier(),
                    owner.partition_io.limits.bytes_max(),
                );
            }
            owner.on_start_view(view()).await;

            assert_eq!(
                settle_view_truncation(&owner, namespace, FIRST..=LAST).await,
                usize::from(deferred),
                "one transition truncates the whole proven run"
            );
            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            assert!(partition.consensus().view_log_unproven());
            assert_eq!(
                partition.consensus().view_log_repair_op(),
                Some(FIRST),
                "repair opens at the lowest removed op, so one window refetches the run"
            );
            partition.commit_journal(partitions.config()).await;
            assert_eq!(partition.consensus().commit_min(), APPLIED);
            let journal = &partition.log.journal().inner;
            for prepare in prepares.iter().skip(usize::try_from(APPLIED).unwrap()) {
                let op = prepare.header().op;
                if op <= LAST {
                    assert!(!journal.holds_op(op), "divergent op {op} must go");
                } else {
                    assert_eq!(
                        journal.header_by_op(op),
                        Some(*prepare.header()),
                        "canonical op {op} must stay"
                    );
                }
            }
        }
    }

    #[compio::test]
    async fn given_a_bridged_header_off_the_view_chain_when_reconciling_a_partition_should_fetch_its_op()
     {
        const APPLIED: u64 = 2;
        const HEAD: u64 = 8;
        const BRIDGED: u64 = 5;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let canonical: Vec<_> = prepares
            .iter()
            .rev()
            .take(usize::try_from(HEAD - APPLIED).unwrap())
            .map(|prepare| *prepare.header())
            .collect();
        install_divergent_backup(
            &owner,
            &bus,
            namespace,
            &prepares,
            APPLIED,
            BRIDGED..=BRIDGED,
        )
        .await;
        let partitions = owner.plane.partitions();
        partitions
            .get_by_ns(&namespace)
            .unwrap()
            .log
            .journal()
            .inner
            .remove_range(BRIDGED..=BRIDGED)
            .await
            .unwrap();
        partitions.set_io_notifier(
            owner.partition_io.notifier(),
            owner.partition_io.limits.bytes_max(),
        );

        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                HEAD,
                HEAD,
                &off_chain_at(&canonical, BRIDGED),
            ))
            .await;

        let partition = partitions.get_by_ns(&namespace).unwrap();
        assert!(
            !partition.view_truncation_pending(),
            "op {BRIDGED} is not held, so a truncation there removes nothing and the next \
             proof defers it again"
        );
        assert!(partition.consensus().view_log_unproven());
        assert_eq!(
            partition.consensus().view_log_repair_op(),
            Some(BRIDGED),
            "the op under the off-chain header is a gap to fetch"
        );
        let journal = &partition.log.journal().inner;
        for prepare in prepares.iter().skip(usize::try_from(APPLIED).unwrap()) {
            let op = prepare.header().op;
            assert_eq!(
                journal.header_by_op(op),
                (op != BRIDGED).then_some(*prepare.header()),
                "op {op}: the canonical tail and the unproven entries below the gap stay"
            );
        }
    }

    /// Service the I/O lane until the view truncation settles, and count the steps
    /// that removed part of `divergent`.
    #[allow(clippy::future_not_send)]
    async fn settle_view_truncation(
        owner: &IoTestShard,
        namespace: IggyNamespace,
        divergent: RangeInclusive<u64>,
    ) -> usize {
        let partitions = owner.plane.partitions();
        let pending = || {
            partitions
                .get_by_ns(&namespace)
                .unwrap()
                .view_truncation_pending()
        };
        let held = || {
            let partition = partitions.get_by_ns(&namespace).unwrap();
            divergent
                .clone()
                .filter(|op| partition.log.journal().inner.holds_op(*op))
                .count()
        };
        let mut still_held = held();
        let mut truncations = 0;
        for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
            if !pending() {
                break;
            }
            owner.service_partition_io().await;
            let now_held = held();
            if now_held < still_held {
                truncations += 1;
                still_held = now_held;
            }
        }
        assert!(!pending(), "the view truncation never settled");
        truncations
    }

    /// Install a backup that journaled `prepares`, applied through `applied`, and
    /// holds a stale copy of every op in `divergent`. It inherits the I/O lane only
    /// once the test set one.
    #[allow(clippy::future_not_send)]
    async fn install_divergent_backup(
        owner: &IoTestShard,
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        prepares: &[Message<PrepareHeader>],
        applied: u64,
        divergent: RangeInclusive<u64>,
    ) {
        let mut partition =
            repairing_replica(bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in prepares {
            partition.on_replicate(prepare.clone()).await;
        }
        partition.consensus().advance_commit_max(applied);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        let journal = &partition.log.journal().inner;
        journal.remove_range(divergent.clone()).await.unwrap();
        for prepare in prepares
            .iter()
            .filter(|prepare| divergent.contains(&prepare.header().op))
        {
            let stale = prepare
                .clone()
                .transmute_header(|previous, header: &mut PrepareHeader| {
                    *header = previous;
                    header.client += 1;
                    header.checksum = header.identity_checksum();
                });
            journal.append(stale.into_frozen()).await.unwrap();
        }
        let partitions = owner.plane.partitions();
        partitions.remove(&namespace).unwrap();
        partitions.insert(namespace, partition);
    }

    /// `suffix`, high to low, with the header at `bridged` moved off the view's
    /// chain. The header above it goes blank, so the suffix still decodes: only
    /// adjacent headers must chain there.
    fn off_chain_at(suffix: &[PrepareHeader], bridged: u64) -> Vec<PrepareHeader> {
        suffix
            .iter()
            .map(|header| {
                if header.op == bridged + 1 {
                    consensus::dvc_blank(header.op)
                } else if header.op == bridged {
                    let mut off_chain = *header;
                    off_chain.client += 1;
                    off_chain.checksum = off_chain.identity_checksum();
                    off_chain
                } else {
                    *header
                }
            })
            .collect()
    }

    #[compio::test]
    async fn given_deferred_view_truncation_when_resuming_should_settle_proof_only_after_success() {
        const APPLIED: u64 = 2;
        const LOCAL_HEAD: u64 = 6;
        const NEW_HEAD: u64 = 4;
        const NEW_VIEW: u32 = 3;
        for (truncate_fails, repair_fallback) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            let namespace = IggyNamespace::new(0, 0, 0);
            let prepares = primary_prepares(&bus, namespace, LOCAL_HEAD).await;
            let mut partition =
                repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
            partition.repair = None;
            for prepare in prepares.iter().take(usize::try_from(LOCAL_HEAD).unwrap()) {
                partition.on_replicate(prepare.clone()).await;
            }
            partition.consensus().advance_commit_max(APPLIED);
            partition
                .commit_journal(owner.plane.partitions().config())
                .await;
            let partitions = owner.plane.partitions();
            partitions.remove(&namespace).unwrap();
            partitions.insert(namespace, partition);
            partitions.set_io_notifier(
                owner.partition_io.notifier(),
                owner.partition_io.limits.bytes_max(),
            );
            let view = start_view_suffix(
                namespace,
                NEW_VIEW,
                NEW_HEAD,
                NEW_HEAD,
                &[*prepares[usize::try_from(NEW_HEAD - 1).unwrap()].header()],
            );
            if repair_fallback {
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                partition.consensus().handle_start_view(
                    consensus::PlaneKind::Partitions,
                    view.header(),
                    view.body(),
                );
                assert!(
                    !owner
                        .maybe_request_partition_repair(partition, REPAIR_PRIMARY)
                        .await
                );
            } else {
                owner.on_start_view(view).await;
            }
            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            assert!(partition.view_truncation_pending());
            assert!(partition.consensus().view_log_unproven());
            if truncate_fails {
                partition.log.active_segment_mut().size = IggyByteSize::from(u64::MAX);
            }

            for _ in 0..crate::router::COOPERATIVE_EVENT_BUDGET {
                owner.service_partition_io().await;
                if !partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .view_truncation_pending()
                {
                    break;
                }
            }

            let partition = partitions.get_mut_by_ns(&namespace).unwrap();
            assert!(!partition.view_truncation_pending());
            assert_eq!(partition.consensus().view_log_unproven(), truncate_fails);
            partition.commit_journal(partitions.config()).await;
            assert_eq!(
                partition.consensus().commit_min(),
                if truncate_fails { APPLIED } else { NEW_HEAD }
            );
            assert_eq!(
                partition.log.journal().inner.holds_op(NEW_HEAD + 1),
                truncate_fails
            );
        }
    }

    #[compio::test]
    async fn given_commit_only_start_view_when_entries_are_acked_should_keep_the_running_suffix() {
        const FIRST_HEAD: u64 = 4;
        const NEW_HEAD: u64 = 5;
        const NEW_VIEW: u32 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, NEW_HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for prepare in prepares.iter().take(usize::try_from(FIRST_HEAD).unwrap()) {
            partition.on_replicate(prepare.clone()).await;
        }
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        let suffix: Vec<_> = prepares[..usize::try_from(FIRST_HEAD).unwrap()]
            .iter()
            .rev()
            .map(|prepare| *prepare.header())
            .collect();
        owner
            .on_start_view(start_view_suffix(
                namespace, NEW_VIEW, FIRST_HEAD, 2, &suffix,
            ))
            .await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        let prepare = prepares.last().unwrap().clone().transmute_header(
            |previous, header: &mut PrepareHeader| {
                *header = previous;
                header.view = NEW_VIEW;
                header.replica = 0;
                header.commit = 3;
                header.checksum = header.identity_checksum();
            },
        );
        partition.on_replicate(prepare.clone()).await;
        assert!(partition.log.journal().inner.holds_op(NEW_HEAD));
        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                NEW_HEAD,
                FIRST_HEAD,
                &[*prepare.header()],
            ))
            .await;
        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert_eq!(partition.consensus().commit_min(), FIRST_HEAD);
        assert!(partition.log.journal().inner.holds_op(NEW_HEAD));
    }

    #[compio::test]
    async fn given_evicted_commit_backlog_when_adopting_start_view_should_preserve_the_ring_floor()
    {
        const COMMITTED: u64 = 3;
        const STAMPED: u64 = 5;
        const NEW_HEAD: u64 = 7;
        let bus = Rc::new(IggyMessageBus::new(0));
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, NEW_HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        for prepare in prepares.iter().take(usize::try_from(STAMPED).unwrap()) {
            let prepare = if prepare.header().op == STAMPED {
                prepare
                    .clone()
                    .transmute_header(|previous, header: &mut PrepareHeader| {
                        *header = previous;
                        header.commit = STAMPED - 1;
                        header.checksum = header.identity_checksum();
                    })
            } else {
                prepare.clone()
            };
            partition.on_replicate(prepare).await;
        }
        partition.consensus().advance_commit_max(COMMITTED);
        partition
            .log
            .journal()
            .inner
            .evict_prefix(usize::try_from(COMMITTED).unwrap());
        assert_eq!(partition.consensus().commit_min(), 0);
        let message = start_view_suffix(
            namespace,
            3,
            NEW_HEAD,
            NEW_HEAD,
            &[*prepares.last().unwrap().header()],
        );
        partition.consensus().handle_start_view(
            consensus::PlaneKind::Partitions,
            message.header(),
            &message.as_slice()[size_of::<StartViewHeader>()..],
        );
        let pending = partition.consensus().pending_view_log();
        assert_eq!(
            crate::partition_view_divergence_from(&partition, pending.as_ref()),
            None
        );
        assert!(
            partition
                .log
                .journal()
                .inner
                .repair_header(COMMITTED)
                .is_some()
        );
    }

    #[compio::test]
    async fn given_pending_view_truncation_when_peer_requests_repair_should_not_serve_old_entries()
    {
        const HEAD: u64 = 4;
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(REPAIR_PRIMARY, 1));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let mut partition =
            repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
        for prepare in prepares {
            partition.on_replicate(prepare).await;
        }
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        let request = Message::<iggy_binary_protocol::RequestPreparesHeader>::new(size_of::<
            iggy_binary_protocol::RequestPreparesHeader,
        >())
        .transmute_header(
            |_, header: &mut iggy_binary_protocol::RequestPreparesHeader| {
                header.command = Command::RequestPrepares;
                header.cluster = 1;
                header.group = namespace.inner();
                header.replica = REPAIR_PRIMARY;
                header.from_op = 3;
                header.to_op = HEAD;
                header.size = u32::try_from(std::mem::size_of_val(header)).unwrap();
                header.seal();
            },
        );
        sent.borrow_mut().clear();
        owner.on_request_prepares(&request).await;
        assert!(
            !sent.borrow().is_empty(),
            "the unblocked replica serves this range"
        );
        sent.borrow_mut().clear();
        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace)
            .unwrap()
            .defer_view_truncation(3..=u64::MAX, Vec::new(), None);
        owner.on_request_prepares(&request).await;
        assert!(
            sent.borrow().is_empty(),
            "unproven entries must not reach a peer"
        );
    }

    #[compio::test]
    async fn given_abandoned_prepares_when_a_backup_requests_repair_should_serve_only_committed_ops()
     {
        const REPLICAS: u8 = 5;
        const DONOR: u8 = 3;
        const BACKUP: u8 = 4;
        const NEXT_VIEW: u32 = 2;
        const NEXT_PRIMARY: u8 = 2;
        const COMMITTED: u64 = 2;
        const HEAD: u64 = 3;
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            let message =
                Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            if message.header().command == Command::RepairPrepare {
                let prepare = Message::<iggy_binary_protocol::RepairPrepareHeader>::try_from(
                    Owned::copy_from_slice(frame.as_slice()),
                )
                .unwrap();
                captured.borrow_mut().push(prepare.header().0);
            }
            Ok(())
        }));
        for requester in [BACKUP, NEXT_PRIMARY] {
            assert!(bus.owner_table().try_claim(requester, 1));
        }
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let mut donor = repairing_replica(&bus, namespace, (DONOR, REPLICAS), REPAIR_PRIMARY, 0, 0);
        donor.repair = None;
        for prepare in prepares {
            donor.on_replicate(prepare).await;
        }
        donor.consensus().advance_commit_max(COMMITTED);
        donor
            .commit_journal(owner.plane.partitions().config())
            .await;
        let change =
            view_control_message(namespace, Command::StartViewChange, NEXT_VIEW, NEXT_PRIMARY);
        let MessageBag::StartViewChange(change) = change else {
            unreachable!()
        };
        donor
            .consensus()
            .handle_start_view_change(consensus::PlaneKind::Partitions, change.header());
        assert_eq!(donor.consensus().status(), consensus::Status::ViewChange);
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, donor);

        for (requester, view, through) in [
            (BACKUP, NEXT_VIEW, COMMITTED),
            (NEXT_PRIMARY, NEXT_VIEW, HEAD),
            (NEXT_PRIMARY, REPAIR_VIEW, COMMITTED),
        ] {
            let request = Message::<RequestPreparesHeader>::new(size_of::<RequestPreparesHeader>())
                .transmute_header(|_, header: &mut RequestPreparesHeader| {
                    header.command = Command::RequestPrepares;
                    header.cluster = 1;
                    header.group = namespace.inner();
                    header.replica = requester;
                    header.view = view;
                    header.from_op = 1;
                    header.to_op = HEAD;
                    header.size = u32::try_from(size_of::<RequestPreparesHeader>()).unwrap();
                    header.seal();
                });
            sent.borrow_mut().clear();
            owner.on_request_prepares(&request).await;
            let served: Vec<_> = sent.borrow().iter().map(|header| header.op).collect();
            assert_eq!(
                served,
                (1..=through).collect::<Vec<_>>(),
                "requester {requester} in view {view}"
            );
        }
    }

    /// Seven replicas: the requester committed ops 1..=2 in view 1, adopted view 2
    /// at op 2 and learned commit 3 from view 2's primary. A stale donor serves the
    /// op 3 view 1 prepared, which view 2 replaced. Nothing the requester holds
    /// vouches for it, so it is refused and the window is re-requested from the
    /// primary, whose op 3 commits instead.
    #[compio::test]
    async fn given_a_stale_donor_serving_an_abandoned_op_when_commit_covers_it_should_commit_the_primary_op()
     {
        let bus = Rc::new(IggyMessageBus::new(0));
        let requests = capture_repair_requests(&bus);
        assert!(bus.owner_table().try_claim(STALE_SHAPE_PRIMARY, 1));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let (abandoned, committed) = Box::pin(requester_past_an_abandoned_op(
            &owner,
            &bus,
            namespace,
            STALE_SHAPE_DONOR,
        ))
        .await;

        owner.on_repair_prepare(as_repair_prepare(abandoned)).await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(
            partition.consensus().commit_min(),
            STALE_SHAPE_COMMITTED,
            "the abandoned op committed"
        );
        assert!(!partition.log.journal().inner.holds_op(STALE_SHAPE_OP));
        let session = partition.repair.unwrap();
        assert_eq!(session.peer, STALE_SHAPE_PRIMARY);
        assert_ne!(session.nonce, STALE_SHAPE_NONCE);
        let request = requests.borrow_mut().pop().expect("a re-request");
        assert_eq!(
            (
                request.0,
                request.1.nonce,
                request.1.from_op,
                request.1.to_op
            ),
            (
                STALE_SHAPE_PRIMARY,
                session.nonce,
                STALE_SHAPE_OP,
                STALE_SHAPE_OP
            )
        );

        owner
            .on_repair_range_reply(&repair_done(
                namespace,
                STALE_SHAPE_DONOR,
                STALE_SHAPE_NONCE,
                STALE_SHAPE_OP,
            ))
            .await;
        let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
        assert_eq!(
            partition
                .repair
                .map(|standing| (standing.peer, standing.nonce)),
            Some((session.peer, session.nonce))
        );
        assert!(!partition.log.journal().inner.holds_op(STALE_SHAPE_OP));
        assert!(requests.borrow().is_empty());

        commit_from_primary(&owner, namespace, &committed, session.nonce).await;
    }

    /// Bodies name no sender, so a body that the session's peer did not serve
    /// is told apart only by the gates: one nothing vouches for is refused, and
    /// the session against the primary stands.
    #[compio::test]
    async fn given_a_body_from_a_peer_outside_the_session_when_nothing_vouches_for_it_should_refuse_it()
     {
        let bus = Rc::new(IggyMessageBus::new(0));
        let requests = capture_repair_requests(&bus);
        assert!(bus.owner_table().try_claim(STALE_SHAPE_PRIMARY, 1));
        let (owner, _sender) = test_owner(&bus, None);
        let namespace = IggyNamespace::new(0, 0, 0);
        let (abandoned, committed) = Box::pin(requester_past_an_abandoned_op(
            &owner,
            &bus,
            namespace,
            STALE_SHAPE_PRIMARY,
        ))
        .await;

        owner.on_repair_prepare(as_repair_prepare(abandoned)).await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(partition.consensus().commit_min(), STALE_SHAPE_COMMITTED);
        assert!(!partition.log.journal().inner.holds_op(STALE_SHAPE_OP));
        let session = partition.repair.unwrap();
        assert_eq!(
            (session.peer, session.nonce),
            (STALE_SHAPE_PRIMARY, STALE_SHAPE_NONCE)
        );
        assert!(requests.borrow().is_empty());

        commit_from_primary(&owner, namespace, &committed, session.nonce).await;
    }

    /// Repair rotates off a peer that served an abandoned branch. The primary's
    /// prepares are never spliced onto the bodies that branch left unproven, the
    /// proof walk removes the branch from the view's head down, and the old
    /// peer's late bodies and terminator change nothing.
    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_an_abandoned_branch_from_the_previous_peer_when_repair_rotates_should_never_splice_it()
     {
        const REPLICAS: u8 = 5;
        const REQUESTER: u8 = 4;
        const OLD_PEER: u8 = 1;
        const OLD_NONCE: u128 = 1;
        const NEW_VIEW: u32 = 2;
        const PRIMARY: u8 = 2;
        const APPLIED: u64 = 2;
        const FORK: u64 = 3;
        const HEAD: u64 = 6;
        const CHUNK: u64 = 4;
        let bus = Rc::new(IggyMessageBus::new(0));
        let requests = capture_repair_requests(&bus);
        assert!(bus.owner_table().try_claim(PRIMARY, 1));
        let (owner, _sender) = test_owner(&bus, None);
        owner.set_repair_chunk_max(CHUNK);
        let namespace = IggyNamespace::new(0, 0, 0);
        let prepares = primary_prepares(&bus, namespace, HEAD).await;
        let abandoned = abandoned_branch(&prepares, FORK);
        let canonical = |op: u64| prepares[usize::try_from(op - 1).unwrap()].clone();
        let branch = |op: u64| abandoned[usize::try_from(op - FORK).unwrap()].clone();
        let checksum = |prepare: Message<PrepareHeader>| Some(prepare.header().checksum);
        let mut partition =
            repairing_replica(&bus, namespace, (REQUESTER, REPLICAS), REPAIR_PRIMARY, 0, 0);
        partition.repair = None;
        for op in 1..=APPLIED {
            partition.on_replicate(canonical(op)).await;
        }
        partition.consensus().advance_commit_max(APPLIED);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(partition.consensus().commit_min(), APPLIED);
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        owner
            .on_start_view(start_view_suffix(
                namespace,
                NEW_VIEW,
                HEAD,
                HEAD,
                &[*canonical(HEAD).header()],
            ))
            .await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        assert!(partition.consensus().view_log_unproven());
        let session = partition.repair.as_mut().unwrap();
        session.peer = OLD_PEER;
        session.nonce = OLD_NONCE;
        requests.borrow_mut().clear();
        let held = |op: u64| {
            owner
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap()
                .log
                .journal()
                .inner
                .header_by_op(op)
                .map(|header| header.checksum)
        };

        // The view's head bridges op 6, so the walk asks for 3..=5. The old peer
        // serves them with its op 3 delayed. Nothing held is next to op 4, so it
        // waits for the walk; op 5 does not chain to the view's head.
        for op in FORK + 1..=HEAD {
            owner.on_repair_prepare(as_repair_prepare(branch(op))).await;
        }
        assert_eq!(held(4), checksum(branch(4)));
        assert_eq!(held(5), None);
        assert_eq!(held(HEAD), None);
        let session = partition.repair.unwrap();
        assert_eq!(session.peer, PRIMARY);
        assert_ne!(session.nonce, OLD_NONCE);
        let request = requests.borrow_mut().pop().expect("a re-request");
        assert_eq!(
            (
                request.0,
                request.1.nonce,
                request.1.from_op,
                request.1.to_op
            ),
            (PRIMARY, session.nonce, FORK, HEAD - 1)
        );
        owner
            .on_repair_range_reply(&repair_done(namespace, OLD_PEER, OLD_NONCE, HEAD - 1))
            .await;
        assert_eq!(
            partition
                .repair
                .map(|standing| (standing.peer, standing.nonce)),
            Some((session.peer, session.nonce))
        );
        assert!(requests.borrow().is_empty());

        // The primary's chunk. Op 3 chains to the committed prefix but not to the
        // branch's op 4 above it. Op 5 chains to the view's head, and the walk
        // removes the branch's op 4.
        owner
            .on_repair_prepare(as_repair_prepare(canonical(FORK)))
            .await;
        assert_eq!(held(FORK), None, "op 3 spliced under the abandoned branch");
        for op in FORK + 1..HEAD {
            owner
                .on_repair_prepare(as_repair_prepare(canonical(op)))
                .await;
        }
        assert_eq!(held(4), None);
        assert_eq!(held(5), checksum(canonical(5)));

        for op in FORK + 1..=HEAD {
            owner.on_repair_prepare(as_repair_prepare(branch(op))).await;
        }
        owner
            .on_repair_range_reply(&repair_done(namespace, OLD_PEER, OLD_NONCE, HEAD - 1))
            .await;
        assert_eq!(held(4), None, "a late body from the old peer was journaled");
        assert_eq!(held(5), checksum(canonical(5)));
        assert_eq!(
            held(HEAD),
            None,
            "a late body from the old peer was journaled"
        );
        assert!(requests.borrow().is_empty());

        for (served_through, window) in [(HEAD - 1, FORK..=4), (4, HEAD..=HEAD)] {
            owner
                .on_repair_range_reply(&repair_done(
                    namespace,
                    PRIMARY,
                    session.nonce,
                    served_through,
                ))
                .await;
            let request = requests.borrow_mut().pop().expect("the next chunk");
            assert_eq!(request.1.from_op..=request.1.to_op, window);
            for op in window {
                owner
                    .on_repair_prepare(as_repair_prepare(canonical(op)))
                    .await;
            }
        }
        assert!(!partition.consensus().view_log_unproven());
        owner
            .on_repair_prepare(as_repair_prepare(branch(FORK)))
            .await;
        assert_eq!(held(FORK), checksum(canonical(FORK)));

        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        while partition.consensus().commit_min() < HEAD {
            let before = partition.consensus().commit_min();
            partition
                .commit_journal(owner.plane.partitions().config())
                .await;
            assert!(partition.consensus().commit_min() > before);
        }
        for op in FORK..=HEAD {
            assert_eq!(held(op), checksum(canonical(op)), "op {op}");
        }
    }

    #[compio::test]
    async fn given_a_blank_start_view_head_when_repair_arrives_should_require_vouched_history() {
        const HEAD: u64 = 1;
        const NEW_VIEW: u32 = 3;
        for held in [false, true] {
            let bus = Rc::new(IggyMessageBus::new(0));
            let (owner, _sender) = test_owner(&bus, None);
            let namespace = IggyNamespace::new(0, 0, 0);
            let prepares = primary_prepares(&bus, namespace, HEAD).await;
            let prepare = prepares.into_iter().next().unwrap();
            let mut partition =
                repairing_replica(&bus, namespace, (REPAIR_BACKUP, 3), REPAIR_PRIMARY, 0, 0);
            partition.repair = None;
            if held {
                partition.on_replicate(prepare.clone()).await;
            }
            owner.plane.partitions().remove(&namespace).unwrap();
            owner.plane.partitions().insert(namespace, partition);
            owner
                .on_start_view(start_view_suffix(
                    namespace,
                    NEW_VIEW,
                    HEAD,
                    HEAD,
                    &[consensus::dvc_blank(HEAD)],
                ))
                .await;
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            assert!(partition.consensus().view_log_unproven());
            assert!(!partition.view_truncation_pending());
            assert_eq!(partition.log.journal().inner.holds_op(HEAD), held);
            let vouched =
                consensus::restamp_prepare_view(prepare.clone().into_frozen(), NEW_VIEW).unwrap();
            let vouched =
                Message::<PrepareHeader>::try_from(Owned::copy_from_slice(vouched.as_slice()))
                    .unwrap();
            let repaired = prepare.transmute_header(
                |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                    repaired.0 = header;
                    repaired.0.command = Command::RepairPrepare;
                },
            );
            owner.on_repair_prepare(repaired).await;
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            assert!(partition.consensus().view_log_unproven());
            assert_eq!(partition.consensus().commit_min(), 0);
            let repaired = vouched.transmute_header(
                |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                    repaired.0 = header;
                    repaired.0.command = Command::RepairPrepare;
                },
            );
            owner.on_repair_prepare(repaired).await;
            let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
            assert!(!partition.consensus().view_log_unproven());
            partition
                .commit_journal(owner.plane.partitions().config())
                .await;
            assert_eq!(partition.consensus().commit_min(), HEAD);
        }
    }

    fn start_view_suffix(
        namespace: IggyNamespace,
        view: u32,
        op: u64,
        commit: u64,
        suffix: &[PrepareHeader],
    ) -> Message<StartViewHeader> {
        let header_size = size_of::<StartViewHeader>();
        let total = header_size + std::mem::size_of_val(suffix);
        let mut message = Message::<StartViewHeader>::new(total);
        consensus::encode_prepare_headers(suffix, &mut message.as_mut_slice()[header_size..]);
        let checksum_body = crate::control_body_checksum(&message.as_slice()[header_size..]);
        message.transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = 1;
            header.group = namespace.inner();
            header.replica = (view % 3) as u8;
            header.view = view;
            header.op = op;
            header.commit = commit;
            header.size = u32::try_from(total).unwrap();
            header.checksum_body = checksum_body;
            header.seal();
        })
    }

    /// The vote of `replica` in `view` with `suffix` as its log up to `op` and
    /// nothing committed.
    fn do_view_change_suffix(
        namespace: IggyNamespace,
        replica: u8,
        view: u32,
        log_view: u32,
        op: u64,
        suffix: &DvcSuffix,
    ) -> Message<DoViewChangeHeader> {
        let header_size = size_of::<DoViewChangeHeader>();
        let total = header_size + suffix.encoded_len();
        let mut message = Message::<DoViewChangeHeader>::new(total);
        suffix.encode_into(&mut message.as_mut_slice()[header_size..]);
        let checksum_body = crate::control_body_checksum(&message.as_slice()[header_size..]);
        message.transmute_header(|_, header: &mut DoViewChangeHeader| {
            header.command = Command::DoViewChange;
            header.cluster = 1;
            header.group = namespace.inner();
            header.replica = replica;
            header.view = view;
            header.log_view = log_view;
            header.op = op;
            header.nack_bitset = suffix.nack_bitset();
            header.present_bitset = suffix.present_bitset();
            header.size = u32::try_from(total).unwrap();
            header.checksum_body = checksum_body;
            header.seal();
        })
    }

    /// The seven-replica stale-donor shape of
    /// [`requester_past_an_abandoned_op`]: view 2's primary, a donor still on view
    /// 1's log and the requester between them.
    const STALE_SHAPE_REPLICAS: u8 = 7;
    const STALE_SHAPE_REQUESTER: u8 = 4;
    const STALE_SHAPE_DONOR: u8 = 3;
    const STALE_SHAPE_VIEW: u32 = 2;
    const STALE_SHAPE_PRIMARY: u8 = 2;
    const STALE_SHAPE_COMMITTED: u64 = 2;
    const STALE_SHAPE_OP: u64 = 3;
    const STALE_SHAPE_NONCE: u128 = 1;

    /// Installs a requester that committed ops 1..=2 in view 1, adopted
    /// [`STALE_SHAPE_VIEW`] at op 2 and learned commit 3 from its primary, with a
    /// repair session for op 3 against `peer`. Returns view 1's op 3, which the
    /// view replaced, and the op 3 the view's primary prepared.
    #[allow(clippy::future_not_send)]
    async fn requester_past_an_abandoned_op(
        owner: &IoTestShard,
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        peer: u8,
    ) -> (Message<PrepareHeader>, Message<PrepareHeader>) {
        let mut prepares = primary_prepares(bus, namespace, STALE_SHAPE_OP).await;
        let abandoned = prepares.pop().unwrap();
        let committed =
            abandoned
                .clone()
                .transmute_header(|previous, header: &mut PrepareHeader| {
                    *header = previous;
                    header.view = STALE_SHAPE_VIEW;
                    header.timestamp += 1;
                    header.checksum = header.identity_checksum();
                });
        let mut partition = repairing_replica(
            bus,
            namespace,
            (STALE_SHAPE_REQUESTER, STALE_SHAPE_REPLICAS),
            peer,
            0,
            0,
        );
        partition.repair = None;
        for prepare in &prepares {
            partition.on_replicate(prepare.clone()).await;
        }
        partition
            .consensus()
            .advance_commit_max(STALE_SHAPE_COMMITTED);
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        owner.plane.partitions().remove(&namespace).unwrap();
        owner.plane.partitions().insert(namespace, partition);
        owner
            .on_start_view(start_view_suffix(
                namespace,
                STALE_SHAPE_VIEW,
                STALE_SHAPE_COMMITTED,
                STALE_SHAPE_COMMITTED,
                &[*prepares.last().unwrap().header()],
            ))
            .await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        assert_eq!(partition.consensus().commit_min(), STALE_SHAPE_COMMITTED);
        assert!(!partition.consensus().view_log_unproven());
        partition.consensus().advance_commit_max(STALE_SHAPE_OP);
        partition.repair = Some(RepairSession {
            nonce: STALE_SHAPE_NONCE,
            view: STALE_SHAPE_VIEW,
            from_op: STALE_SHAPE_OP,
            commit_to_op: STALE_SHAPE_OP,
            fetch_to_op: STALE_SHAPE_OP,
            floor: None,
            peer,
            idle_ticks: 0,
        });
        (abandoned, committed)
    }

    /// The primary of [`STALE_SHAPE_VIEW`] serves `committed` to the session
    /// `nonce`, which then commits it.
    #[allow(clippy::future_not_send)]
    async fn commit_from_primary(
        owner: &IoTestShard,
        namespace: IggyNamespace,
        committed: &Message<PrepareHeader>,
        nonce: u128,
    ) {
        owner
            .on_repair_prepare(as_repair_prepare(committed.clone()))
            .await;
        owner
            .on_repair_range_reply(&repair_done(
                namespace,
                STALE_SHAPE_PRIMARY,
                nonce,
                STALE_SHAPE_OP,
            ))
            .await;
        let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        partition
            .commit_journal(owner.plane.partitions().config())
            .await;
        assert_eq!(partition.consensus().commit_min(), STALE_SHAPE_OP);
        assert_eq!(
            partition
                .log
                .journal()
                .inner
                .header_by_op(STALE_SHAPE_OP)
                .map(|header| header.checksum),
            Some(committed.header().checksum)
        );
    }

    /// The branch an earlier view's primary prepared on top of op `fork - 1` and
    /// no later view committed: another prepare at each op of `canonical` from
    /// `fork` on, each chained to the branch's previous op.
    fn abandoned_branch(
        canonical: &[Message<PrepareHeader>],
        fork: u64,
    ) -> Vec<Message<PrepareHeader>> {
        let fork = usize::try_from(fork).unwrap();
        let mut parent = canonical[fork - 2].header().checksum;
        canonical[fork - 1..]
            .iter()
            .map(|prepare| {
                let chained_to = parent;
                let abandoned =
                    prepare
                        .clone()
                        .transmute_header(|previous, header: &mut PrepareHeader| {
                            *header = previous;
                            header.timestamp += 1;
                            header.parent = chained_to;
                            header.checksum = header.identity_checksum();
                        });
                parent = abandoned.header().checksum;
                abandoned
            })
            .collect()
    }

    fn as_repair_prepare(
        prepare: Message<PrepareHeader>,
    ) -> Message<iggy_binary_protocol::RepairPrepareHeader> {
        prepare.transmute_header(
            |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                repaired.0 = header;
                repaired.0.command = Command::RepairPrepare;
            },
        )
    }

    fn repair_done(
        namespace: IggyNamespace,
        replica: u8,
        nonce: u128,
        op: u64,
    ) -> Message<RepairRangeReplyHeader> {
        Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
            .transmute_header(|_, header: &mut RepairRangeReplyHeader| {
                header.command = Command::RepairDone;
                header.size = u32::try_from(size_of::<RepairRangeReplyHeader>()).unwrap();
                header.cluster = 1;
                header.group = namespace.inner();
                header.replica = replica;
                header.nonce = nonce;
                header.op = op;
                header.seal();
            })
    }

    /// Every `RequestPrepares` the shard sends to a claimed replica, with that
    /// replica.
    fn capture_repair_requests(
        bus: &IggyMessageBus,
    ) -> Rc<RefCell<Vec<(u8, RequestPreparesHeader)>>> {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&requests);
        bus.set_replica_forward_fn(Box::new(move |replica, _, frame| {
            let message =
                Message::<GenericHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            if message.header().command == Command::RequestPrepares {
                let request = Message::<RequestPreparesHeader>::try_from(Owned::copy_from_slice(
                    frame.as_slice(),
                ))
                .unwrap();
                captured.borrow_mut().push((replica, *request.header()));
            }
            Ok(())
        }));
        requests
    }

    fn test_owner<B: MessageBus + Clone + 'static>(
        bus: &B,
        store: Option<&Rc<HeldSuperblock>>,
    ) -> (IoTestShard<B>, TaggedSender) {
        let shard_id = ShardId::new(0);
        let config = PartitionsConfig {
            messages_required_to_save: 100,
            size_of_messages_required_to_save: IggyByteSize::from(SEGMENT_BYTES),
            validate_checksum: true,
            segment_size: IggyByteSize::from(SEGMENT_BYTES),
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: PartitionPathLayout::default(),
        };
        let partitions = IggyPartitions::new(shard_id, config);
        let routes = PapayaShardsTable::new();
        let mut inner = StreamsInner::default();
        let mut stream = Stream::default();
        let mut topic = Topic::default();
        for partition_id in 0..TEST_PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            let consensus = VsrConsensus::new(
                1,
                0,
                1,
                namespace.inner(),
                bus.clone(),
                LocalPipeline::new(),
            );
            consensus.init();
            let mut partition = IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(SEGMENT_BYTES),
            );
            if partition_id == 0
                && let Some(store) = store
            {
                partition.set_superblock(Rc::clone(store), None);
            }
            partitions.insert(namespace, partition);
            routes.insert(namespace, PartitionLocation::new(shard_id, 1));
            topic.partitions.push(Partition::new(
                partition_id,
                namespace.inner(),
                IggyTimestamp::default(),
                1,
                0,
            ));
        }
        stream.topics.insert(topic);
        inner.items.insert(stream);
        let metadata = IoTestMetadata::new((Users::default(), (inner.into(), ())));
        let metadata = IggyMetadata::new(None, None, None, None, metadata, None);
        let (sender, inbox, replies) = shard_channel(0, 2, 1);
        let owner = IoTestShard::<B>::new(
            ShardIdentity::new(0, "partition-io-test".to_owned()),
            bus.clone(),
            Rc::new(NoopHost),
            metadata,
            partitions,
            vec![sender.clone()],
            inbox,
            replies,
            2,
            None,
            routes,
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
            None,
            ShardMetrics::for_shard(),
        )
        .expect("valid shard wiring");
        (owner, sender)
    }

    /// A backup in [`REPAIR_VIEW`] whose repair session for
    /// `(commit_min, commit_max]` is pinned to the primary.
    fn repairing_backup(
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        commit_min: u64,
        commit_max: u64,
    ) -> IggyPartition<Rc<IggyMessageBus>, HeldSuperblock> {
        repairing_replica(
            bus,
            namespace,
            (REPAIR_BACKUP, 3),
            REPAIR_PRIMARY,
            commit_min,
            commit_max,
        )
    }

    /// Replica `replica` of a group of `replica_count` in [`REPAIR_VIEW`],
    /// whose repair session for `(commit_min, commit_max]` is pinned to
    /// `peer`.
    fn repairing_replica(
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        (replica, replica_count): (u8, u8),
        peer: u8,
        commit_min: u64,
        commit_max: u64,
    ) -> IggyPartition<Rc<IggyMessageBus>, HeldSuperblock> {
        let mut consensus = VsrConsensus::new(
            1,
            replica,
            replica_count,
            namespace.inner(),
            Rc::clone(bus),
            LocalPipeline::new(),
        );
        consensus.set_view(REPAIR_VIEW);
        consensus.set_log_view(REPAIR_VIEW);
        consensus.init();
        consensus.restore_commit_state(commit_min, commit_min);
        consensus.advance_commit_max(commit_max);
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(SEGMENT_BYTES),
        );
        partition.repair = Some(RepairSession {
            nonce: 1,
            view: REPAIR_VIEW,
            from_op: commit_min + 1,
            commit_to_op: commit_max,
            fetch_to_op: commit_max,
            floor: None,
            peer,
            idle_ticks: 0,
        });
        partition
    }

    /// Copies of the first `ops` prepares that the primary of [`REPAIR_VIEW`]
    /// journals, one send each, as a peer serves them.
    #[allow(clippy::future_not_send)]
    async fn primary_prepares(
        bus: &Rc<IggyMessageBus>,
        namespace: IggyNamespace,
        ops: u64,
    ) -> Vec<Message<PrepareHeader>> {
        let mut consensus = VsrConsensus::new(
            1,
            REPAIR_PRIMARY,
            3,
            namespace.inner(),
            Rc::clone(bus),
            LocalPipeline::new(),
        );
        consensus.set_view(REPAIR_VIEW);
        consensus.set_log_view(REPAIR_VIEW);
        consensus.init();
        let mut origin: IggyPartition<_, HeldSuperblock> = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(SEGMENT_BYTES),
        );
        let mut prepares = Vec::new();
        for op in 1..=ops {
            origin.on_request(send_request(namespace, op), None).await;
            let frozen = origin.log.journal().inner.repair_entry(op).unwrap();
            let mut prepare = Message::<PrepareHeader>::new(frozen.len());
            prepare.as_mut_slice().copy_from_slice(frozen.as_slice());
            prepares.push(prepare);
            // Capture historical bodies beyond one live acknowledgment window.
            origin.consensus().clear_pipeline();
            origin.consensus().drain_loopback_into(&mut Vec::new());
        }
        prepares
    }

    /// Run `rounds` stall rounds, one per sweep, and record after each the
    /// peer of the repair session and of the transfer session.
    #[allow(clippy::future_not_send)]
    async fn stall_rounds(
        owner: &IoTestShard,
        namespace: IggyNamespace,
        rounds: usize,
    ) -> Vec<(Option<u8>, Option<u8>)> {
        let mut observed = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            assert!(owner.tick_partitions(&mut Vec::new()).await.is_none());
            let partition = owner.plane.partitions().get_by_ns(&namespace).unwrap();
            observed.push((
                partition.repair.map(|session| session.peer),
                partition.transfer.as_ref().map(|session| session.peer),
            ));
        }
        observed
    }

    /// Backs the first segment with files, so flushes and transfer plans read
    /// the bytes they name.
    #[allow(clippy::future_not_send)]
    async fn attach_segment_files(
        partition: &mut IggyPartition<Rc<IggyMessageBus>, HeldSuperblock>,
        directory: &Path,
        fsync: bool,
    ) -> (PathBuf, PathBuf) {
        partition.set_partition_dir(directory.to_str().unwrap().to_owned());
        let log_path = directory.join("00000000000000000000.log");
        let index_path = directory.join("00000000000000000000.index");
        let messages = MessagesWriter::new(
            log_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            fsync,
            false,
            None,
        )
        .await
        .unwrap();
        let indexes = IggyIndexWriter::new(
            index_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            fsync,
            false,
        )
        .await
        .unwrap();
        let storage = SegmentStorage::new(
            log_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            0,
            0,
            true,
        )
        .await
        .unwrap();
        partition.log.retire_back();
        partition.log.add_persisted_segment(
            Segment::new(0, IggyByteSize::from(SEGMENT_BYTES)),
            storage,
            Some(Rc::new(messages)),
            Some(Rc::new(indexes)),
        );
        (log_path, index_path)
    }

    fn submit(
        sender: &TaggedSender,
        namespace: IggyNamespace,
    ) -> Receiver<Option<Message<GenericHeader>>> {
        let (reply, replies) = channel(1);
        sender
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: send_request(namespace, 1),
                reply,
                attachment: None,
            }))
            .expect("fixture request fits the inbox");
        replies
    }

    /// Each request comes from its own client, because a client has one request
    /// in flight per partition.
    fn send_request(namespace: IggyNamespace, request: u64) -> Message<RoutedRequestHeader> {
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: u128::from(request),
                payload_length: 1,
                ..Default::default()
            },
            payload: vec![1].into(),
            user_headers: None,
        });
        SendMessagesOwned::from_messages(namespace, &messages)
            .unwrap()
            .encode_request(RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::SendMessages,
                client: u128::from(request),
                session: 1,
                request: 1,
                group: namespace.inner(),
                ..Default::default()
            })
            .unwrap()
    }

    fn view_control_message(
        namespace: IggyNamespace,
        command: Command,
        view: u32,
        peer: u8,
    ) -> MessageBag {
        match command {
            Command::StartViewChange => MessageBag::StartViewChange(
                Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                    .transmute_header(|_, header: &mut StartViewChangeHeader| {
                        header.command = command;
                        header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                        header.cluster = 1;
                        header.group = namespace.inner();
                        header.replica = peer;
                        header.view = view;
                        header.seal();
                    }),
            ),
            Command::DoViewChange => MessageBag::DoViewChange(
                Message::<DoViewChangeHeader>::new(size_of::<DoViewChangeHeader>())
                    .transmute_header(|_, header: &mut DoViewChangeHeader| {
                        header.command = command;
                        header.size = u32::try_from(size_of::<DoViewChangeHeader>()).unwrap();
                        header.cluster = 1;
                        header.group = namespace.inner();
                        header.replica = peer;
                        header.view = view;
                        header.seal();
                    }),
            ),
            Command::StartView => MessageBag::StartView(
                Message::<StartViewHeader>::new(size_of::<StartViewHeader>()).transmute_header(
                    |_, header: &mut StartViewHeader| {
                        header.command = command;
                        header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
                        header.cluster = 1;
                        header.group = namespace.inner();
                        header.replica = peer;
                        header.view = view;
                        header.seal();
                    },
                ),
            ),
            _ => unreachable!("only view-control frames are built here"),
        }
    }

    fn store_request(namespace: IggyNamespace, ack: AckLevel) -> Message<RoutedRequestHeader> {
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer {
                kind: ConsumerKind::Consumer.as_code(),
                id: WireIdentifier::Numeric(1),
            },
            stream_id: WireIdentifier::Numeric(namespace.stream_id().try_into().unwrap()),
            topic_id: WireIdentifier::Numeric(namespace.topic_id().try_into().unwrap()),
            partition_id: Some(namespace.partition_id().try_into().unwrap()),
            offset: 0,
            ack,
        }
        .to_bytes();
        let header_size = size_of::<RoutedRequestHeader>();
        let size = header_size + body.len();
        let mut request = Message::<RoutedRequestHeader>::new(size);
        request.as_mut_slice()[header_size..].copy_from_slice(&body);
        request.transmute_header(|_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = Operation::StoreConsumerOffset;
            header.client = 1;
            header.session = 1;
            header.request = 1;
            header.group = namespace.inner();
            header.size = u32::try_from(size).unwrap();
        })
    }

    #[derive(Default)]
    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct HeldSuperblock {
        entered: RefCell<Option<oneshot::Sender<()>>>,
        held: RefCell<Option<oneshot::Receiver<()>>>,
    }

    #[allow(clippy::future_not_send)]
    impl SuperblockStore for HeldSuperblock {
        async fn write(&self, _payload: &[u8]) -> io::Result<()> {
            let held = self.held.borrow_mut().take();
            if let Some(held) = held {
                self.entered.borrow_mut().take().unwrap().send(()).unwrap();
                held.await.unwrap();
            }
            Ok(())
        }

        fn read_latest(&self) -> impl Future<Output = io::Result<SuperblockContents>> {
            std::future::ready(Ok(SuperblockContents::Empty))
        }
    }
}
