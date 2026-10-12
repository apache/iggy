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
                if let Some(peer) = peer {
                    self.finish_partition_view_adoption(namespace, peer, actions)
                        .await;
                } else {
                    self.advance_pending_partition_view(namespace).await;
                }
                self.partition_io.continue_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Transition(message) => {
                self.on_message(message).await;
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
    use std::cell::RefCell;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    use std::time::Duration;

    use consensus::{
        ArtifactProgress, Consensus, LocalPipeline, PartitionsHandle, Pipeline, Sequencer,
        StateArtifact, StateTransferStage, VsrConsensus, artifact_kind,
    };
    use futures::FutureExt;
    use futures::channel::oneshot;
    use iggy_binary_protocol::primitives::consumer::WireConsumer;
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::{
        AckLevel, Command, ConsensusHeader, DoViewChangeHeader, GenericHeader, Operation,
        PrepareHeader, PrepareOkHeader, RepairRangeReplyHeader, ReplyHeader, RoutedRequestHeader,
        StartViewChangeHeader, StartViewHeader, WireEncode, WireIdentifier,
    };
    use iggy_common::{
        ConsumerGroupOffsets, ConsumerKind, ConsumerOffsets, Durability, IggyByteSize,
        IggyTimestamp, PartitionStats, TopicRuntimeOptions, variadic,
    };
    use journal::prepare_journal::PrepareJournal;
    use journal::superblock::{SuperblockContents, SuperblockStore};
    use message_bus::{IggyMessageBus, MessageBus};
    use metadata::stm::stream::{Partition, Stream, Streams, StreamsInner, Topic};
    use metadata::stm::user::Users;
    use metadata::{IggyMetadata, MuxStateMachine};
    use partitions::state_transfer::{
        PartitionArtifactSource, PartitionTransferSession, TransferArtifact,
    };
    use partitions::{
        IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionPathLayout,
        PartitionsConfig, RepairConclusion, RepairSession, Segment,
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
        IggyShard, LifecycleFrame, PartitionConsensusConfig, Receiver, ReconcileOp,
        ReplicaTopology, ShardFrame, ShardIdentity, TaggedSender, channel, shard_channel,
    };

    const SEGMENT_BYTES: u64 = 1024 * 1024;
    const TEST_PARTITIONS: usize = 3;
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
                    commit_to_op: 0,
                    fetch_to_op: 0,
                    floor: None,
                    peer: 1,
                    first_batch_offset: None,
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
            commit_to_op: 0,
            fetch_to_op: 0,
            floor: None,
            peer: 0,
            first_batch_offset: None,
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
                commit_to_op: commit_max,
                fetch_to_op: commit_max,
                floor: None,
                peer: 0,
                first_batch_offset: None,
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
            encryptor: None,
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
