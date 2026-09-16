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

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::task::Waker;

use consensus::PartitionsHandle;
use crossfire::{RecvError, TryRecvError};
use journal::local_gate::OwnedLocalGateGuard;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{
    CapturedPartitionIo, PartitionIncarnation, PartitionIoIdentity, PartitionIoResources,
    PartitionIoResult,
};
use server_common::sharding::IggyNamespace;
use thiserror::Error;

use crate::{IggyShard, Receiver, Sender, channel};

pub const DEFAULT_PARTITION_IO_CAPACITY: usize = 16;
pub const DEFAULT_PARTITION_IO_BYTES: usize = 256 * 1024 * 1024;
pub const PARTITION_IO_CAPACITY_MAX: usize = 1 << 20;

#[derive(Clone, Copy, Debug)]
pub struct PartitionIoLimits {
    capacity: usize,
    bytes_max: usize,
}

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
        let bytes_max = bytes_max.unwrap_or_else(|| DEFAULT_PARTITION_IO_BYTES.max(minimum));
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
    pub const fn capacity(self) -> usize {
        self.capacity
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
    charge: usize,
    result: RefCell<Option<PartitionIoResult>>,
    resources: RefCell<Option<PartitionIoResources<SB>>>,
    gate: RefCell<Option<OwnedLocalGateGuard>>,
    quiescence: RefCell<Option<Rc<partitions::PartitionIoQuiescence>>>,
}

#[derive(Default)]
struct ReadyPartitions {
    queue: RefCell<VecDeque<(IggyNamespace, PartitionIncarnation)>>,
    present: RefCell<BTreeSet<(IggyNamespace, PartitionIncarnation)>>,
    waker: RefCell<Option<Waker>>,
}

#[derive(Default)]
struct IoCounters {
    active: Cell<usize>,
    queued: Cell<usize>,
    quarantined: Cell<usize>,
}

impl ReadyPartitions {
    fn notify(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        if !self.present.borrow_mut().insert((namespace, incarnation)) {
            return;
        }
        self.queue.borrow_mut().push_back((namespace, incarnation));
        let waker = self.waker.borrow().clone();
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
    sender: Sender<PartitionIoToken>,
    receiver: Receiver<PartitionIoToken>,
    ready: Rc<ReadyPartitions>,
    interrupted: Rc<Cell<bool>>,
    undelivered: Rc<Cell<bool>>,
    closed: Cell<bool>,
    capacity_blocked: Cell<bool>,
    counters: Rc<IoCounters>,
    #[cfg(test)]
    execution_gate: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl<SB: SuperblockStore> PartitionIoLane<SB> {
    pub(crate) fn new(limits: PartitionIoLimits) -> Self {
        let (sender, receiver) = channel(limits.capacity);
        Self {
            limits,
            slots: RefCell::new((0..limits.capacity).map(|_| None).collect()),
            charged: Cell::new(0),
            sender,
            receiver,
            ready: Rc::default(),
            interrupted: Rc::new(Cell::new(false)),
            undelivered: Rc::new(Cell::new(false)),
            closed: Cell::new(false),
            capacity_blocked: Cell::new(false),
            counters: Rc::default(),
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
        self.head().is_some() || self.undelivered.get() || self.interrupted.get()
    }

    pub(crate) fn head(&self) -> Option<(IggyNamespace, PartitionIncarnation)> {
        if !self.capacity_blocked.get() {
            return self.ready.queue.borrow().front().copied();
        }
        let present = self.ready.present.borrow();
        self.slots.borrow().iter().flatten().find_map(|slot| {
            (slot.state.get() == SlotState::Settled
                && present.contains(&(slot.namespace, slot.incarnation)))
            .then_some((slot.namespace, slot.incarnation))
        })
    }

    pub(crate) fn pop_ready(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        self.ready
            .queue
            .borrow_mut()
            .retain(|queued| *queued != (namespace, incarnation));
        self.ready
            .present
            .borrow_mut()
            .remove(&(namespace, incarnation));
    }

    pub(crate) fn reschedule(&self, namespace: IggyNamespace, incarnation: PartitionIncarnation) {
        self.ready.notify(namespace, incarnation);
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
        self.counters.active.set(self.counters.active.get() + 1);
        let marker = InterruptionMarker {
            slot: Rc::clone(&slot),
            interrupted: Rc::clone(&self.interrupted),
            quiescence,
            counters: Rc::clone(&self.counters),
        };
        let sender = self.sender.clone();
        let undelivered = Rc::clone(&self.undelivered);
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
            *slot.result.borrow_mut() = Some(result);
            slot.state.set(SlotState::Queued);
            counters.active.set(counters.active.get() - 1);
            counters.queued.set(counters.queued.get() + 1);
            if sender
                .try_send(PartitionIoToken {
                    slot: index,
                    identity,
                })
                .is_err()
            {
                undelivered.set(true);
                if let Some(waker) = ready.waker.borrow().as_ref() {
                    waker.wake_by_ref();
                }
            }
            drop(marker);
        });
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn recv(&self) -> Result<PartitionIoToken, RecvError> {
        self.receiver.recv().await
    }

    pub(crate) fn try_recv(&self) -> Result<PartitionIoToken, TryRecvError> {
        self.receiver.try_recv().or_else(|error| {
            if !self.undelivered.get() {
                return Err(error);
            }
            self.slots
                .borrow()
                .iter()
                .enumerate()
                .find_map(|(index, slot)| {
                    let slot = slot.as_ref()?;
                    (slot.state.get() == SlotState::Queued).then(|| PartitionIoToken {
                        slot: index,
                        identity: slot.identity.get().expect("queued slot has an identity"),
                    })
                })
                .ok_or_else(|| {
                    self.undelivered.set(false);
                    error
                })
        })
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
            self.capacity_blocked.set(false);
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

    fn record_metrics(&self, metrics: &crate::metrics::ShardMetrics) {
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
    pub(crate) async fn service_partition_io(&self) -> bool {
        self.partition_io.record_metrics(&self.metrics);
        let progressed = if let Ok(token) = self.partition_io.try_recv() {
            self.accept_partition_io_completion(token);
            self.cooperate().await;
            true
        } else {
            false
        };
        for identity in self.partition_io.interrupted() {
            if let Some(partition) = self.plane.partitions().get_io_owner(&identity.namespace) {
                partition.interrupt_io(identity);
            }
            self.cooperate().await;
        }
        let Some((namespace, incarnation)) = self.partition_io.head() else {
            return progressed;
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
            return true;
        };
        let step = partition.resume_io(partitions.config()).await;
        if let Some(token) = self.partition_io.retained(namespace, incarnation)
            && !partition.retains_io_reservation(token.identity)
        {
            self.partition_io.release(token.slot);
        }
        match step {
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
            partitions::PartitionIoStep::PurgeFinished {
                generation,
                outcome,
            } => {
                self.partition_io.pop_ready(namespace, incarnation);
                self.drop_partition_transfer_state(namespace, partition);
                match outcome {
                    Ok(()) => self.partition_io.reschedule(namespace, incarnation),
                    Err(partitions::PurgeError::Unserviceable(error)) => {
                        tracing::error!(namespace_raw = namespace.inner(), generation, %error, "partition purge failed after mutation");
                        self.fence_partition_for_rebuild(namespace, partition, Some(0));
                    }
                    Err(error) => {
                        tracing::warn!(namespace_raw = namespace.inner(), generation, %error, "partition purge remains unapplied; reconciler will retry");
                    }
                }
            }
            partitions::PartitionIoStep::ViewApplied { actions, peer } => {
                self.partition_io.pop_ready(namespace, incarnation);
                if let Some(peer) = peer {
                    self.finish_partition_view_adoption(namespace, peer, actions)
                        .await;
                } else {
                    self.advance_pending_partition_view(namespace).await;
                }
            }
            partitions::PartitionIoStep::Transition(message) => {
                self.partition_io.pop_ready(namespace, incarnation);
                self.on_message(message).await;
            }
            partitions::PartitionIoStep::Progress => {
                self.partition_io.pop_ready(namespace, incarnation);
                self.partition_io.reschedule(namespace, incarnation);
            }
            partitions::PartitionIoStep::Pending => {
                self.partition_io.pop_ready(namespace, incarnation);
            }
            partitions::PartitionIoStep::Ready(plan) => {
                let Some(index) =
                    self.partition_io
                        .try_reserve(namespace, incarnation, plan.allocation_charge)
                else {
                    self.partition_io.capacity_blocked.set(true);
                    return progressed;
                };
                self.partition_io.pop_ready(namespace, incarnation);
                match partition.capture_io(plan, partitions.config()) {
                    Ok(Some(captured)) => {
                        if matches!(
                            plan.continuation,
                            partitions::PartitionIoContinuation::Retention
                                | partitions::PartitionIoContinuation::Purge
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
        true
    }
}

struct InterruptionMarker<SB> {
    slot: Rc<PartitionIoSlot<SB>>,
    interrupted: Rc<Cell<bool>>,
    quiescence: Rc<partitions::PartitionIoQuiescence>,
    counters: Rc<IoCounters>,
}

impl<SB> Drop for InterruptionMarker<SB> {
    fn drop(&mut self) {
        if self.slot.state.get() == SlotState::Running {
            self.slot.state.set(SlotState::Interrupted);
            self.interrupted.set(true);
            self.quiescence.interrupt();
            self.counters.active.set(self.counters.active.get() - 1);
            self.counters
                .quarantined
                .set(self.counters.quarantined.get() + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    use consensus::{LocalPipeline, PartitionsHandle, VsrConsensus};
    use futures::FutureExt;
    use futures::channel::oneshot;
    use iggy_binary_protocol::primitives::consumer::WireConsumer;
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::{
        AckLevel, Command, ConsensusHeader, GenericHeader, Operation, ReplyHeader,
        RoutedRequestHeader, StartViewChangeHeader, WireEncode, WireIdentifier,
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
    use partitions::{
        IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionPathLayout,
        PartitionsConfig, RepairConclusion, RepairSession, Segment,
    };
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
    use server_common::{Message, SegmentStorage};

    use crate::metrics::ShardMetrics;
    use crate::shards_table::{PapayaShardsTable, ShardsTable};
    use crate::{
        IggyShard, LifecycleFrame, PartitionConsensusConfig, Receiver, ReplicaTopology, ShardFrame,
        ShardIdentity, TaggedSender, channel, shard_channel,
    };

    const SEGMENT_BYTES: u64 = 1024 * 1024;
    const TEST_PARTITIONS: usize = 3;
    const REPLY_DEADLINE: Duration = Duration::from_secs(1);
    const TASK_POLL_INTERVAL: Duration = Duration::from_millis(1);

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
                for purge in [false, true] {
                    Box::pin(held_file_job_allows_progress(operation, durability, purge)).await;
                }
            }
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn held_file_job_allows_progress(
        operation: Operation,
        durability: Durability,
        purge: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let bus = Rc::new(IggyMessageBus::new(0));
        let (owner, sender) = test_owner(&bus, None);
        let (release, held) = oneshot::channel();
        *owner.partition_io.execution_gate.borrow_mut() = Some(held);
        let namespace = IggyNamespace::new(0, 0, 0);
        let partitions = owner.plane.partitions();
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
        partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
        let log_path = directory.path().join("00000000000000000000.log");
        let index_path = directory.path().join("00000000000000000000.index");
        let messages = MessagesWriter::new(
            log_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            durability.is_persisted(),
            false,
            None,
        )
        .await
        .unwrap();
        let indexes = IggyIndexWriter::new(
            index_path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            durability.is_persisted(),
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
        let request = if operation == Operation::StoreConsumerOffset {
            let consumer_path = directory.path().join("consumer_offsets");
            let group_path = directory.path().join("consumer_group_offsets");
            std::fs::create_dir(&consumer_path).unwrap();
            std::fs::create_dir(&group_path).unwrap();
            partition.configure_consumer_offset_storage(
                consumer_path.to_str().unwrap().to_owned(),
                group_path.to_str().unwrap().to_owned(),
                ConsumerOffsets::with_capacity(1),
                ConsumerGroupOffsets::with_capacity(1),
            );
            partition.stats.increment_messages_count(1);
            store_request(namespace)
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
            assert!(futures::poll!(first.as_mut()).is_pending());
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
            if purge {
                let partition = partitions.get_mut_by_ns(&namespace).unwrap();
                assert!(matches!(
                    partition.purge(partitions.config(), 1).await,
                    Err(partitions::PurgeError::Pending)
                ));
                assert_eq!(partition.applied_purge_generation(), 0);
            }
            stop.try_send(()).unwrap();
            compio::time::sleep(TASK_POLL_INTERVAL).await;
            assert_eq!(owner.partition_io.outstanding(), 1);
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
            let reply: Message<ReplyHeader> = first.await.unwrap().try_into_typed().unwrap();
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
        if purge {
            assert_eq!(
                partitions
                    .get_by_ns(&namespace)
                    .unwrap()
                    .applied_purge_generation(),
                1
            );
            assert_eq!(std::fs::metadata(log_path).unwrap().len(), 0);
            assert!(!directory.path().join("consumer_offsets/1").exists());
        } else if operation == Operation::SendMessages {
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
                consensus::PIPELINE_PREPARE_QUEUE_MAX
            );
            assert_eq!(
                partitions
                    .get_by_ns(&first_namespace)
                    .unwrap()
                    .consensus()
                    .commit_min(),
                next_request - 1,
                "new self-acks cannot enter the unfinished round"
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
            assert_eq!(owner.process_loopback(&mut round).await, 1);
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
        owner.partition_io =
            super::PartitionIoLane::new(super::PartitionIoLimits::new(1, None).unwrap());
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
            owner.fence_partition_for_rebuild(namespace, partition, None);
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
            compio::time::timeout(REPLY_DEADLINE, finish).await.unwrap();
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
    async fn disconnected_completion_channel_preserves_local_result_for_acceptance() {
        let store = Rc::new(HeldSuperblock {
            entered: RefCell::new(None),
            held: RefCell::new(None),
        });
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut owner, _sender) = test_owner(&bus, Some(&store));
        let captured_bus = crate::poll::timeout_tests::PollTestBus::default();
        let (slot, captured) = capture_first_write(&owner).await;
        owner.partition_io.receiver = channel(1).1;
        owner.partition_io.dispatch(slot, captured, &captured_bus);
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
        lane.capacity_blocked.set(true);
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
    fn oldest_large_reservation_waits_without_allowing_smaller_jobs_to_overtake() {
        let limits = super::PartitionIoLimits::new(TEST_PARTITIONS, None).unwrap();
        let lane = super::PartitionIoLane::<HeldSuperblock>::new(limits);
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
        lane.capacity_blocked.set(true);
        assert!(
            lane.head().is_none(),
            "new reservations wait for the oldest eligible job"
        );
        lane.release(first);
        assert_eq!(lane.head(), Some(identities[1]));
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
            Rc::new(|_, _| {}),
            Rc::new(|_, _| {}),
            Rc::new(|_| {}),
            Rc::new(|_| {}),
            metadata,
            partitions,
            vec![sender.clone()],
            inbox,
            replies,
            2,
            routes,
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
            None,
            ShardMetrics::for_shard(),
        )
        .expect("valid shard wiring");
        (owner, sender)
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
                client: 1,
                session: 1,
                request,
                group: namespace.inner(),
                ..Default::default()
            })
            .unwrap()
    }

    fn store_request(namespace: IggyNamespace) -> Message<RoutedRequestHeader> {
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer {
                kind: ConsumerKind::Consumer.as_code(),
                id: WireIdentifier::Numeric(1),
            },
            stream_id: WireIdentifier::Numeric(namespace.stream_id().try_into().unwrap()),
            topic_id: WireIdentifier::Numeric(namespace.topic_id().try_into().unwrap()),
            partition_id: Some(namespace.partition_id().try_into().unwrap()),
            offset: 0,
            ack: AckLevel::NoAck,
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
