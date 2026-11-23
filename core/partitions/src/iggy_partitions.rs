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

#![allow(dead_code)]

use crate::iggy_partition::ReliefFlush;
use crate::poll_plan::{PollPlan, PollReadResult};
use crate::repair_memory::{Relief, RepairRingShed, RingCharge, RingTicket};
use crate::state_transfer::TransferSlot;
use crate::stream_keyring::{StreamKeyIdentity, StreamKeyring};
use crate::types::PartitionsConfig;
use crate::{IggyPartition, Partition, PollingArgs, PollingConsumer};
use crate::{PollCompletion, PollReplication};
use ahash::AHashMap;
use consensus::{
    Consensus, Plane, PlaneIdentity, TickClock, VsrConsensus, build_deny_reply_from_request_header,
};
use iggy_binary_protocol::{
    Command, ConsensusHeader, Operation, PrepareHeader, PrepareOkHeader, ReplyHeader,
    RoutedRequestHeader,
};
use iggy_common::{EncryptorKind, IggyError, ServerKeyContext, StreamEncryption};
use iggy_object_store::PositionHolder;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use message_bus::MessageBus;
use server_common::Message;
use server_common::send_messages::{ChecksumMode, convert_request_message, encrypt_batch_request};
use server_common::sharding::{IggyNamespace, LocalIdx, ShardId};
use std::cell::Cell;
use std::cell::{RefCell, UnsafeCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;
use tracing::{info, warn};

/// RAII counter for live [`IggyPartitions::with_partition`] borrows. The
/// decrement runs on `Drop`, so a panic inside the closure still restores the
/// count instead of poisoning the `remove` tripwire for the rest of the
/// process.
#[cfg(debug_assertions)]
struct BorrowGuard<'a>(&'a Cell<u32>);

#[cfg(debug_assertions)]
impl<'a> BorrowGuard<'a> {
    fn new(cell: &'a Cell<u32>) -> Self {
        cell.set(cell.get() + 1);
        Self(cell)
    }
}

#[cfg(debug_assertions)]
impl Drop for BorrowGuard<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// Per-shard collection of all partitions.
///
/// This struct manages ALL partitions assigned to a single shard, regardless
/// of which stream/topic they belong to.
///
/// The partitions live in a slab. A partition keeps its slot, its
/// [`LocalIdx`], from insert to removal, and a removal frees the slot for a
/// later insert. The slot is NOT the `partition_id` within `IggyNamespace`:
/// shard 0 might hold `partition_ids` [0, 2, 4] in slots [0, 1, 2].
pub struct IggyPartitions<B, SB = PingPongSuperblock>
where
    B: MessageBus,
{
    shard_id: ShardId,
    config: PartitionsConfig,
    /// The slab, indexed by `LocalIdx`. A removed partition leaves `None`.
    ///
    /// # Safety invariant
    ///
    /// Only [`Self::insert`] and [`Self::remove`] take a `&mut` to the array,
    /// and they run only on the shard's pump task, never during a walk over
    /// the slots. Reconciler routes mutations through `ReconcileOp` +
    /// `ReconcileApply`. Cross-task access would be UB under cooperative
    /// `.await` interleaving.
    slots: UnsafeCell<Vec<Option<Slot<B, SB>>>>,
    /// Vacant slots, reused last freed first.
    free_slots: RefCell<Vec<LocalIdx>>,
    /// The tick of `tick_clock` at which the shard tick next visits each
    /// slot, by `LocalIdx`: [`WAKE_NOW`] for a slot that the next tick
    /// visits, [`WAKE_NEVER`] for a vacant one. Dense and apart from the
    /// slots, because every tick scans it whole.
    ///
    /// A slot sleeps only after [`Self::settle`], which the shard calls with
    /// a tick for a partition with nothing to do before it. Every accessor
    /// that hands out a partition wakes its slot, so no path that reaches a
    /// partition through this table can leave it asleep with new work.
    wake_at: RefCell<Vec<Cell<u64>>>,
    /// Sum of the [`GaugeShare`] of every slot.
    gauge_sums: Cell<GaugeShare>,
    /// Slot of each namespace. Never iterated, so its per-process hash order
    /// cannot reach the simulator schedule: every walk follows the slots.
    namespace_to_local: RefCell<AHashMap<IggyNamespace, LocalIdx>>,
    /// Tombstone gate: reconciler sets it synchronously before awaiting
    /// disk delete; pump clears on `ConfirmRemove`. Pump's `Plane::on_*`
    /// short-circuits frames hitting a tombstoned namespace.
    ///
    /// `RefCell` (not `UnsafeCell`) because both the reconciler task and
    /// the pump task mutate it, so the single-pump invariant protecting
    /// `slots` does NOT hold here. Compio's per-shard runtime is
    /// single-threaded, so runtime borrow checks suffice; callers must not
    /// hold a borrow across `.await`.
    tombstoned: RefCell<AHashMap<IggyNamespace, Option<u64>>>,
    /// Stream encryptors by stream incarnation, opened with this node's keys.
    /// Same single-threaded borrow rules as `tombstoned`.
    keyring: RefCell<StreamKeyring>,
    /// Reconciler fences must not borrow a partition held by the pump across await.
    teardown: RefCell<AHashMap<IggyNamespace, crate::PartitionTeardown>>,
    /// Namespaces whose consumer group offsets need a sweep, shared with every
    /// partition here. Same single-threaded borrow rules as `tombstoned`.
    consumer_group_offsets_dirty: Rc<RefCell<BTreeSet<IggyNamespace>>>,
    fence_published: Rc<Cell<bool>>,
    persistence_notifier: RefCell<Option<crate::PersistenceNotifier>>,
    io_notifier: RefCell<Option<(crate::PartitionIoNotifier, usize)>>,
    /// Where the partitions here read their remote segments from. `None` on a
    /// shard without an object store.
    remote_segments: RefCell<Option<Rc<crate::RemoteSegments>>>,
    /// What the shard keeps of what it reads from object stores, which the
    /// reads of its own store and of its geo peers share.
    read_cache: RefCell<Option<Rc<crate::ReadCache>>>,
    /// Where the partitions that follow another cluster's log read it from,
    /// by the peer's origin name.
    peer_remote_segments: RefCell<Vec<(String, Rc<crate::RemoteSegments>)>>,
    repair_memory: Option<Arc<crate::RepairMemoryBudget>>,
    repair_rings: Option<Rc<crate::RepairRingBudget>>,
    /// The delays of the checkpoint rule. `None` leaves the rule off.
    checkpoint_delays: Option<crate::CheckpointDelays>,
    /// Partitions due for a checkpoint, oldest first. One entry per
    /// partition, see [`IggyPartition::in_checkpoint_queue`]. A due partition
    /// waits here asleep, so a burst of deadlines does not keep every one of
    /// them awake until a start slot frees.
    checkpoint_queue: RefCell<VecDeque<(IggyNamespace, crate::CheckpointTrigger)>>,
    /// Partitions that the queue requested a checkpoint for, until it ends.
    /// One entry per partition. Read from their state on every pop, so a
    /// cancel or a fence cannot leak a slot of
    /// [`QUEUED_CHECKPOINTS_IN_FLIGHT_MAX`].
    checkpoints_in_flight: RefCell<Vec<IggyNamespace>>,
    /// This shard's share of the WAL bytes above the checkpoints. `None`
    /// leaves the reload budget off.
    wal_reload_bytes_max: Option<u64>,
    /// The first-op stamps of the partitions, oldest first, as (namespace,
    /// tick). An entry whose partition no longer holds that stamp is stale.
    /// See [`Self::queue_reload_checkpoints`].
    reload_order: RefCell<VecDeque<(IggyNamespace, u64)>>,
    /// The partitions in the checkpoint queue or checkpointing that count
    /// toward the excess of the reload budget.
    reload_queued: RefCell<Vec<IggyNamespace>>,
    /// Receiving-side transfer sessions alive on this shard. Each holds a
    /// [`TransferSlot`] on this count.
    transfers_inflight: Rc<Cell<usize>>,
    /// The consensus timers of every partition here run on this clock.
    /// [`Self::insert`] installs it.
    tick_clock: TickClock,
    loopback_ready: Rc<LoopbackReady>,
    /// Debug-only tripwire: counts live [`Self::with_partition`] borrows so
    /// `remove` can assert no partition leaves while a sanctioned non-pump
    /// read borrow is outstanding. Cannot fire for correct code: the closure
    /// is `FnOnce` (so it cannot span an `.await`) and [`BorrowGuard`]
    /// decrements on unwind, so a panic inside it does not leave the count
    /// stuck.
    #[cfg(debug_assertions)]
    borrow_active: Cell<u32>,
}

/// A live slot of [`IggyPartitions`].
///
/// The `Box` keeps the partition at one address while the slot array grows.
/// The `UnsafeCell` lets a `&mut` to the partition come from a shared borrow
/// of the array, so a walk over the slots stays valid while the pump mutates
/// the partitions it visits.
struct Slot<B, SB>
where
    B: MessageBus,
{
    namespace: IggyNamespace,
    partition: Box<UnsafeCell<IggyPartition<B, SB>>>,
    sent_report: Cell<SentReport>,
    gauge_share: Cell<GaugeShare>,
    /// The first-op stamp of the partition that the reload order last got,
    /// held as tick + 1, so that `None` takes no more than 8 B.
    reload_stamp: Cell<Option<NonZeroU64>>,
}

/// A sleeping partition, as the check for missed wake-ups reads it.
pub struct Sleeper<'a, B, SB>
where
    B: MessageBus,
{
    pub namespace: IggyNamespace,
    pub partition: &'a IggyPartition<B, SB>,
    pub sent_report: SentReport,
    /// The tick at which the shard tick next visits it.
    pub wake_at: u64,
    /// Whether its recorded [`GaugeShare`] equals its levels now.
    pub gauge_share_current: bool,
}

/// A slot that the next shard tick visits.
const WAKE_NOW: u64 = 0;
/// A vacant slot. No tick reaches it.
const WAKE_NEVER: u64 = u64::MAX;

/// Checkpoints that the queue keeps requested or running on one shard. Each
/// partition has its own WAL writer, so this caps the WAL rewrites and their
/// syncs that run at the same time.
pub const QUEUED_CHECKPOINTS_IN_FLIGHT_MAX: usize = 64;

/// Queue entries one tick takes. An entry whose partition has nothing left
/// to cover costs one lookup and no start, and this bounds the tick when
/// many entries went stale, after a burst of checkpoints by the WAL size
/// trigger for example.
const CHECKPOINT_QUEUE_POPS_PER_TICK_MAX: usize = 256;

/// Partitions that the reload budget counts as queued on one shard: the
/// running checkpoints and as many waiting. The budget then feeds the queue
/// as fast as it drains, not with every partition at once.
const RELOAD_CHECKPOINTS_QUEUED_MAX: usize = 2 * QUEUED_CHECKPOINTS_IN_FLIGHT_MAX;

/// Entries of the reload order that one tick takes, stale ones too.
const RELOAD_ORDER_POPS_PER_TICK_MAX: usize = 256;

/// Entries of the reload order per partition before a push drops every
/// stale entry. A partition holds one stamp at a time, so the order then
/// keeps at most one entry per partition.
const RELOAD_ORDER_ENTRIES_PER_PARTITION_MAX: usize = 2;

/// Flushes that the journal budget of one shard asks per tick. Each writes a
/// segment batch of its own.
const RELIEF_FLUSHES_PER_TICK_MAX: usize = 16;

/// Partitions that one shed visits in a queue of relief or of commands. A
/// visit that takes nothing costs one lookup, and this bounds the tick when
/// many partitions cannot flush.
const SHED_VISITS_PER_TICK_MAX: usize = 256;

/// How long a skipped or asked flush counts as relief, in ticks: one view
/// change and its retry. After it, the shed drops for that relief.
pub const RELIEF_WAIT_TICKS: u64 = 2 * consensus::TimeoutManager::VIEW_CHANGE_STATUS_TICKS;

/// What one partition last added to the shard gauges that count sleeping
/// partitions too.
///
/// The shard tick visits only the partitions with work, so it cannot sum
/// these levels over all of them: each visit replaces the share of its
/// partition, and [`IggyPartitions::gauge_sums`] is the total.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GaugeShare {
    pub wal_disk_bytes: u64,
    pub wal_retained_bytes: u64,
    /// The retained bytes above the checkpoints, which a boot loads again.
    pub wal_reload_bytes: u64,
    pub repair_ring_entries: usize,
    pub repair_ring_bytes: u64,
}

impl GaugeShare {
    const fn replaced(self, old: Self, new: Self) -> Self {
        Self {
            wal_disk_bytes: self.wal_disk_bytes - old.wal_disk_bytes + new.wal_disk_bytes,
            wal_retained_bytes: self.wal_retained_bytes - old.wal_retained_bytes
                + new.wal_retained_bytes,
            wal_reload_bytes: self.wal_reload_bytes - old.wal_reload_bytes + new.wal_reload_bytes,
            repair_ring_entries: self.repair_ring_entries - old.repair_ring_entries
                + new.repair_ring_entries,
            repair_ring_bytes: self.repair_ring_bytes - old.repair_ring_bytes
                + new.repair_ring_bytes,
        }
    }
}

/// What the shard last sent shard 0 about one partition, so it reports again
/// only on a change.
///
/// The slot holds it, not [`IggyPartition`], which fills its allocator size
/// class. A removal drops it with the slot, and a reused slot starts from the
/// default, which owes a first report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SentReport {
    /// The view of the last placement report, and of the last status.
    pub view: u32,
    /// The in-sync nodes of the last status, 0 after a retraction.
    pub in_sync_nodes: u32,
    /// A hash of the counters of the last status. The counters themselves
    /// would cost 24 B more per partition. A change that keeps the hash, at
    /// odds of 2^-64 per check, waits for the next change.
    pub counters: u64,
    /// The shard tick that last checked the counters.
    pub checked_at: u64,
    /// A placement report went out.
    pub placed: bool,
    /// The last status went out from the acting primary, not as a retraction.
    pub primary: bool,
    /// The partition waits in the shard's report queue.
    pub queued: bool,
}

impl<B, SB> Slot<B, SB>
where
    B: MessageBus,
{
    fn partition(&self) -> &IggyPartition<B, SB> {
        // SAFETY: a shared borrow, sound while no `&mut` from
        // `IggyPartitions::get_mut` to this partition is live: the same pump
        // discipline that `&mut` relies on.
        unsafe { &*self.partition.get() }
    }
}

#[derive(Default)]
struct LoopbackReady {
    partitions: RefCell<BTreeMap<IggyNamespace, crate::PartitionIncarnation>>,
    waker: RefCell<Option<Waker>>,
}

impl LoopbackReady {
    fn wake(&self) {
        let waker = self.waker.borrow().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<B, SB> IggyPartitions<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    #[must_use]
    pub fn new(shard_id: ShardId, config: PartitionsConfig) -> Self {
        Self::with_capacity(shard_id, config, 0)
    }

    /// Reserves slots and index entries for `capacity` partitions.
    #[must_use]
    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        let keyring = RefCell::new(StreamKeyring::new(Arc::clone(&config.key_context)));
        Self {
            shard_id,
            config,
            slots: UnsafeCell::new(Vec::with_capacity(capacity)),
            free_slots: RefCell::new(Vec::new()),
            wake_at: RefCell::new(Vec::with_capacity(capacity)),
            gauge_sums: Cell::default(),
            namespace_to_local: RefCell::new(AHashMap::with_capacity(capacity)),
            tombstoned: RefCell::new(AHashMap::new()),
            keyring,
            teardown: RefCell::new(AHashMap::new()),
            consumer_group_offsets_dirty: Rc::default(),
            fence_published: Rc::default(),
            persistence_notifier: RefCell::new(None),
            io_notifier: RefCell::new(None),
            remote_segments: RefCell::new(None),
            read_cache: RefCell::new(None),
            peer_remote_segments: RefCell::default(),
            repair_memory: None,
            repair_rings: None,
            checkpoint_delays: None,
            checkpoint_queue: RefCell::default(),
            checkpoints_in_flight: RefCell::default(),
            wal_reload_bytes_max: None,
            reload_order: RefCell::default(),
            reload_queued: RefCell::default(),
            transfers_inflight: Rc::default(),
            tick_clock: TickClock::default(),
            loopback_ready: Rc::default(),
            #[cfg(debug_assertions)]
            borrow_active: Cell::new(0),
        }
    }

    /// Reserves unproven repairs and snapshot repair tails of every partition
    /// inserted from now on against `budget`, which the shards of the node
    /// share.
    #[must_use]
    pub fn with_repair_memory(mut self, budget: Arc<crate::RepairMemoryBudget>) -> Self {
        self.repair_memory = Some(budget);
        self
    }

    /// Charges the repair ring of every partition inserted from now on to
    /// `budget`, which this shard owns. [`Self::shed_repair_rings`] enforces
    /// it.
    #[must_use]
    pub fn with_repair_rings(mut self, budget: Rc<crate::RepairRingBudget>) -> Self {
        self.repair_rings = Some(budget);
        self
    }

    /// Turns on the checkpoint rule with `delays` for the partitions of this
    /// shard.
    #[must_use]
    pub const fn with_checkpoint_delays(mut self, delays: crate::CheckpointDelays) -> Self {
        self.checkpoint_delays = Some(delays);
        self
    }

    /// The delays of the checkpoint rule. `None` leaves the rule off.
    #[must_use]
    pub const fn checkpoint_delays(&self) -> Option<crate::CheckpointDelays> {
        self.checkpoint_delays
    }

    /// Turns on the reload budget with `bytes_max`, this shard's share of the
    /// WAL bytes above the checkpoints.
    #[must_use]
    pub const fn with_wal_reload_budget(mut self, bytes_max: u64) -> Self {
        self.wal_reload_bytes_max = Some(bytes_max);
        self
    }

    /// This shard's share of the WAL bytes above the checkpoints. `None`
    /// leaves the reload budget off.
    #[must_use]
    pub const fn wal_reload_bytes_max(&self) -> Option<u64> {
        self.wal_reload_bytes_max
    }

    /// Receiving-side transfer sessions alive on this shard. A partition
    /// removed or tombstoned during its transfer counts until it drops.
    #[must_use]
    pub fn transfers_inflight(&self) -> usize {
        self.transfers_inflight.get()
    }

    #[must_use]
    pub fn transfer_slot(&self) -> TransferSlot {
        TransferSlot::new(&self.transfers_inflight)
    }

    /// The clock of the partition timers on this shard. The shard tick
    /// advances it once per tick, and the shard reads its tick count from it.
    #[must_use]
    pub const fn tick_clock(&self) -> &TickClock {
        &self.tick_clock
    }

    /// Puts a partition that came due at the back of the checkpoint queue.
    /// [`IggyPartition::take_due_checkpoint`] returns the trigger only for a
    /// partition outside the queue.
    pub fn enqueue_checkpoint(&self, namespace: IggyNamespace, trigger: crate::CheckpointTrigger) {
        self.checkpoint_queue
            .borrow_mut()
            .push_back((namespace, trigger));
    }

    /// Requests checkpoints for the partitions at the front of the queue:
    /// at most `starts_max`, and only while fewer than
    /// `QUEUED_CHECKPOINTS_IN_FLIGHT_MAX` are in flight. Returns how many it
    /// requested. A request that waits for its start can come due again, and
    /// then takes no second slot.
    ///
    /// Runs before the tick takes its namespaces. A requested partition is
    /// woken, so the same tick can start its checkpoint.
    pub fn request_queued_checkpoints(&self, starts_max: usize) -> usize {
        let mut in_flight = self.checkpoints_in_flight.borrow_mut();
        in_flight.retain(|namespace| {
            self.find(*namespace)
                .is_some_and(IggyPartition::queued_checkpoint_in_flight)
        });
        let mut queue = self.checkpoint_queue.borrow_mut();
        let mut requested = 0;
        let mut pops = 0;
        while requested < starts_max
            && in_flight.len() < QUEUED_CHECKPOINTS_IN_FLIGHT_MAX
            && pops < CHECKPOINT_QUEUE_POPS_PER_TICK_MAX
            && let Some((namespace, trigger)) = queue.pop_front()
        {
            pops += 1;
            if self
                .find(namespace)
                .is_some_and(|partition| partition.request_queued_checkpoint(trigger))
            {
                self.wake_namespace(namespace);
                if !in_flight.contains(&namespace) {
                    in_flight.push(namespace);
                    requested += 1;
                }
            }
        }
        requested
    }

    #[must_use]
    pub fn checkpoint_queue_len(&self) -> usize {
        self.checkpoint_queue.borrow().len()
    }

    #[must_use]
    pub fn queued_checkpoints_in_flight(&self) -> usize {
        self.checkpoints_in_flight.borrow().len()
    }

    /// Queues checkpoints while the WAL bytes above the checkpoints of this
    /// shard pass its reload budget: for the partitions whose first op since
    /// their checkpoint is oldest, until the bytes of the queued partitions
    /// cover the excess. Returns how many it queued, 0 without a budget.
    ///
    /// Runs after the settle pass of a tick, so the sums and the shares it
    /// reads agree. A partition that waits in the queue already counts. One
    /// with a running checkpoint or with only uncommitted ops is armed again,
    /// as at its deadline. At most `RELOAD_CHECKPOINTS_QUEUED_MAX` queued
    /// partitions count.
    pub fn queue_reload_checkpoints(&self) -> usize {
        let Some(bytes_max) = self.wal_reload_bytes_max else {
            return 0;
        };
        let mut counted = self.reload_queued.borrow_mut();
        counted.retain(|namespace| self.reload_checkpoint_pending(*namespace));
        let mut covered: u64 = counted
            .iter()
            .filter_map(|namespace| self.slot(*namespace))
            .map(|slot| slot.gauge_share.get().wal_reload_bytes)
            .sum();
        let excess = self
            .gauge_sums
            .get()
            .wal_reload_bytes
            .saturating_sub(bytes_max);
        let now = self.tick_clock.now();
        let mut queued = 0;
        for _ in 0..RELOAD_ORDER_POPS_PER_TICK_MAX {
            let Some((namespace, tick)) = self.reload_order.borrow().front().copied() else {
                break;
            };
            // An entry of this tick waits for the next one, so a partition
            // armed again here is not taken twice in one pass.
            if tick >= now {
                break;
            }
            let holder = self
                .slot(namespace)
                .filter(|slot| slot.partition().checkpoint_first_op_tick() == Some(tick))
                .filter(|_| !self.is_tombstoned(&namespace));
            if holder.is_some()
                && (covered >= excess || counted.len() >= RELOAD_CHECKPOINTS_QUEUED_MAX)
            {
                break;
            }
            self.reload_order.borrow_mut().pop_front();
            let Some(slot) = holder else {
                continue;
            };
            let partition = slot.partition();
            if let Some(trigger) =
                partition.queue_stamped_checkpoint(now, crate::CheckpointTrigger::Reload)
            {
                self.enqueue_checkpoint(namespace, trigger);
                queued += 1;
            }
            if partition.in_checkpoint_queue() && !counted.contains(&namespace) {
                counted.push(namespace);
                covered += slot.gauge_share.get().wal_reload_bytes;
            }
            self.track_reload_stamp(slot);
        }
        queued
    }

    /// Whether a partition that the reload budget counts still frees its
    /// bytes, or its share in the sums is not yet the level it has now. A
    /// checkpoint that ended leaves the count in the tick that its share
    /// leaves the sums.
    fn reload_checkpoint_pending(&self, namespace: IggyNamespace) -> bool {
        self.find(namespace).is_some_and(|partition| {
            partition.in_checkpoint_queue() || partition.queued_checkpoint_in_flight()
        }) || self
            .slot(namespace)
            .is_some_and(|slot| slot.gauge_share.get() != slot.partition().gauge_share())
    }

    /// Puts a new first-op stamp of the partition in `slot` at the back of
    /// the reload order. Settles run in slot order, so the order can be one
    /// tick out of stamp order, and only an equal tick names the same stamp.
    fn track_reload_stamp(&self, slot: &Slot<B, SB>) {
        if self.wal_reload_bytes_max.is_none() {
            return;
        }
        let stamp = slot.partition().checkpoint_first_op_tick();
        let held = stamp.map(|tick| NonZeroU64::MIN.saturating_add(tick));
        if slot.reload_stamp.replace(held) == held {
            return;
        }
        let Some(tick) = stamp else {
            return;
        };
        let mut order = self.reload_order.borrow_mut();
        order.push_back((slot.namespace, tick));
        let partitions = self.namespace_to_local.borrow().len();
        if order.len() > RELOAD_ORDER_ENTRIES_PER_PARTITION_MAX * partitions {
            order.retain(|(namespace, tick)| {
                self.find(*namespace)
                    .is_some_and(|partition| partition.checkpoint_first_op_tick() == Some(*tick))
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn reload_order_len(&self) -> usize {
        self.reload_order.borrow().len()
    }

    /// The slot of `namespace`, tombstoned or not.
    fn slot(&self, namespace: IggyNamespace) -> Option<&Slot<B, SB>> {
        self.slots().get(*self.local_idx(&namespace)?)?.as_ref()
    }

    /// Keeps the partitions that a graceful stop can still checkpoint, at
    /// most `reach`, and holds the stop of each one until its checkpoint
    /// starts. The requests that wait for their start come first, then the
    /// oldest queue entries. If the queue is shorter, the partitions with a
    /// committed op above their checkpoint follow, the oldest first op first.
    /// Returns how many partitions need a checkpoint that the stop cannot
    /// serve.
    ///
    /// Every other partition passes the hold at once, so it does not wait
    /// for a start that cannot come.
    pub fn plan_stop_checkpoints(&self, reach: usize) -> usize {
        let waiting: Vec<IggyNamespace> = self
            .checkpoints_in_flight
            .borrow()
            .iter()
            .copied()
            .filter(|namespace| {
                self.find(*namespace)
                    .is_some_and(IggyPartition::needs_stop_checkpoint)
            })
            .collect();
        let reach = reach.saturating_sub(waiting.len());
        let mut queue = self.checkpoint_queue.borrow_mut();
        // No partition enters the queue again in the stop, so the queue flags
        // of the dropped entries can stay set.
        queue.retain(|(namespace, _)| {
            self.find(*namespace)
                .is_some_and(IggyPartition::needs_stop_checkpoint)
        });
        let dropped = queue.len().saturating_sub(reach);
        queue.truncate(reach);
        let mut extra: Vec<_> = self
            .slots()
            .iter()
            .flatten()
            .filter(|slot| {
                !self.is_tombstoned(&slot.namespace) && !waiting.contains(&slot.namespace)
            })
            .map(|slot| (slot.namespace, slot.partition()))
            .filter(|(_, partition)| {
                partition.needs_stop_checkpoint() && !partition.in_checkpoint_queue()
            })
            .map(|(namespace, partition)| {
                (
                    partition.checkpoint_first_op_tick().unwrap_or(u64::MAX),
                    namespace,
                )
            })
            .collect();
        extra.sort_unstable();
        let room = reach - queue.len();
        let unserved = dropped + extra.len().saturating_sub(room);
        extra.truncate(room);
        for (_, namespace) in extra {
            if let Some(partition) = self.find(namespace) {
                partition.join_checkpoint_queue();
            }
            queue.push_back((namespace, crate::CheckpointTrigger::Stop));
        }
        let served = waiting
            .iter()
            .chain(queue.iter().map(|(namespace, _)| namespace));
        for namespace in served {
            if let Some(partition) = self.find(*namespace) {
                partition.hold_shutdown_for_checkpoint();
            }
        }
        unserved
    }

    /// Ends the stop hold of every queued partition and empties the queue:
    /// no checkpoint starts for them in this stop. Returns how many still
    /// held, so their checkpoint did not start.
    pub fn release_stop_checkpoints(&self) -> usize {
        let queue = std::mem::take(&mut *self.checkpoint_queue.borrow_mut());
        let mut released = queue
            .iter()
            .filter(|(namespace, _)| {
                self.find(*namespace)
                    .is_some_and(IggyPartition::release_checkpoint_hold)
            })
            .count();
        for namespace in self.checkpoints_in_flight.borrow().iter() {
            released += usize::from(
                self.find(*namespace)
                    .is_some_and(IggyPartition::release_checkpoint_hold),
            );
        }
        released
    }

    /// Sheds journal memory once the shard is past its budget, and reports
    /// what it took. `None` without a budget.
    ///
    /// 1. The rings that grew earliest release their oldest bodies to
    ///    segments, down to the low watermark of the budget. Repair still
    ///    serves a released entry, from disk.
    /// 2. While the asked flushes bring less than the excess, the journals
    ///    whose committed messages waited longest get a flush through their
    ///    commit point. It moves those messages to the ring, where step 1
    ///    releases their bodies.
    /// 3. For the excess that no counted relief covers, the rings with no
    ///    body left to release drop their oldest entries, and then the
    ///    journals whose commands grew earliest release checkpointed
    ///    commands. Either sends a lagging replica to state transfer. Every
    ///    ring keeps its newest entry, and every journal the command at its
    ///    commit point.
    pub fn shed_repair_rings(&self) -> Option<RepairRingShed> {
        let budget = self.repair_rings.as_deref()?;
        budget.retain_live(|ticket| self.ring_charge_of(ticket).map(|charge| charge.relief()));
        let mut bodies_released = 0;
        let mut entries_dropped = 0;
        if budget.excess().is_some() {
            // A visit that keeps its ticket took the bytes it was asked for,
            // so every pass takes a ticket out of its queue or is the last.
            while let Some(excess) = budget.excess_over_low_watermark()
                && let Some(ticket) = budget.oldest_grown()
            {
                let visit = self.shed_ring_owner(ticket).map(|partition| {
                    partition
                        .log
                        .journal()
                        .inner
                        .release_ring_bodies(excess, partition.pinned_transfer_tail())
                });
                bodies_released += visit.map_or(0, |visit| visit.released);
                entries_dropped += visit.map_or(0, |visit| visit.dropped);
                if visit.is_some_and(|visit| visit.left) {
                    break;
                }
                budget.pop_grown();
            }
        }
        self.expire_relief(budget);
        let flushes_asked = self.ask_relief_flushes(budget);
        while let Some(excess) = budget.costly_excess()
            && let Some(ticket) = budget.oldest_released()
        {
            let visit = self.shed_ring_owner(ticket).map(|partition| {
                partition
                    .log
                    .journal()
                    .inner
                    .drop_ring_entries(excess, partition.pinned_transfer_tail())
            });
            entries_dropped += visit.map_or(0, |visit| visit.dropped);
            if visit.is_some_and(|visit| visit.left) {
                break;
            }
            budget.pop_released();
        }
        let (commands_released, checkpoints_asked) = self.release_commands(budget);
        self.report_ring_floor(budget);
        Some(RepairRingShed {
            bodies_released,
            entries_dropped,
            flushes_asked,
            checkpoints_asked,
            commands_released,
            usage: budget.usage(),
        })
    }

    /// Stops counting the relief of the skips and asks that waited
    /// [`RELIEF_WAIT_TICKS`].
    fn expire_relief(&self, budget: &crate::RepairRingBudget) {
        let now = self.tick_clock.now();
        while let Some((ticket, stamp)) = budget.pop_expired_stamp(now, RELIEF_WAIT_TICKS) {
            if let Some(charge) = self.ring_charge_of(ticket) {
                charge.expire_relief(stamp);
            }
        }
    }

    /// Step 2 of [`Self::shed_repair_rings`]. Returns how many flushes it
    /// asked.
    fn ask_relief_flushes(&self, budget: &crate::RepairRingBudget) -> usize {
        let now = self.tick_clock.now();
        let mut asked = 0;
        let mut visits = budget.relief_waiting_len().min(SHED_VISITS_PER_TICK_MAX);
        while visits > 0
            && asked < RELIEF_FLUSHES_PER_TICK_MAX
            && budget.wants_flushes()
            && let Some(ticket) = budget.pop_relief_waiting()
        {
            visits -= 1;
            let Some(charge) = self.ring_charge_of(ticket) else {
                continue;
            };
            charge.relief_visited();
            if charge.relief_bytes() == 0 {
                continue;
            }
            let previous = charge.relief();
            // A partition fenced for delete takes no flush.
            let flush = self
                .get_mut_by_ns(&ticket.namespace)
                .map_or(ReliefFlush::Never, IggyPartition::ask_relief_flush);
            charge.set_relief(match flush {
                ReliefFlush::Asked => {
                    asked += 1;
                    self.wake_namespace(ticket.namespace);
                    Relief::Asked(now)
                }
                ReliefFlush::Pending => match previous {
                    Relief::Waiting | Relief::Skipped(_) => Relief::Asked(now),
                    Relief::Asked(_) | Relief::Excluded => previous,
                },
                ReliefFlush::Blocked => {
                    charge.queue_relief();
                    match previous {
                        Relief::Waiting | Relief::Asked(_) => Relief::Skipped(now),
                        Relief::Skipped(_) | Relief::Excluded => previous,
                    }
                }
                ReliefFlush::Never => Relief::Excluded,
            });
        }
        asked
    }

    /// Step 3 of [`Self::shed_repair_rings`] for held commands. Returns how
    /// many it released and how many checkpoints it asked, so that more can
    /// go.
    fn release_commands(&self, budget: &crate::RepairRingBudget) -> (usize, usize) {
        let mut released = 0;
        let mut checkpoints = 0;
        let mut visits = budget.commands_grown_len().min(SHED_VISITS_PER_TICK_MAX);
        while visits > 0
            && let Some(excess) = budget.costly_excess()
            && let Some(ticket) = budget.pop_commands_grown()
        {
            visits -= 1;
            let (Some(partition), Some(charge)) =
                (self.ring_owner(ticket), self.ring_charge_of(ticket))
            else {
                continue;
            };
            charge.commands_visited();
            let (count, wants_checkpoint) = partition.release_commands_for(excess);
            if count > 0 {
                released += count;
                self.wake_namespace(ticket.namespace);
            }
            if charge.command_bytes() > 0 {
                charge.queue_commands();
            }
            if wants_checkpoint
                && !partition.in_checkpoint_queue()
                && !partition.checkpoint_in_progress()
                && !partition.queued_checkpoint_in_flight()
            {
                partition.join_checkpoint_queue();
                self.enqueue_checkpoint(ticket.namespace, crate::CheckpointTrigger::Demand);
                checkpoints += 1;
            }
        }
        (released, checkpoints)
    }

    /// The charge that `ticket` names, while its partition holds it.
    fn ring_charge_of(&self, ticket: RingTicket) -> Option<Rc<RingCharge>> {
        self.ring_owner(ticket)?.log.journal().inner.ring_charge()
    }

    /// The partition whose ring `ticket` charges, fenced for delete or not:
    /// its ring holds memory until the partition leaves. `None` once the
    /// partition left the shard or took a new charge. Does not wake it: the
    /// shed asks for the owner of every ticket on every tick.
    fn ring_owner(&self, ticket: RingTicket) -> Option<&IggyPartition<B, SB>> {
        let local_idx = self.local_idx(&ticket.namespace)?;
        self.slots()
            .get(*local_idx)?
            .as_ref()
            .map(Slot::partition)
            .filter(|partition| partition.log.journal().inner.charged_by(ticket))
    }

    /// [`Self::ring_owner`] for a shed that takes memory from the ring, which
    /// changes the gauge share of the partition.
    fn shed_ring_owner(&self, ticket: RingTicket) -> Option<&IggyPartition<B, SB>> {
        let owner = self.ring_owner(ticket)?;
        self.wake_namespace(ticket.namespace);
        Some(owner)
    }

    fn report_ring_floor(&self, budget: &crate::RepairRingBudget) {
        let Some(over) = budget.floor_crossed() else {
            return;
        };
        let usage = budget.usage();
        if over {
            warn!(
                shard = self.shard_id.id(),
                floor_bytes = usage.floor_bytes,
                budget_bytes = usage.budget_bytes,
                "the newest entry of every repair ring takes more than the repair ring budget of the shard: \
                 the rings hold that entry only, and a lagging replica catches up by state transfer. \
                 Raise partition.journal_memory_node_bytes_max"
            );
        } else {
            info!(
                shard = self.shard_id.id(),
                floor_bytes = usage.floor_bytes,
                budget_bytes = usage.budget_bytes,
                "the newest entries of the repair rings fit the repair ring budget of the shard again"
            );
        }
    }

    /// This node's key-encryption keys, for wrapping new stream keys.
    #[must_use]
    pub fn key_context(&self) -> Arc<ServerKeyContext> {
        Arc::clone(&self.config.key_context)
    }

    /// The encryptor of one stream incarnation, `None` only for a plaintext
    /// stream.
    ///
    /// # Errors
    ///
    /// See [`StreamKeyring::resolve`]: the stream is unavailable on this node.
    pub fn resolve_stream_encryptor(
        &self,
        identity: StreamKeyIdentity,
        encryption: &StreamEncryption,
    ) -> Result<Option<Arc<EncryptorKind>>, IggyError> {
        self.keyring.borrow_mut().resolve(identity, encryption)
    }

    /// Whether this shard holds the key of any stream incarnation.
    #[must_use]
    pub fn has_stream_keys(&self) -> bool {
        !self.keyring.borrow().is_empty()
    }

    /// Drop the encryptors of the stream incarnations `is_live` rejects.
    pub fn retain_stream_keys(&self, is_live: impl FnMut(StreamKeyIdentity) -> bool) {
        self.keyring.borrow_mut().retain(is_live);
    }

    pub fn set_persistence_notifier(&self, notifier: crate::PersistenceNotifier) {
        for namespace in self.namespaces() {
            if let Some(partition) = self.get_by_ns(namespace) {
                partition.set_persistence_notifier(Rc::clone(&notifier));
            }
        }
        *self.persistence_notifier.borrow_mut() = Some(notifier);
    }

    pub fn set_io_notifier(&self, notifier: crate::PartitionIoNotifier, bytes_max: usize) {
        for partition in self.partitions() {
            partition.set_io_notifier(Rc::clone(&notifier), bytes_max);
        }
        *self.io_notifier.borrow_mut() = Some((notifier, bytes_max));
    }

    /// Give every partition here, and every one inserted later, the store
    /// its remote segments are read from.
    ///
    /// A partition that is here already takes it the next time the pump
    /// reaches for it through [`Self::get_mut_with_remote`]: the pump can
    /// hold a partition across a suspension, so no other task writes to one.
    pub fn set_remote_segments(&self, remote: Rc<crate::RemoteSegments>) {
        *self.remote_segments.borrow_mut() = Some(remote);
    }

    /// Give the shard the cache that its reads of object stores share.
    pub fn set_read_cache(&self, cache: Rc<crate::ReadCache>) {
        *self.read_cache.borrow_mut() = Some(cache);
    }

    /// The cache that the shard's reads of object stores share, `None` on a
    /// shard that was given none.
    #[must_use]
    pub fn read_cache(&self) -> Option<Rc<crate::ReadCache>> {
        self.read_cache.borrow().clone()
    }

    /// Give the partitions here that follow another cluster's log the store
    /// of their peer, by origin name, in place of the stores they had. A
    /// partition here already takes its peer's store the next time the pump
    /// reaches for it, as [`Self::set_remote_segments`] says.
    pub fn set_peer_remote_segments(&self, peers: Vec<(String, Rc<crate::RemoteSegments>)>) {
        *self.peer_remote_segments.borrow_mut() = peers;
    }

    /// [`Self::get_mut_by_ns`] for work that reads the partition's remote
    /// data: a poll, or the rewrite of its placed batches. A partition that
    /// was here before the shard got its store takes the store now.
    ///
    /// Pump only, as [`Self::get_mut_by_ns`].
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_with_remote(
        &self,
        namespace: &IggyNamespace,
    ) -> Option<&mut IggyPartition<B, SB>> {
        let partition = self.get_mut_by_ns(namespace)?;
        if !partition.has_remote_segments()
            && let Some(remote) = self.remote_segments_for(partition)
        {
            partition.set_remote_segments(remote);
        }
        Some(partition)
    }

    /// The store `partition` reads its remote segments from: its peer's, for
    /// a partition that follows another cluster's log, and the node's own for
    /// any other. A peer's partition never gets the node's own store, whose
    /// root holds other objects under the same prefix.
    fn remote_segments_for(
        &self,
        partition: &IggyPartition<B, SB>,
    ) -> Option<Rc<crate::RemoteSegments>> {
        partition.peer_log().map_or_else(
            || self.remote_segments.borrow().clone(),
            |peer| {
                self.peer_remote_segments
                    .borrow()
                    .iter()
                    .find(|(origin, _)| origin == peer.origin())
                    .map(|(_, remote)| Rc::clone(remote))
            },
        )
    }

    /// The store the partitions here read their remote data from, `None` on
    /// a shard that was given none.
    #[must_use]
    pub fn remote_segments(&self) -> Option<Rc<crate::RemoteSegments>> {
        self.remote_segments.borrow().clone()
    }

    /// Access the physical owner through the tombstone gate for I/O settlement,
    /// shutdown, fault scans, and ticks. Admission must use the live lookup.
    #[allow(clippy::mut_from_ref)]
    pub fn get_io_owner(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition<B, SB>> {
        self.get_mut(self.local_idx(namespace)?)
    }

    /// Capture writer ownership even when a previous teardown already tombstoned it.
    pub fn capture_teardown(&self, namespace: &IggyNamespace) -> Option<crate::PartitionTeardown> {
        let teardown = self.teardown.borrow().get(namespace)?.clone();
        teardown.delete();
        self.wake_namespace(*namespace);
        Some(teardown)
    }

    #[must_use]
    pub fn take_ready_loopbacks(&self) -> BTreeMap<IggyNamespace, crate::PartitionIncarnation> {
        std::mem::take(&mut *self.loopback_ready.partitions.borrow_mut())
    }

    #[must_use]
    pub fn has_ready_loopbacks(&self) -> bool {
        !self.loopback_ready.partitions.borrow().is_empty()
    }

    pub fn register_loopback_waker(&self, waker: &Waker) {
        let mut registered = self.loopback_ready.waker.borrow_mut();
        if registered
            .as_ref()
            .is_none_or(|previous| !previous.will_wake(waker))
        {
            *registered = Some(waker.clone());
        }
    }

    /// Metadata shares the pump wake without entering partition readiness.
    pub fn loopback_wake_notifier(&self) -> consensus::LoopbackNotifier {
        let ready = Rc::clone(&self.loopback_ready);
        consensus::LoopbackNotifier::new(move || ready.wake())
    }

    fn install_loopback_notifier(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        let ready = Rc::clone(&self.loopback_ready);
        let incarnation = partition.incarnation();
        let io = partition.teardown_handle().io;
        partition
            .consensus()
            .set_loopback_notifier(Some(consensus::LoopbackNotifier::new(move || {
                if io.is_retiring() {
                    return;
                }
                ready.partitions.borrow_mut().insert(namespace, incarnation);
                ready.wake();
            })));
    }

    pub const fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn slots(&self) -> &Vec<Option<Slot<B, SB>>> {
        // SAFETY: see the `slots` field doc. No `&mut` to the array is live
        // while a walk or lookup reads it. The returned `&` is sound only
        // while not held across an `.await` on a non-pump task (a sibling
        // reconcile could insert or remove); single-threadedness alone is not
        // enough.
        unsafe { &*self.slots.get() }
    }

    /// Every partition on this shard, in slot order, tombstoned ones too.
    /// Does not wake them.
    fn partitions(&self) -> impl Iterator<Item = &IggyPartition<B, SB>> {
        self.slots().iter().flatten().map(Slot::partition)
    }

    /// Every partition on this shard with its namespace, in slot order,
    /// tombstoned ones too. Does not wake them.
    pub(crate) fn stored(&self) -> impl Iterator<Item = (&IggyNamespace, &IggyPartition<B, SB>)> {
        self.slots()
            .iter()
            .flatten()
            .map(|slot| (&slot.namespace, slot.partition()))
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Live partitions, not slots.
    pub fn len(&self) -> usize {
        self.namespace_to_local.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.namespace_to_local.borrow().is_empty()
    }

    /// Get partition by local index. Wakes it: the caller can change it
    /// through its cells.
    pub fn get(&self, local_idx: LocalIdx) -> Option<&IggyPartition<B, SB>> {
        let partition = self
            .slots()
            .get(*local_idx)?
            .as_ref()
            .map(Slot::partition)?;
        self.wake(local_idx);
        Some(partition)
    }

    /// Get mutable partition by local index. Wakes it.
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self, local_idx: LocalIdx) -> Option<&mut IggyPartition<B, SB>> {
        let slot = self.slots().get(*local_idx)?.as_ref()?;
        self.wake(local_idx);
        // SAFETY: the slot array is only read, so a walk over it stays valid.
        // The `&mut` covers this one partition, through its own `UnsafeCell`,
        // and is sound on the pump task only (the sole mutator).
        // Single-threadedness alone is not enough.
        Some(unsafe { &mut *slot.partition.get() })
    }

    /// Lookup local index by namespace.
    pub fn local_idx(&self, namespace: &IggyNamespace) -> Option<LocalIdx> {
        self.namespace_to_local.borrow().get(namespace).copied()
    }

    /// Insert a new partition and return its slot: the last vacant slot
    /// freed, otherwise a new one at the end. A namespace that is already
    /// here leaves first, through [`Self::remove`].
    ///
    /// Insertion is the moment a build becomes the addressable incarnation,
    /// so this is also where its offset counter is published into the shared
    /// `PartitionStats` ([`IggyPartition::publish_current_offset`]). No
    /// earlier point is safe: a build that is never adopted must leave the
    /// live incarnation's counters alone.
    /// Configure persistence before insertion; teardown retains the mounted writer.
    ///
    /// # Safety discipline (compiler cannot enforce)
    ///
    /// Must only be called from the shard's pump task (i.e. inside
    /// `IggyShard::apply_reconcile_ops`), and not during a walk over
    /// [`Self::namespaces`]. A growth of the slot array moves only the `Box`
    /// pointers, with a raw copy and no retag, so a live `&mut IggyPartition`
    /// from [`Self::get_mut_by_ns`] survives it. New external call sites MUST
    /// route through `ReconcileOp::InsertOwned` instead.
    #[doc(hidden)]
    pub fn insert(
        &self,
        namespace: IggyNamespace,
        mut partition: IggyPartition<B, SB>,
    ) -> LocalIdx {
        if self.contains(&namespace) {
            drop(self.remove(&namespace));
        }
        if let Some(notifier) = self.persistence_notifier.borrow().as_ref() {
            partition.set_persistence_notifier(Rc::clone(notifier));
        }
        if let Some((notifier, bytes_max)) = self.io_notifier.borrow().as_ref() {
            partition.set_io_notifier(Rc::clone(notifier), *bytes_max);
        }
        partition
            .consensus()
            .install_tick_clock(self.tick_clock.clone());
        partition.reset_checkpoint_stamps();
        if let Some(budget) = &self.repair_memory {
            partition.proof_walk_memory = Some(budget.reservation());
        }
        if let Some(remote) = self.remote_segments_for(&partition) {
            partition.set_remote_segments(remote);
        }
        if let Some(budget) = &self.repair_rings {
            partition
                .log
                .journal()
                .inner
                .attach_ring_budget(Rc::clone(budget), namespace);
        }
        self.install_loopback_notifier(namespace, &partition);
        partition.publish_current_offset();
        partition.set_consumer_group_offsets_dirty(Rc::clone(&self.consumer_group_offsets_dirty));
        partition.set_fence_published_flag(Rc::clone(&self.fence_published));
        self.teardown
            .borrow_mut()
            .insert(namespace, partition.teardown_handle());
        let slot = Some(Slot {
            namespace,
            partition: Box::new(UnsafeCell::new(partition)),
            sent_report: Cell::default(),
            gauge_share: Cell::default(),
            reload_stamp: Cell::default(),
        });
        // SAFETY: see the `slots` field doc: pump-only, outside any walk, so
        // no other borrow of the array is live. Caller responsibility.
        let slots = unsafe { &mut *self.slots.get() };
        let local_idx = if let Some(local_idx) = self.free_slots.borrow_mut().pop() {
            slots[*local_idx] = slot;
            self.wake(local_idx);
            local_idx
        } else {
            slots.push(slot);
            self.wake_at.borrow_mut().push(Cell::new(WAKE_NOW));
            LocalIdx::new(slots.len() - 1)
        };
        self.namespace_to_local
            .borrow_mut()
            .insert(namespace, local_idx);
        local_idx
    }

    /// Takes the namespaces whose consumer group offsets need a sweep. A
    /// partition records itself when it is inserted, installs a group owner,
    /// creates or deletes a group-keyed offset, and at the first operation
    /// that sees a new view.
    #[must_use]
    pub fn take_consumer_group_offsets_dirty(&self) -> BTreeSet<IggyNamespace> {
        self.consumer_group_offsets_dirty.take()
    }

    pub fn mark_consumer_group_offsets_dirty(&self, namespace: IggyNamespace) {
        self.consumer_group_offsets_dirty
            .borrow_mut()
            .insert(namespace);
    }

    /// Whether a partition here published a fence since the last call.
    pub fn take_fence_published(&self) -> bool {
        self.fence_published.replace(false)
    }

    /// Check if a namespace exists.
    pub fn contains(&self, namespace: &IggyNamespace) -> bool {
        self.namespace_to_local.borrow().contains_key(namespace)
    }

    /// Get partition by namespace directly.
    ///
    /// Returns `None` for tombstoned namespaces so callers outside
    /// [`Plane`] (view-change handlers, loopback drain, `tick_partitions`)
    /// can't drive journal writes against a partition the reconciler has
    /// already fenced for delete.
    ///
    /// A pump-task caller MAY hold the returned reference across an `.await`:
    /// the slab mutators (`apply_reconcile_ops`) run on the same task and so
    /// cannot remove the partition while the pump is suspended.
    /// `tick_partitions` is such a caller (it holds the partition across the
    /// VSR action dispatch await) and still relies on the tombstone gate above.
    /// A non-pump caller must instead use [`Self::with_partition`], which scopes
    /// the borrow to a synchronous closure, and must never hold the reference
    /// across an `.await` (a sibling task's reconcile could remove the
    /// partition mid-await).
    pub fn get_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition<B, SB>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        self.get(self.local_idx(namespace)?)
    }

    /// Run `f` against the partition for `namespace`, returning its result.
    ///
    /// Sanctioned non-pump read path: unlike [`Self::get_by_ns`], the borrow
    /// cannot escape `f`, so it cannot be held across an `.await` while the
    /// pump task removes the partition. Returns `None` for a missing or
    /// tombstoned namespace.
    pub fn with_partition<R>(
        &self,
        namespace: &IggyNamespace,
        f: impl FnOnce(&IggyPartition<B, SB>) -> R,
    ) -> Option<R> {
        let partition = self.get_by_ns(namespace)?;
        #[cfg(debug_assertions)]
        let _guard = BorrowGuard::new(&self.borrow_active);
        Some(f(partition))
    }

    /// TEST / SIMULATOR ONLY, DELIBERATELY UNSOUND. Acquire a
    /// [`Self::with_partition`]-style borrow and hold it across `suspend.await`
    /// -- the exact borrow-across-`.await` anti-pattern PR #3557 closed and
    /// which `with_partition`'s `FnOnce` bound structurally forbids.
    ///
    /// Exists only so the simulator's deterministic dispatch shell can prove
    /// its detector for that async-concurrency class: parked here (borrow
    /// live), a concurrent [`Self::remove`] on a sibling task trips the debug
    /// borrow tripwire, exactly as a real reconcile dropping the partition
    /// under a stale read would. The `simulator` feature / `cfg(test)` gate
    /// excludes it from every production build.
    #[cfg(any(test, feature = "simulator"))]
    pub async fn hold_borrow_across_await(&self, suspend: impl std::future::Future<Output = ()>) {
        // The guard IS the borrow the tripwire counts; holding it across the
        // await models a partition reference outliving a suspension point.
        #[cfg(debug_assertions)]
        let _guard = BorrowGuard::new(&self.borrow_active);
        suspend.await;
    }

    /// Get mutable partition by namespace directly. Tombstone-gated like
    /// [`Self::get_by_ns`].
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition<B, SB>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        self.get_mut(self.local_idx(namespace)?)
    }

    /// Remove a partition by namespace. Its slot turns vacant for a later
    /// [`Self::insert`], and every other partition keeps its slot.
    ///
    /// # Safety discipline (compiler cannot enforce)
    ///
    /// Must only be called from the shard's pump task, and not during a walk
    /// over [`Self::namespaces`]. Removing a partition while any reference to
    /// it is live is UB, for example a `&mut` from [`Self::get_mut_by_ns`]
    /// that a sibling task holds across an `.await`. Other partitions do not
    /// move. New external call sites MUST route through
    /// `ReconcileOp::ConfirmRemove` instead.
    ///
    /// # Panics
    ///
    /// Panics if the namespace maps to a missing or vacant slot, an invariant
    /// violation. Going on would put that slot on the free list twice and hand
    /// it to two partitions.
    ///
    /// The debug tripwire tracks the `&` borrows handed out by
    /// [`Self::with_partition`]; the `&mut` path above is uncounted (it is
    /// pump-only, so it cannot alias this same-task mutation).
    #[doc(hidden)]
    pub fn remove(&self, namespace: &IggyNamespace) -> Option<IggyPartition<B, SB>> {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            self.borrow_active.get(),
            0,
            "IggyPartitions::remove while a with_partition borrow is live"
        );
        let local_idx = self.namespace_to_local.borrow_mut().remove(namespace)?;
        // SAFETY: see the `slots` field doc: pump-only, outside any walk, so
        // no other borrow of the array is live. Caller responsibility.
        let slots = unsafe { &mut *self.slots.get() };
        let slot = slots
            .get_mut(*local_idx)
            .and_then(Option::take)
            .expect("IggyPartitions invariant: a namespace maps to no live slot");
        self.wake_at.borrow()[*local_idx].set(WAKE_NEVER);
        self.gauge_sums.set(
            self.gauge_sums
                .get()
                .replaced(slot.gauge_share.get(), GaugeShare::default()),
        );
        self.free_slots.borrow_mut().push(local_idx);
        let partition = UnsafeCell::into_inner(*slot.partition);
        self.teardown.borrow_mut().remove(namespace);
        partition.invalidate_workerless_offset_files();
        partition.consensus().set_loopback_notifier(None);
        self.loopback_ready
            .partitions
            .borrow_mut()
            .remove(namespace);
        if let Some(remote) = self.remote_segments.borrow().as_deref() {
            remote.forget_partition(*namespace);
        }
        Some(partition)
    }

    /// Remove multiple partitions at once.
    ///
    /// Same pump-only safety discipline as [`Self::remove`].
    #[doc(hidden)]
    pub fn remove_many(&self, namespaces: &[IggyNamespace]) -> Vec<IggyPartition<B, SB>> {
        namespaces.iter().filter_map(|ns| self.remove(ns)).collect()
    }

    /// Iterate over all namespaces owned by this shard, in slot order. That
    /// order follows the inserts and removes, so it is deterministic, but a
    /// reused slot puts a namespace out of numeric order.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.slots().iter().flatten().map(|slot| &slot.namespace)
    }

    /// The partition of `namespace`, tombstoned or not, with what its shard
    /// last reported of it. Wakes it.
    pub fn sent_report(
        &self,
        namespace: &IggyNamespace,
    ) -> Option<(&IggyPartition<B, SB>, &Cell<SentReport>)> {
        let local_idx = self.local_idx(namespace)?;
        let slot = self.slots().get(*local_idx)?.as_ref()?;
        self.wake(local_idx);
        Some((slot.partition(), &slot.sent_report))
    }

    /// The commit fault of the first fenced partition in slot order,
    /// tombstoned ones too. Wakes none: it reads the fault and hands out no
    /// partition.
    pub fn first_commit_fault(&self) -> Option<crate::types::FatalCommit> {
        self.partitions()
            .find_map(|partition| partition.fatal().cloned())
    }

    /// Makes the next shard tick visit the partition in `local_idx`.
    fn wake(&self, local_idx: LocalIdx) {
        self.wake_at.borrow()[*local_idx].set(WAKE_NOW);
    }

    fn wake_namespace(&self, namespace: IggyNamespace) {
        if let Some(local_idx) = self.local_idx(&namespace) {
            self.wake(local_idx);
        }
    }

    /// Appends the namespaces that the shard tick at `now` visits, in slot
    /// order, tombstoned ones too: every slot that is awake or whose wake
    /// tick came.
    pub fn due_namespaces(&self, now: u64, due: &mut Vec<IggyNamespace>) {
        let wake_at = self.wake_at.borrow();
        let slots = self.slots();
        for (index, wake_at) in wake_at.iter().enumerate() {
            if wake_at.get() <= now
                && let Some(slot) = &slots[index]
            {
                due.push(slot.namespace);
            }
        }
    }

    /// The last step of a shard tick for a partition that the tick visited.
    /// `quiet_until` is the tick before which no tick has work for it, when
    /// it has such a tick: the partition then sleeps until that tick or
    /// until an accessor hands it out. Any other partition stays awake.
    ///
    /// The gauge share is read here, in the step that decides the sleep, so
    /// a sleeping partition always holds its current share.
    pub fn settle(&self, namespace: &IggyNamespace, quiet_until: Option<u64>) {
        let Some(local_idx) = self.local_idx(namespace) else {
            return;
        };
        let Some(slot) = self.slots().get(*local_idx).and_then(Option::as_ref) else {
            return;
        };
        let share = slot.partition().gauge_share();
        let old = slot.gauge_share.replace(share);
        self.gauge_sums
            .set(self.gauge_sums.get().replaced(old, share));
        self.track_reload_stamp(slot);
        self.wake_at.borrow()[*local_idx].set(quiet_until.unwrap_or(WAKE_NOW));
    }

    /// Slots so far, vacant ones too.
    pub fn slot_count(&self) -> usize {
        self.slots().len()
    }

    /// The partition in slot `index`, when it sleeps past `now`. Does NOT
    /// wake it: this is the read of the check that a sleeping partition has
    /// no work.
    pub fn sleeper(&self, index: usize, now: u64) -> Option<Sleeper<'_, B, SB>> {
        let wake_at = self.wake_at.borrow().get(index)?.get();
        let slot = self.slots().get(index)?.as_ref()?;
        (wake_at > now).then(|| Sleeper {
            namespace: slot.namespace,
            partition: slot.partition(),
            sent_report: slot.sent_report.get(),
            wake_at,
            gauge_share_current: slot.gauge_share.get() == slot.partition().gauge_share(),
        })
    }

    /// The partition of `namespace` for a read that changes nothing, such as
    /// the oracle of a simulation. Unlike [`Self::get_by_ns`] it does NOT
    /// wake the partition, so an observer cannot hide a missed wake-up.
    #[cfg(any(test, feature = "simulator"))]
    pub fn peek_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition<B, SB>> {
        self.find(*namespace)
    }

    /// A live partition, without a wake. Only for a read, or for a change
    /// that wakes the partition itself when it gives the partition work.
    fn find(&self, namespace: IggyNamespace) -> Option<&IggyPartition<B, SB>> {
        if self.is_tombstoned(&namespace) {
            return None;
        }
        self.slots()
            .get(*self.local_idx(&namespace)?)?
            .as_ref()
            .map(Slot::partition)
    }

    /// Every partition here, tombstoned ones too, for a read that changes
    /// nothing. Like [`Self::peek_by_ns`] it does NOT wake them.
    #[cfg(any(test, feature = "simulator"))]
    pub fn peek_all(&self) -> impl Iterator<Item = &IggyPartition<B, SB>> {
        self.partitions()
    }

    /// Makes the next shard tick visit every partition, as a tick did before
    /// partitions slept. A run that calls it before each tick must behave as
    /// a run that never does.
    #[cfg(any(test, feature = "simulator"))]
    pub fn wake_all(&self) {
        for (wake_at, slot) in self.wake_at.borrow().iter().zip(self.slots()) {
            if slot.is_some() {
                wake_at.set(WAKE_NOW);
            }
        }
    }

    /// The gauge levels of every partition here, sleeping ones too.
    pub const fn gauge_sums(&self) -> GaugeShare {
        self.gauge_sums.get()
    }

    pub fn is_tombstoned(&self, namespace: &IggyNamespace) -> bool {
        self.tombstoned.borrow().contains_key(namespace)
    }

    /// Snapshot every tombstoned namespace, including ones never
    /// materialised: a boot-time damage verdict fences a namespace before
    /// any partition exists, so it appears in no other view of this map.
    pub fn tombstoned_namespaces(&self) -> Vec<IggyNamespace> {
        self.tombstoned.borrow().keys().copied().collect()
    }

    /// Mark a namespace as tombstoned. Callable from any task on the
    /// shard's runtime (reconciler sets the fence synchronously before
    /// awaiting disk delete).
    pub fn tombstone(&self, namespace: IggyNamespace) {
        if let Some(teardown) = self.teardown.borrow().get(&namespace) {
            teardown.retire();
        }
        self.loopback_ready
            .partitions
            .borrow_mut()
            .remove(&namespace);
        self.tombstoned
            .borrow_mut()
            .entry(namespace)
            .or_insert(None);
        if let Some(remote) = self.remote_segments.borrow().as_deref() {
            remote.forget_partition(namespace);
        }
        self.wake_namespace(namespace);
    }

    /// A permanent recovery failure, unlike a temporary teardown tombstone.
    /// Retirement must persist this incarnation's fence before reporting success.
    pub fn fence(&self, namespace: IggyNamespace, created_revision: u64) {
        self.tombstoned
            .borrow_mut()
            .insert(namespace, Some(created_revision));
        self.wake_namespace(namespace);
    }

    #[must_use]
    pub fn failed_revision(&self, namespace: &IggyNamespace) -> Option<u64> {
        self.tombstoned.borrow().get(namespace).copied().flatten()
    }

    /// Clear a namespace tombstone. Pump-side hook called from
    /// `ReconcileOp::ConfirmRemove` after the partition is dropped.
    pub fn untombstone(&self, namespace: &IggyNamespace) {
        self.tombstoned.borrow_mut().remove(namespace);
        self.wake_namespace(*namespace);
    }

    /// Snapshot read resources under a synchronous borrow on the owning pump.
    /// Returns `None` for a missing or tombstoned namespace. Execution carries
    /// no partition borrow; its result returns to [`Self::complete_poll`].
    pub fn build_poll_snapshot(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
        args: &PollingArgs,
    ) -> Option<PollPlan> {
        self.build_poll_snapshot_within(namespace, consumer, args, usize::MAX)
    }

    /// [`Self::build_poll_snapshot`] for a poll whose reply holds at most
    /// `byte_budget` bytes of messages, so the snapshot stops at the segment
    /// that covers them instead of at the partition's tail.
    pub fn build_poll_snapshot_within(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
        args: &PollingArgs,
        byte_budget: usize,
    ) -> Option<PollPlan> {
        // `build_poll_plan` touches the partition's sealed-read-handle LRU, so it
        // needs `&mut`. Sound on the pump: it is fully synchronous (no `.await`
        // inside), so no sibling task can remove the partition under it.
        // Read the knob first: the `&mut` borrow below covers `self.config` too.
        let validate_checksum = self.config.validate_checksum;
        let partition = self.get_mut_with_remote(namespace)?;
        Some(partition.build_poll_plan_within(consumer, args, validate_checksum, byte_budget))
    }

    /// Validate and accept a poll synchronously on the owning pump.
    /// Attempt the reply synchronously, then immediately await any returned
    /// continuation through [`Self::replicate_poll_completion`] on the same pump.
    /// Do not suspend between acceptance and that call or discard the continuation.
    /// Success does not acknowledge a durable offset commit.
    ///
    /// Acceptance is independent of reply delivery. A caller that stopped
    /// waiting does not cancel completion: an accepted read can still advance
    /// progress and admit an automatic commit even if its reply cannot be sent.
    /// A history rejection accepts no progress from this read, but cannot
    /// replace a response the transport already sent.
    ///
    /// # Errors
    /// Rejects stale history, unavailable partition or admission state, invalid
    /// consumer identifiers, and exhausted consumer, queue, or journal capacity.
    /// A read of the served history that the object store failed for good is
    /// rejected with that failure. Rejection does not accept progress from
    /// this read.
    pub fn complete_poll(
        &self,
        namespace: &IggyNamespace,
        result: PollReadResult,
    ) -> Result<PollCompletion, IggyError> {
        let partition = self
            .get_mut_by_ns(namespace)
            .ok_or(IggyError::TransientNotAccepted)?;
        partition.complete_poll(result)
    }

    /// Drive an accepted poll's replication on the owning pump after its reply.
    /// Use the continuation returned by [`Self::complete_poll`] for this namespace.
    /// Begin this call immediately after the synchronous reply attempt, with no
    /// intervening suspension. Dropping the assigned prepare would leave an
    /// unjournaled operation in the pipeline and prevent later commits.
    /// This call may suspend with a partition borrow, so it must remain on the
    /// owning pump.
    ///
    /// # Panics
    /// Panics if the namespace is missing or tombstoned. Its availability cannot
    /// change between acceptance and this call in the same synchronous owner turn.
    pub async fn replicate_poll_completion(
        &self,
        namespace: &IggyNamespace,
        replication: PollReplication,
    ) {
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("IggyPartitions invariant: accepted poll namespace missing or tombstoned");
        partition.replicate_poll_completion(replication).await;
    }

    /// Read a consumer's stored offset + the partition commit offset. Fully
    /// synchronous (atomics + lock-free maps), so it runs under a single
    /// [`Self::with_partition`] borrow. `None` for a missing/tombstoned
    /// namespace.
    pub fn consumer_offset_read(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
    ) -> Option<(Option<u64>, u64)> {
        self.with_partition(namespace, |partition| {
            (
                partition.get_consumer_offset(consumer),
                partition.offsets().commit_offset,
            )
        })
    }

    /// [`Self::consumer_offset_read`] for an external group, which is never polled.
    pub fn external_group_offset_read(
        &self,
        namespace: &IggyNamespace,
        group_id: u32,
    ) -> Option<(Option<u64>, u64)> {
        self.with_partition(namespace, |partition| {
            (
                partition.geo_external_group_offset(group_id),
                partition.offsets().commit_offset,
            )
        })
    }

    /// [`Self::external_group_offset_read`] for a group that this cluster
    /// does not have, by its name: the position that another datacenter
    /// published for its external offsets, if any.
    pub fn external_position_read(
        &self,
        namespace: &IggyNamespace,
        name: String,
    ) -> Option<(Option<u64>, u64)> {
        let holder = PositionHolder::ExternalGroup(name);
        self.with_partition(namespace, |partition| {
            (
                partition.geo_position_offset(&holder),
                partition.offsets().commit_offset,
            )
        })
    }

    /// Cooperative-rebalance: a group's `(last_polled, committed)` offsets on the
    /// partition for `namespace`. Synchronous (lock-free maps), under a single
    /// [`Self::with_partition`] borrow. `None` for a missing/tombstoned namespace.
    pub fn group_offset_state(
        &self,
        namespace: &IggyNamespace,
        group_id: u64,
    ) -> Option<(Option<u64>, Option<u64>)> {
        self.with_partition(namespace, |partition| {
            partition.group_offset_state(group_id)
        })
    }

    /// Resolve a `DeleteSegments` count to the `end_offset` of the `count`-th
    /// oldest sealed segment on the partition for `namespace`. The outer `None`
    /// is a missing/tombstoned namespace; the inner `None` is a partition with
    /// no deletable sealed segment.
    pub fn nth_oldest_sealed_end_offset(
        &self,
        namespace: &IggyNamespace,
        count: u32,
    ) -> Option<Option<u64>> {
        self.with_partition(namespace, |partition| {
            partition.nth_oldest_sealed_end_offset(count)
        })
    }

    /// Read `(end_offset, lagging, created_revision)` in one partition access.
    /// The caller compares this local history with committed metadata before
    /// submitting the resolved watermark.
    /// A replica that is not a normal primary that applied the view it
    /// started, or that installs a transfer, may miss whole sealed segments,
    /// so it lags for every answer. That primary's commits above `commit_min`
    /// only seal its active segment and append after it: they leave a sealed
    /// offset settled and make only a no-op lag. A `count` above the sealed
    /// segments resolves to the newest one even though those commits may seal
    /// another, so the delete can remove fewer segments than `count`. Lagging
    /// there would answer `TransientNotAccepted` for as long as disk-bound load
    /// keeps commits waiting. `None` means no local partition.
    pub fn segment_delete_resolution(
        &self,
        namespace: &IggyNamespace,
        count: u32,
    ) -> Option<(Option<u64>, bool, u64)> {
        self.with_partition(namespace, |partition| {
            let consensus = partition.consensus();
            let up_to_offset = partition.nth_oldest_sealed_end_offset(count);
            let lagging = !consensus.applied_view_start()
                || consensus.is_follower()
                || !consensus.is_normal()
                || consensus.is_transferring()
                || (up_to_offset.is_none() && consensus.commit_min() < consensus.commit_max());
            (up_to_offset, lagging, partition.created_revision())
        })
    }
}

impl<B, SB> IggyPartitions<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// [`Plane::on_request`] carrying the in-process reply channel a
    /// `PartitionSubmit` arrived with; `None` keeps the bus-reply path.
    pub async fn on_request_with_reply(
        &self,
        message: <VsrConsensus<B> as Consensus>::Message<RoutedRequestHeader>,
        reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let namespace = IggyNamespace::from_raw(message.header().group);
        // Every exit below that drops the request answers its waiter first: a
        // submit's reply cannot be routed by `header.client` (the VSR id), so
        // an unanswered channel costs the client a full read-timeout for an
        // outcome that was decided here and now.
        let mut reply = reply;
        if self.is_tombstoned(&namespace) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = namespace.inner(),
                "dropping request: namespace tombstoned"
            );
            Self::answer_waiter(
                reply.take(),
                message.header(),
                IggyError::TransientNotAccepted.as_code(),
            );
            return;
        }
        let Some(partition) = self.get_mut_by_ns(&namespace) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                operation = ?message.header().operation,
                "partition not initialized for namespace"
            );
            Self::answer_waiter(
                reply.take(),
                message.header(),
                IggyError::TransientNotAccepted.as_code(),
            );
            return;
        };
        // At-rest encryption happens HERE, once, before the op enters
        // consensus: canonicalize the wire form first so both the legacy and
        // v2 request encodings encrypt identically, then encrypt payload +
        // user headers per message. The ciphertext is what gets journaled,
        // replicated, checksummed, and persisted -- every replica stores
        // identical bytes -- and the poll reply is the single decrypt point.
        // The partition's own encryptor: each stream has its own key, pinned
        // to the partition when it was built.
        let message = if message.header().operation == Operation::SendMessages
            && let Some(encryptor) = partition.encryptor()
        {
            // Compute the batch checksum: this canonical output is validated by
            // `encrypt_batch_request`'s decode before re-encryption, and the
            // re-encrypted batch (checksum kept by `encrypt_batch_request`) then
            // re-enters `convert` as the canonical-vs-legacy discriminator.
            // The header outlives the message consumed by the conversion, so a
            // failure can still be answered with the frame's own identity.
            let header = *message.header();
            let canonical = convert_request_message(namespace, message, ChecksumMode::Compute)
                .and_then(|message| encrypt_batch_request(message, encryptor));
            match canonical {
                Ok(message) => message,
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        namespace_raw = namespace.inner(),
                        %error,
                        "dropping send_messages: failed to encrypt batch at ingestion"
                    );
                    Self::answer_waiter(reply.take(), &header, error.as_code());
                    return;
                }
            }
        } else {
            message
        };
        partition.on_request(message, reply).await;
    }

    /// [`Self::on_request_with_reply`] for the request that commits where a
    /// client's batch sits in the object store, see
    /// [`IggyPartition::on_placed_request`]. Only the shard's own upload path
    /// calls it, once the object is stored.
    pub async fn on_placed_request_with_reply(
        &self,
        message: <VsrConsensus<B> as Consensus>::Message<RoutedRequestHeader>,
        reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let namespace = IggyNamespace::from_raw(message.header().group);
        // The partition takes its store before the batch commits, so the
        // commit can tell the shard's cache where the batch ends.
        let partition = (!self.is_tombstoned(&namespace))
            .then(|| self.get_mut_with_remote(&namespace))
            .flatten();
        let Some(partition) = partition else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                "dropping a placed request: its partition is gone"
            );
            Self::answer_waiter(
                reply,
                message.header(),
                IggyError::TransientNotAccepted.as_code(),
            );
            return;
        };
        partition.on_placed_request(message, reply).await;
    }

    /// Whether the partition of `namespace` is here and places its batches,
    /// so a client's send goes to the object store before it is committed.
    #[must_use]
    pub fn places_batches(&self, namespace: &IggyNamespace) -> bool {
        self.get_by_ns(namespace)
            .is_some_and(IggyPartition::places_batches)
    }

    /// Deny a request that never reached its partition on the submit channel
    /// it arrived with, if any. The bus path has no waiter to answer and keeps
    /// its drop-and-warn behaviour.
    fn answer_waiter(
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
        header: &RoutedRequestHeader,
        status: u32,
    ) {
        if let Some(waiter) = waiter {
            let _ = waiter.send(build_deny_reply_from_request_header(header, status));
        }
    }
}

impl<B, SB> Plane<VsrConsensus<B>> for IggyPartitions<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    async fn on_request(
        &self,
        message: <VsrConsensus<B> as Consensus>::Message<RoutedRequestHeader>,
    ) {
        self.on_request_with_reply(message, None).await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let group = IggyNamespace::from_raw(message.header().group);
        if self.is_tombstoned(&group) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = group.inner(),
                "dropping prepare: namespace tombstoned"
            );
            return;
        }
        let Some(partition) = self.get_mut_by_ns(&group) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = group.inner(),
                op = message.header().op,
                operation = ?message.header().operation,
                "partition not initialized for namespace"
            );
            return;
        };
        partition.on_replicate(message).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareOkHeader>) {
        let group = IggyNamespace::from_raw(message.header().group);
        if self.is_tombstoned(&group) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = group.inner(),
                "dropping prepare-ok: namespace tombstoned"
            );
            return;
        }
        let Some(partition) = self.get_mut_by_ns(&group) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = group.inner(),
                op = message.header().op,
                "partition not initialized for namespace"
            );
            return;
        };
        partition.on_ack(message, &self.config).await;
    }
}

impl<B, SB> PlaneIdentity<VsrConsensus<B>> for IggyPartitions<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    fn is_applicable<H>(&self, message: &<VsrConsensus<B> as Consensus>::Message<H>) -> bool
    where
        H: ConsensusHeader,
    {
        assert!(matches!(
            message.header().command(),
            Command::Request | Command::Prepare | Command::PrepareOk
        ));
        message.header().operation().is_partition()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{BodyPosition, EvictedHeader, MessageLookup};
    use crate::repair_memory::LOW_WATERMARK_DIVISOR;
    use crate::{RepairRingBudget, Segment};
    use bytes::Bytes;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::Operation;
    use iggy_common::{IggyByteSize, PartitionStats, PollingStrategy};
    use journal::Journal as _;
    use message_bus::IggyMessageBus;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, PREPARE_SPLIT_POINT, SendMessagesOwned,
        stamp_prepare_for_persistence,
    };
    use server_common::{
        Message, SegmentStorage,
        iobuf::{Frozen, Owned},
    };
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    const TEST_CLUSTER: u128 = 1;
    const RING_OPS: RangeInclusive<u64> = 1..=4;

    fn remote_segments() -> Rc<crate::RemoteSegments> {
        Rc::new(crate::RemoteSegments::new(
            Rc::new(iggy_object_store::MemoryStore::new()),
            iggy_object_store::KeyLayout::new("iggy").unwrap(),
        ))
    }

    fn partitions() -> IggyPartitions<IggyMessageBus> {
        IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
                validate_checksum: false,
                segment_size: IggyByteSize::from(1024 * 1024),
                preallocate_segments: false,
                key_context: Arc::default(),
                path_layout: crate::PartitionPathLayout::default(),
            },
        )
    }

    #[test]
    fn given_a_remote_store_when_a_partition_is_inserted_should_hand_it_the_store() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = partitions();
        partitions.set_remote_segments(remote_segments());

        partitions.insert(namespace, build_partition());

        assert!(
            partitions
                .get_by_ns(&namespace)
                .unwrap()
                .has_remote_segments()
        );
    }

    #[test]
    fn given_a_partition_inserted_before_the_remote_store_when_it_is_polled_should_take_the_store()
    {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = partitions();
        partitions.insert(namespace, build_partition());
        partitions.set_remote_segments(remote_segments());
        assert!(
            !partitions
                .get_by_ns(&namespace)
                .unwrap()
                .has_remote_segments()
        );

        let plan = partitions.build_poll_snapshot(
            &namespace,
            PollingConsumer::Consumer(7, 0),
            &PollingArgs::new(PollingStrategy::next(), 1, false),
        );

        assert!(plan.is_some());
        assert!(
            partitions
                .get_by_ns(&namespace)
                .unwrap()
                .has_remote_segments()
        );
    }

    /// The origin's partition incarnation that a follower reads.
    const ORIGIN_PREFIX: iggy_object_store::ObjectPrefix = iggy_object_store::ObjectPrefix {
        stream_id: 3,
        topic_id: 1,
        partition_id: 0,
        created_revision: 12,
    };

    fn holds(
        partition: &IggyPartition<IggyMessageBus>,
        remote: &Rc<crate::RemoteSegments>,
    ) -> bool {
        partition
            .remote_segments()
            .is_some_and(|held| Rc::ptr_eq(held, remote))
    }

    #[test]
    fn given_a_partition_that_follows_a_peer_when_inserted_and_polled_should_read_only_its_peers_store()
     {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = partitions();
        let own = remote_segments();
        partitions.set_remote_segments(Rc::clone(&own));
        let mut follower = build_partition();
        follower.follow_peer_log("dc-b", ORIGIN_PREFIX).unwrap();

        partitions.insert(namespace, follower);

        assert!(
            !partitions
                .get_by_ns(&namespace)
                .unwrap()
                .has_remote_segments(),
            "the node's own store holds other objects under the same keys"
        );

        let (dc_b, dc_c) = (remote_segments(), remote_segments());
        partitions.set_peer_remote_segments(vec![
            ("dc-c".to_owned(), Rc::clone(&dc_c)),
            ("dc-b".to_owned(), Rc::clone(&dc_b)),
        ]);
        let plan = partitions.build_poll_snapshot(
            &namespace,
            PollingConsumer::Consumer(7, 0),
            &PollingArgs::new(PollingStrategy::next(), 1, false),
        );

        assert!(plan.is_some());
        let follower = partitions.get_by_ns(&namespace).unwrap();
        assert!(holds(follower, &dc_b));
        assert!(!holds(follower, &dc_c) && !holds(follower, &own));
    }

    #[test]
    fn given_peer_stores_when_a_partition_of_this_cluster_is_inserted_should_hand_it_the_nodes_store()
     {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = partitions();
        let own = remote_segments();
        partitions.set_remote_segments(Rc::clone(&own));
        partitions.set_peer_remote_segments(vec![("dc-b".to_owned(), remote_segments())]);

        partitions.insert(namespace, build_partition());

        assert!(holds(partitions.get_by_ns(&namespace).unwrap(), &own));
    }

    fn build_partition() -> IggyPartition<IggyMessageBus> {
        let namespace = IggyNamespace::new(1, 1, 0);
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            1,
            namespace.inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.init();
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
        )
    }

    #[compio::test]
    async fn loopback_readiness_tracks_mount_tombstone_and_replacement() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let config = PartitionsConfig {
            messages_required_to_save: 100,
            size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024_u64),
            validate_checksum: true,
            segment_size: IggyByteSize::from(1024 * 1024_u64),
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: crate::PartitionPathLayout::default(),
        };
        let partitions = IggyPartitions::new(ShardId::new(0), config);
        let first = build_partition();
        let incarnation = first.incarnation();
        let header = PrepareHeader {
            command: Command::Prepare,
            cluster: TEST_CLUSTER,
            group: namespace.inner(),
            checksum: 42,
            ..Default::default()
        };
        consensus::send_prepare_ok(first.consensus(), &header, true).await;
        partitions.insert(namespace, first);
        assert_eq!(
            partitions.take_ready_loopbacks().get(&namespace),
            Some(&incarnation)
        );
        assert!(!partitions.has_ready_loopbacks());

        let mut messages = Vec::new();
        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        partition.consensus().drain_loopback_into(&mut messages);
        consensus::send_prepare_ok(partition.consensus(), &header, true).await;
        assert!(partitions.has_ready_loopbacks());
        partitions.tombstone(namespace);
        assert!(!partitions.has_ready_loopbacks());
        assert!(partition.teardown_handle().io.is_retiring());
        partition.consensus().drain_loopback_into(&mut messages);
        consensus::send_prepare_ok(partition.consensus(), &header, true).await;
        assert!(
            !partitions.has_ready_loopbacks(),
            "tombstoned owners cannot notify"
        );
        let teardown = partitions.capture_teardown(&namespace).unwrap();
        assert!(partition.teardown_handle().io.is_deleting());
        teardown.drain().await.unwrap();
        let retired = Box::new(partitions.remove(&namespace).unwrap());
        partitions.untombstone(&namespace);
        assert!(!partitions.has_ready_loopbacks());
        retired.consensus().drain_loopback_into(&mut messages);
        consensus::send_prepare_ok(retired.consensus(), &header, true).await;
        assert!(
            !partitions.has_ready_loopbacks(),
            "retired notifier was detached"
        );

        let replacement = build_partition();
        assert_ne!(replacement.incarnation(), incarnation);
        let replacement_incarnation = replacement.incarnation();
        consensus::send_prepare_ok(replacement.consensus(), &header, true).await;
        partitions.insert(namespace, replacement);
        assert_eq!(
            partitions.take_ready_loopbacks().get(&namespace),
            Some(&replacement_incarnation)
        );
        let replacement = partitions.get_mut_by_ns(&namespace).unwrap();
        replacement.consensus().drain_loopback_into(&mut messages);
        consensus::send_prepare_ok(replacement.consensus(), &header, true).await;
        assert!(partitions.has_ready_loopbacks());
        let removed = Box::new(partitions.remove(&namespace).unwrap());
        assert!(!partitions.has_ready_loopbacks());
        removed.consensus().drain_loopback_into(&mut messages);
        consensus::send_prepare_ok(removed.consensus(), &header, true).await;
        assert!(
            !partitions.has_ready_loopbacks(),
            "removed notifier was detached"
        );
    }

    /// `build_partition` for a replicated group. The replica count is what
    /// decides whether the journal retains evicted entries for repair, so a
    /// single-replica partition cannot exercise anything that reads the ring.
    fn build_replicated_partition(namespace: IggyNamespace) -> IggyPartition<IggyMessageBus> {
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            3,
            namespace.inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.init();
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
        )
    }

    /// One-message `SendMessages` journal entry stamped at `op` / `base_offset`.
    /// Reuses the production blob builder + checksum stamping so the entry
    /// decodes through `decode_prepare_slice` and indexes into `offset_to_op`,
    /// the map `candidate_start_op` and the contiguity guard read.
    fn build_send_messages_entry(
        namespace: IggyNamespace,
        op: u64,
        base_offset: u64,
    ) -> Frozen<4096> {
        let mut batch = IggyMessages::with_capacity(1);
        batch.push(IggyMessage {
            header: IggyMessageHeader {
                payload_length: 8,
                ..Default::default()
            },
            payload: Bytes::from_static(b"abcdefgh"),
            user_headers: None,
        });
        let owned =
            SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");

        let total_size = PREPARE_SPLIT_POINT + owned.blob.len();
        let prepare = Message::<PrepareHeader>::new(total_size).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::SendMessages;
                header.op = op;
                header.size = u32::try_from(total_size).expect("size fits u32");
            },
        );

        let mut prepare = prepare;
        {
            let bytes = prepare.as_mut_slice();
            owned
                .header
                .encode_into(&mut bytes[std::mem::size_of::<PrepareHeader>()..PREPARE_SPLIT_POINT]);
            bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + owned.blob.len()]
                .copy_from_slice(&owned.blob);
        }

        let (prepare, _command, _count) = stamp_prepare_for_persistence(prepare, base_offset, 1)
            .expect("stamp prepare for persistence");
        prepare.into_frozen()
    }

    /// Resident-tail straddle continuation over the OWNED snapshot, equivalent
    /// to the removed `journal_get_contiguous`. Reproduces the disk->journal
    /// straddle gap: a commit between the plan snapshot and the splice persists
    /// offsets `D+1..R-1` and evicts them, leaving `oldest_resident > D+1`. The
    /// continuation must be refused there (so the poll returns disk-only and the
    /// next poll re-routes the evicted run to the flushed disk tier) instead of
    /// silently splicing the next resident op `R` over the gap.
    ///
    /// `count: 1` makes `last_matching_offset` the single matched offset, so the
    /// skip is provable without decoding fragment bytes: a lookup that asked for
    /// offset 1 returning offset 3 means 1 and 2 were skipped. This drives the
    /// exact `select_resident` walk + `oldest_resident <= from_offset` gate that
    /// `ResidentTailSnapshot::straddle_continuation` + `PollPlan::execute` apply.
    #[compio::test]
    async fn straddle_continuation_refuses_journal_when_oldest_resident_advanced_past_gap() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partition = build_partition();

        // Journal offsets 0..=4 (op N carries message offset N).
        for offset in 0..=4u64 {
            partition
                .log
                .journal()
                .inner
                .append(build_send_messages_entry(namespace, offset + 1, offset))
                .await
                .expect("append journal entry");
        }

        // Commit-time flush of the prefix: offsets 0,1,2 leave the journal,
        // so the oldest resident offset advances to 3 (the gap edge).
        let prefix = partition.log.journal().inner.committed_prefix(3);
        assert_eq!(prefix.len(), 3, "ops for offsets 0,1,2 are the prefix");
        partition.log.journal().inner.evict_prefix(prefix.len());

        // Snapshot the resident tail the way `build_poll_plan` does.
        let oldest_resident = partition.log.journal().inner.oldest_resident_offset();
        let entries = partition.log.journal().inner.resident_entries();
        assert_eq!(
            oldest_resident,
            Some(3),
            "offsets 0,1,2 evicted; 3 is the oldest resident",
        );

        // D = 0 (last disk match), so a straddle would continue at offset 1.
        // A raw snapshot walk from offset 1 falls through to the next resident
        // op (offset 3): exactly the silent skip the gate prevents.
        let (_, leaky_offset, _) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: 1,
                count: 1,
                ceiling: u64::MAX,
            },
        )
        .expect("raw snapshot walk still returns the post-gap batch");
        assert_eq!(
            leaky_offset,
            Some(3),
            "raw select_resident skips the evicted 1,2 and serves offset 3 (the hazard)",
        );

        // The contiguity gate refuses that continuation: oldest_resident (3) > 1.
        let from_offset = 1u64;
        let contiguous = oldest_resident.is_some_and(|oldest| oldest <= from_offset);
        assert!(
            !contiguous,
            "contiguity gate must not serve offset 1 over the eviction gap",
        );

        // A contiguous continuation (start == oldest_resident) passes the gate
        // and serves the resident tail.
        let from_offset = 3u64;
        let contiguous = oldest_resident.is_some_and(|oldest| oldest <= from_offset);
        assert!(contiguous, "offset 3 is resident; gate must allow it");
        let (_, contiguous_offset, _) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: from_offset,
                count: 1,
                ceiling: u64::MAX,
            },
        )
        .expect("offset 3 is resident, continuation must serve it");
        assert_eq!(
            contiguous_offset,
            Some(3),
            "contiguous continuation returns the resident tail starting at 3",
        );
    }

    /// A flush evicts the committed prefix up to and INCLUDING `commit_max`, so
    /// a caught-up replica keeps no resident header at its own commit point. The
    /// `DoViewChange` suffix is floored there and cannot nack it, so reading the
    /// resident headers alone sends the commit point out blank, which a quorum of
    /// senders turns into a view change that never starts.
    ///
    /// The entry is still servable (`repair_entry` answers from the evicted
    /// ring), so the suffix reads through `repair_header`, over the same range.
    #[compio::test]
    async fn evicted_commit_point_still_answers_for_the_view_change_suffix() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partition = build_replicated_partition(namespace);

        for offset in 0..=2u64 {
            partition
                .log
                .journal()
                .inner
                .append(build_send_messages_entry(namespace, offset + 1, offset))
                .await
                .expect("append journal entry");
        }

        let commit_max = 3;
        let prefix = partition.log.journal().inner.committed_prefix(commit_max);
        assert_eq!(prefix.len(), 3, "the whole log is committed and flushable");
        partition.log.journal().inner.evict_prefix(prefix.len());

        assert!(
            partition
                .log
                .journal()
                .inner
                .header_by_op(commit_max)
                .is_none(),
            "the flush evicted the commit point from the resident headers",
        );
        assert!(
            partition
                .log
                .journal()
                .inner
                .repair_entry(commit_max)
                .is_some(),
            "yet the entry is still servable from the evicted ring",
        );

        let header = partition
            .log
            .journal()
            .inner
            .repair_header(commit_max)
            .expect("the commit point must stay describable for the DVC suffix");
        assert_eq!(header.op, commit_max);
    }

    /// The resident journal holds replicated-but-uncommitted prepares ahead of
    /// the commit frontier. A poll must clamp at `ceiling` (the commit offset)
    /// so it never returns a dirty read of view-change-rollbackable data, even
    /// when the requested count would otherwise reach into the uncommitted tail.
    #[compio::test]
    async fn poll_clamps_at_commit_ceiling() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partition = build_partition();

        // Offsets 0..=5 all resident; only 0..=2 are committed.
        for offset in 0..=5u64 {
            partition
                .log
                .journal()
                .inner
                .append(build_send_messages_entry(namespace, offset + 1, offset))
                .await
                .expect("append journal entry");
        }

        let entries = partition.log.journal().inner.resident_entries();
        let (_, last, _) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: 2,
            },
        )
        .expect("offsets 0..=2 are within the ceiling and must be served");
        assert_eq!(
            last,
            Some(2),
            "poll must stop at the commit ceiling, never serving 3..=5",
        );

        // A poll starting past the ceiling returns nothing.
        assert!(
            crate::journal::select_resident(
                &entries,
                MessageLookup::Offset {
                    offset: 3,
                    count: 10,
                    ceiling: 2,
                },
            )
            .is_none(),
            "no committed message at or after offset 3, so the poll is empty",
        );
    }

    #[compio::test]
    #[should_panic(expected = "accepted poll namespace missing or tombstoned")]
    async fn given_accepted_poll_when_namespace_is_tombstoned_should_trip_invariant() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let config = PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
            validate_checksum: false,
            segment_size: IggyByteSize::from(1024 * 1024),
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: crate::PartitionPathLayout::default(),
        };
        let partitions = IggyPartitions::new(ShardId::new(0), config);
        partitions.insert(namespace, build_partition());
        let consumer = PollingConsumer::Consumer(7, 0);
        let auto_commit = true;
        let mut read_result = partitions
            .build_poll_snapshot(
                &namespace,
                consumer,
                &PollingArgs::new(PollingStrategy::next(), 1, auto_commit),
            )
            .expect("snapshot the current history")
            .execute_resident();

        // Supply a nonempty result with the captured history so acceptance
        // assigns an automatic commit, without needing message I/O for this test.
        read_result
            .fragments
            .push(crate::Fragment::whole(Owned::<4096>::zeroed(8).into()));
        read_result.last_matching_offset = Some(0);
        let replication = partitions
            .complete_poll(&namespace, read_result)
            .expect("accept automatic commit")
            .replication
            .expect("assign a prepare");
        let partition = partitions.get_by_ns(&namespace).expect("partition exists");
        let prepare_header = partition
            .consensus()
            .pipeline_head_header()
            .expect("acceptance queued the prepare");
        assert_eq!(prepare_header.operation, Operation::StoreConsumerOffset);
        assert!(!partition.log.journal().inner.holds_op(prepare_header.op));

        // A future caller that yields after acceptance could permit this
        // tombstone. The partition still exists and must not lose its prepare.
        partitions.tombstone(namespace);
        assert!(partitions.local_idx(&namespace).is_some());
        partitions
            .replicate_poll_completion(&namespace, replication)
            .await;
    }

    /// Under disk-bound load a primary's quorum commits wait for the writer,
    /// so `commit_min` stays below `commit_max`. Those commits can seal the
    /// active segment but never change a sealed one.
    #[test]
    fn given_commits_waiting_for_the_writer_when_resolving_segment_delete_should_settle_sealed_offsets()
     {
        const SEALED_END: u64 = 9;
        let namespace = IggyNamespace::new(1, 1, 0);
        let segment_size = IggyByteSize::from(1024 * 1024);
        let config = PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: segment_size,
            validate_checksum: false,
            segment_size,
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: crate::PartitionPathLayout::default(),
        };
        let partitions = IggyPartitions::new(ShardId::new(0), config);
        partitions.insert(namespace, build_replicated_partition(namespace));
        let resolve = |count| {
            partitions
                .segment_delete_resolution(&namespace, count)
                .map(|(up_to_offset, lagging, _)| (up_to_offset, lagging))
        };
        assert_eq!(
            resolve(1),
            Some((None, false)),
            "a converged primary settles the no-op"
        );

        {
            let consensus = partitions.get_by_ns(&namespace).unwrap().consensus();
            consensus.advance_commit_max(consensus.commit_min() + 1);
        }
        assert_eq!(
            resolve(1),
            Some((None, true)),
            "the waiting commits can still seal the first segment"
        );

        let partition = partitions.get_mut_by_ns(&namespace).unwrap();
        let first = &mut partition.log.segments_mut()[0];
        first.end_offset = SEALED_END;
        first.sealed = true;
        partition.log.add_persisted_segment(
            Segment::new(SEALED_END + 1, segment_size),
            SegmentStorage::default(),
            None,
            None,
        );
        for count in [1, 2] {
            assert_eq!(
                resolve(count),
                Some((Some(SEALED_END), false)),
                "count {count} resolves to a sealed segment, which the waiting commits cannot change"
            );
        }
    }

    /// A shard whose repair rings share `bytes_max`, holding a replicated
    /// partition for each of `namespaces`.
    fn shard_with_ring_budget(
        bytes_max: usize,
        namespaces: &[IggyNamespace],
    ) -> IggyPartitions<IggyMessageBus> {
        let partitions = IggyPartitions::new(ShardId::new(0), shard_config())
            .with_repair_rings(Rc::new(RepairRingBudget::new(bytes_max)));
        for namespace in namespaces {
            partitions.insert(*namespace, build_replicated_partition(*namespace));
        }
        partitions
    }

    /// Evicts `ops` into the ring of `namespace` as a flush does: one-message
    /// batches that a segment holds.
    async fn grow_ring(
        partitions: &IggyPartitions<IggyMessageBus>,
        namespace: IggyNamespace,
        ops: RangeInclusive<u64>,
    ) {
        let journal = &partitions
            .get_by_ns(&namespace)
            .unwrap()
            .log
            .journal()
            .inner;
        let count = ops.clone().count();
        for op in ops {
            journal
                .append(build_send_messages_entry(namespace, op, op - 1))
                .await
                .unwrap();
        }
        journal.evict_prefix_located(count, |op| {
            Some(BodyPosition {
                segment_start: 0,
                position: op,
            })
        });
    }

    /// The ring of `namespace` over `ops`: `B` for a body in memory, `H` for a
    /// header whose body went back to its segment, `-` for a dropped op.
    fn ring_layout(
        partitions: &IggyPartitions<IggyMessageBus>,
        namespace: IggyNamespace,
        ops: RangeInclusive<u64>,
    ) -> String {
        let journal = &partitions
            .get_by_ns(&namespace)
            .unwrap()
            .log
            .journal()
            .inner;
        ops.map(|op| {
            if journal.repair_entry(op).is_some() {
                'B'
            } else if journal.evicted_header(op).is_some() {
                'H'
            } else {
                '-'
            }
        })
        .collect()
    }

    const HEADER: usize = size_of::<EvictedHeader>();
    /// A ring that released one to four bodies holds this many header slots:
    /// `VecDeque` starts with four.
    const FIRST_HEADER_SLOTS: usize = 4 * HEADER;

    /// What one body of [`grow_ring`] charges to the ring budget.
    async fn ring_body_bytes() -> usize {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = shard_with_ring_budget(usize::MAX, &[namespace]);
        grow_ring(&partitions, namespace, 1..=1).await;
        partitions.shed_repair_rings().unwrap().usage.charged_bytes
    }

    /// The budget under which a shed releases bodies until the rings hold
    /// `sheddable` bytes on top of the `floor` of their newest entries.
    const fn budget_releasing_down_to(floor: usize, sheddable: usize) -> usize {
        floor
            + sheddable * LOW_WATERMARK_DIVISOR / (LOW_WATERMARK_DIVISOR - 1)
            + LOW_WATERMARK_DIVISOR
    }

    /// Past its budget a shard releases the bodies of the ring that grew
    /// first, then those of the next ring, down to the low watermark, and
    /// drops no entry while a body is left to release.
    #[compio::test]
    async fn given_rings_past_the_shard_budget_when_shedding_should_release_the_earliest_growth_first()
     {
        let (first, second) = (IggyNamespace::new(1, 1, 0), IggyNamespace::new(1, 1, 1));
        let body = ring_body_bytes().await;
        // The low watermark is one release into the second ring.
        let released = 4 * HEADER + (2 * body + 4 * HEADER);
        let budget = budget_releasing_down_to(2 * body, released);
        let partitions = shard_with_ring_budget(budget, &[first, second]);
        grow_ring(&partitions, first, RING_OPS).await;
        grow_ring(&partitions, second, RING_OPS).await;

        let shed = partitions.shed_repair_rings().unwrap();

        assert_eq!(ring_layout(&partitions, first, RING_OPS), "HHHB");
        assert_eq!(ring_layout(&partitions, second, RING_OPS), "HBBB");
        assert_eq!((shed.bodies_released, shed.entries_dropped), (4, 0));
        assert_eq!(shed.usage.charged_bytes, 2 * body + released);
        assert_eq!(
            partitions.shed_repair_rings().unwrap().bodies_released,
            0,
            "a shard under its budget sheds nothing"
        );
    }

    /// Once no ring has a body left to release, a shard drops the oldest
    /// entries of the ring whose bodies it released first, and only as many
    /// as bring it back within the budget.
    #[compio::test]
    async fn given_no_body_left_to_release_when_shedding_should_drop_the_oldest_entries_first() {
        let (first, second) = (IggyNamespace::new(1, 1, 0), IggyNamespace::new(1, 1, 1));
        let body = ring_body_bytes().await;
        // The first ring has to go down to one header, in one slot.
        let budget = (body + HEADER) + (body + FIRST_HEADER_SLOTS);
        let partitions = shard_with_ring_budget(budget, &[first, second]);
        grow_ring(&partitions, first, RING_OPS).await;
        grow_ring(&partitions, second, RING_OPS).await;

        let shed = partitions.shed_repair_rings().unwrap();

        assert_eq!(ring_layout(&partitions, first, RING_OPS), "--HB");
        assert_eq!(ring_layout(&partitions, second, RING_OPS), "HHHB");
        assert_eq!((shed.bodies_released, shed.entries_dropped), (6, 2));
        assert_eq!(shed.usage.charged_bytes, budget);
    }

    /// A budget below the newest entries leaves every ring its newest entry,
    /// so the commit point stays servable, and shedding still returns. The
    /// header slots go back with the headers, so nothing but those entries
    /// stays charged.
    #[compio::test]
    async fn given_a_budget_below_the_newest_entries_when_shedding_should_keep_each_newest_entry() {
        let (first, second) = (IggyNamespace::new(1, 1, 0), IggyNamespace::new(1, 1, 1));
        let body = ring_body_bytes().await;
        let partitions = shard_with_ring_budget(0, &[first, second]);
        grow_ring(&partitions, first, RING_OPS).await;
        grow_ring(&partitions, second, RING_OPS).await;

        let shed = partitions.shed_repair_rings().unwrap();
        partitions.shed_repair_rings();

        assert_eq!(ring_layout(&partitions, first, RING_OPS), "---B");
        assert_eq!(ring_layout(&partitions, second, RING_OPS), "---B");
        assert_eq!((shed.bodies_released, shed.entries_dropped), (6, 6));
        assert_eq!(
            shed.usage,
            crate::RepairRingUsage {
                budget_bytes: 0,
                charged_bytes: 2 * body,
                resident_bytes: 0,
                command_bytes: 0,
                relief_bytes: 0,
                relief_excluded_bytes: 0,
                floor_bytes: 2 * body,
            }
        );
    }

    /// A partition that leaves the shard returns its charge, and the ticket
    /// it left queued does not shed the partition that takes its namespace.
    #[compio::test]
    async fn given_a_recreated_partition_when_shedding_should_skip_the_ticket_of_the_removed_one() {
        let (recreated, other) = (IggyNamespace::new(1, 1, 0), IggyNamespace::new(1, 1, 1));
        let body = ring_body_bytes().await;
        // The low watermark is every body the other ring can release.
        let budget = budget_releasing_down_to(2 * body, FIRST_HEADER_SLOTS + 3 * body);
        let partitions = shard_with_ring_budget(budget, &[recreated, other]);
        grow_ring(&partitions, recreated, RING_OPS).await;
        drop(partitions.remove(&recreated));
        grow_ring(&partitions, other, RING_OPS).await;
        partitions.insert(recreated, build_replicated_partition(recreated));
        grow_ring(&partitions, recreated, RING_OPS).await;

        partitions.shed_repair_rings();

        assert_eq!(ring_layout(&partitions, other, RING_OPS), "HHHB");
        assert_eq!(ring_layout(&partitions, recreated, RING_OPS), "BBBB");
    }

    /// A partition fenced for delete still holds its ring until it leaves,
    /// so the shed takes from that ring as from any other.
    #[compio::test]
    async fn given_a_partition_fenced_for_delete_when_shedding_should_shed_its_ring() {
        let (fenced, other) = (IggyNamespace::new(1, 1, 0), IggyNamespace::new(1, 1, 1));
        let body = ring_body_bytes().await;
        let partitions = shard_with_ring_budget(0, &[fenced, other]);
        grow_ring(&partitions, fenced, RING_OPS).await;
        grow_ring(&partitions, other, RING_OPS).await;
        partitions.tombstone(fenced);

        let shed = partitions.shed_repair_rings().unwrap();

        assert_eq!(shed.usage.charged_bytes, 2 * body);
        partitions.untombstone(&fenced);
        assert_eq!(ring_layout(&partitions, fenced, RING_OPS), "---B");
        assert_eq!(ring_layout(&partitions, other, RING_OPS), "---B");
    }

    /// A snapshot lease needs the bodies from its op on in memory. The shed
    /// leaves them and takes the ring out of its queues, so it does not visit
    /// the ring on every tick. A lease that moves puts the ring back.
    #[compio::test]
    async fn given_a_snapshot_lease_when_shedding_should_keep_its_tail_until_the_lease_moves() {
        const LEASE_FROM: u64 = 2;
        let namespace = IggyNamespace::new(1, 1, 0);
        let partitions = shard_with_ring_budget(0, &[namespace]);
        grow_ring(&partitions, namespace, RING_OPS).await;
        let journal = &partitions
            .get_by_ns(&namespace)
            .unwrap()
            .log
            .journal()
            .inner;
        let memory = Arc::new(crate::RepairMemoryBudget::new(usize::MAX));
        let lease = journal.retain_repair_from(LEASE_FROM, memory.reservation());

        let shed = partitions.shed_repair_rings().unwrap();
        assert_eq!(ring_layout(&partitions, namespace, RING_OPS), "-BBB");
        assert_eq!((shed.bodies_released, shed.entries_dropped), (1, 1));
        let idle = partitions.shed_repair_rings().unwrap();
        assert_eq!((idle.bodies_released, idle.entries_dropped), (0, 0));

        lease.advance(*RING_OPS.end());
        partitions.shed_repair_rings();
        assert_eq!(ring_layout(&partitions, namespace, RING_OPS), "---B");
    }

    fn shard_config() -> PartitionsConfig {
        PartitionsConfig {
            messages_required_to_save: 100,
            size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024_u64),
            validate_checksum: true,
            segment_size: IggyByteSize::from(1024 * 1024_u64),
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: crate::PartitionPathLayout::default(),
        }
    }

    /// A shard holding a replicated partition for each of `namespaces`,
    /// inserted in that order.
    fn shard_with(namespaces: &[IggyNamespace]) -> IggyPartitions<IggyMessageBus> {
        let partitions = IggyPartitions::new(ShardId::new(0), shard_config());
        for namespace in namespaces {
            partitions.insert(*namespace, build_replicated_partition(*namespace));
        }
        partitions
    }

    /// The partition ids of a walk over the shard.
    fn walk(partitions: &IggyPartitions<IggyMessageBus>) -> Vec<usize> {
        partitions
            .namespaces()
            .map(IggyNamespace::partition_id)
            .collect()
    }

    /// A removal frees only its own slot: the other partitions keep theirs,
    /// and the next insert takes the freed one.
    #[test]
    fn given_a_removed_partition_when_inserting_should_reuse_its_slot() {
        let [first, removed, last, newcomer] = [0, 1, 2, 3].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with(&[first, removed, last]);
        let (first_slot, last_slot) = (partitions.local_idx(&first), partitions.local_idx(&last));
        let freed = partitions.local_idx(&removed);
        let (_, sent) = partitions.sent_report(&removed).unwrap();
        sent.set(SentReport {
            view: 3,
            placed: true,
            primary: true,
            queued: true,
            ..SentReport::default()
        });

        drop(partitions.remove(&removed));
        assert_eq!(partitions.len(), 2, "len counts live partitions, not slots");
        assert_eq!(partitions.local_idx(&first), first_slot);
        assert_eq!(partitions.local_idx(&last), last_slot);

        let newcomer_slot = partitions.insert(newcomer, build_replicated_partition(newcomer));
        assert_eq!(Some(newcomer_slot), freed);
        assert_eq!(partitions.len(), 3);
        assert!(partitions.get_by_ns(&removed).is_none());
        assert_eq!(
            partitions
                .get_by_ns(&newcomer)
                .map(|partition| partition.consensus().group()),
            Some(newcomer.inner()),
            "a lookup after the reuse finds the new partition"
        );
        assert_eq!(
            partitions.sent_report(&newcomer).unwrap().1.get(),
            SentReport::default(),
            "a reused slot owes a first report, not the removed partition's"
        );
    }

    /// Walks follow the slots and skip the vacant ones, so the same inserts
    /// and removes always give the same order, even out of numeric order.
    #[test]
    fn given_vacant_and_reused_slots_when_walking_should_follow_slot_order() {
        let replay = || {
            let [five, six, seven, one] = [5, 6, 7, 1].map(|id| IggyNamespace::new(1, 1, id));
            let partitions = shard_with(&[five, six, seven]);
            drop(partitions.remove(&six));
            let with_hole = walk(&partitions);
            partitions.insert(one, build_replicated_partition(one));
            (with_hole, walk(&partitions))
        };

        let (with_hole, reused) = replay();
        assert_eq!(with_hole, [5, 7], "a vacant slot is skipped");
        assert_eq!(reused, [5, 1, 7], "the reused slot keeps its place");
        assert_eq!(replay(), (with_hole, reused));
    }

    /// The slot array holds boxes, so its growth moves pointers and never a
    /// partition.
    #[test]
    fn given_a_mutably_borrowed_partition_when_inserts_grow_the_slots_should_keep_its_address() {
        const INSERTS_MAX: usize = 64;
        let first = IggyNamespace::new(1, 1, 0);
        let partitions = shard_with(&[first]);
        let borrowed: *const IggyPartition<IggyMessageBus> =
            partitions.get_mut_by_ns(&first).unwrap();
        let slots_before = partitions.slots().as_ptr();

        let mut id = 1;
        while partitions.slots().as_ptr() == slots_before {
            assert!(id <= INSERTS_MAX, "the slot array never moved");
            let namespace = IggyNamespace::new(1, 1, id);
            partitions.insert(namespace, build_replicated_partition(namespace));
            id += 1;
        }
        assert!(std::ptr::eq(
            partitions.get_mut_by_ns(&first).unwrap(),
            borrowed
        ));
    }

    /// A second insert of a namespace replaces its partition in the same
    /// slot, so a walk still meets the namespace once.
    #[test]
    fn given_a_live_namespace_when_inserting_it_again_should_replace_its_partition() {
        let [replaced, other] = [0, 1].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with(&[replaced, other]);
        let slot = partitions.local_idx(&replaced);
        let replacement = build_replicated_partition(replaced);
        let incarnation = replacement.incarnation();

        assert_eq!(Some(partitions.insert(replaced, replacement)), slot);
        assert_eq!(partitions.len(), 2);
        assert_eq!(walk(&partitions), [0, 1]);
        assert_eq!(
            partitions
                .get_by_ns(&replaced)
                .map(IggyPartition::incarnation),
            Some(incarnation)
        );
    }

    /// The partition ids that a shard tick at `now` visits.
    fn due(partitions: &IggyPartitions<IggyMessageBus>, now: u64) -> Vec<usize> {
        let mut due = Vec::new();
        partitions.due_namespaces(now, &mut due);
        due.iter().map(IggyNamespace::partition_id).collect()
    }

    #[test]
    fn given_a_settled_partition_when_ticks_pass_should_be_due_from_its_wake_tick_on() {
        let [quiet, busy] = [0, 1].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with(&[quiet, busy]);
        assert_eq!(due(&partitions, 1), [0, 1], "a new partition is due");

        partitions.settle(&quiet, Some(10));
        partitions.settle(&busy, None);
        assert_eq!(due(&partitions, 9), [1]);
        assert_eq!(due(&partitions, 10), [0, 1]);
        assert_eq!(due(&partitions, 11), [0, 1], "due until the next settle");
    }

    /// No path that reaches a partition through the table leaves it asleep.
    #[test]
    fn given_a_sleeping_partition_when_the_table_hands_it_out_should_be_due_again() {
        type Access = fn(&IggyPartitions<IggyMessageBus>, IggyNamespace);
        let accesses: [(&str, Access); 9] = [
            ("get_by_ns", |partitions, namespace| {
                partitions.get_by_ns(&namespace).unwrap();
            }),
            ("get_mut_by_ns", |partitions, namespace| {
                partitions.get_mut_by_ns(&namespace).unwrap();
            }),
            ("with_partition", |partitions, namespace| {
                partitions.with_partition(&namespace, |_| ()).unwrap();
            }),
            ("get_io_owner", |partitions, namespace| {
                partitions.get_io_owner(&namespace).unwrap();
            }),
            ("sent_report", |partitions, namespace| {
                partitions.sent_report(&namespace).unwrap();
            }),
            ("capture_teardown", |partitions, namespace| {
                partitions.capture_teardown(&namespace).unwrap();
            }),
            ("tombstone", |partitions, namespace| {
                partitions.tombstone(namespace);
            }),
            ("fence", |partitions, namespace| {
                partitions.fence(namespace, 1);
            }),
            ("untombstone", |partitions, namespace| {
                partitions.untombstone(&namespace);
            }),
        ];
        let namespace = IggyNamespace::new(1, 1, 0);
        for (name, access) in accesses {
            let partitions = shard_with(&[namespace]);
            partitions.settle(&namespace, Some(u64::MAX));
            assert!(due(&partitions, 1).is_empty(), "{name}: asleep");
            assert!(partitions.sleeper(0, 1).is_some(), "{name}: a sleeper");

            access(&partitions, namespace);
            assert_eq!(due(&partitions, 1), [0], "{name}");
            assert!(partitions.sleeper(0, 1).is_none(), "{name}: awake");
        }
    }

    #[test]
    fn given_a_sleeping_partition_when_removed_should_leave_its_slot_awake_for_the_next_one() {
        let [removed, newcomer] = [0, 1].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with(&[removed]);
        partitions.settle(&removed, Some(u64::MAX));

        drop(partitions.remove(&removed));
        assert!(due(&partitions, 1).is_empty(), "a vacant slot is never due");
        assert!(
            partitions.sleeper(0, 1).is_none(),
            "a vacant slot is no sleeper"
        );

        partitions.insert(newcomer, build_replicated_partition(newcomer));
        assert_eq!(due(&partitions, 1), [1]);
    }

    #[test]
    fn given_sleeping_partitions_when_waking_all_should_make_each_one_due() {
        let namespaces = [0, 1, 2].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with(&namespaces);
        drop(partitions.remove(&namespaces[1]));
        partitions.settle(&namespaces[0], Some(u64::MAX));
        partitions.settle(&namespaces[2], Some(50));

        partitions.wake_all();
        assert_eq!(due(&partitions, 1), [0, 2]);
    }

    /// The shed looks at every ring on every tick, so it wakes only the
    /// partitions it takes memory from.
    #[compio::test]
    async fn given_sleeping_ring_owners_when_shedding_should_wake_only_the_shed_ones() {
        let [shed, kept] = [0, 1].map(|id| IggyNamespace::new(1, 1, id));
        let body = ring_body_bytes().await;
        // The low watermark is one release into the first ring.
        let budget = budget_releasing_down_to(2 * body, 4 * HEADER + 4 * body);
        let partitions = shard_with_ring_budget(budget, &[shed, kept]);
        grow_ring(&partitions, shed, RING_OPS).await;
        grow_ring(&partitions, kept, RING_OPS).await;
        partitions.settle(&shed, Some(u64::MAX));
        partitions.settle(&kept, Some(u64::MAX));

        assert!(partitions.shed_repair_rings().unwrap().bodies_released > 0);
        assert_eq!(due(&partitions, 1), [0]);
        // The layout read hands the partition out, so it comes after.
        assert_eq!(ring_layout(&partitions, kept, RING_OPS), "BBBB");

        partitions.settle(&shed, Some(u64::MAX));
        partitions.settle(&kept, Some(u64::MAX));
        assert_eq!(partitions.shed_repair_rings().unwrap().bodies_released, 0);
        assert!(
            due(&partitions, 1).is_empty(),
            "nothing shed, nothing woken"
        );
    }

    /// The sums count a sleeping partition at the levels of its last settle,
    /// and a removed one not at all.
    #[compio::test]
    async fn given_settled_partitions_when_reading_the_gauge_sums_should_count_each_last_share() {
        let [first, second] = [0, 1].map(|id| IggyNamespace::new(1, 1, id));
        let partitions = shard_with_ring_budget(usize::MAX, &[first, second]);
        grow_ring(&partitions, first, RING_OPS).await;
        grow_ring(&partitions, second, 1..=1).await;
        assert_eq!(partitions.gauge_sums(), GaugeShare::default());

        partitions.settle(&first, Some(u64::MAX));
        let first_share = partitions.gauge_sums();
        assert_eq!(first_share.repair_ring_entries, RING_OPS.count());
        assert!(first_share.repair_ring_bytes > 0);
        assert!(partitions.sleeper(0, 1).unwrap().gauge_share_current);

        partitions.settle(&second, None);
        assert_eq!(
            partitions.gauge_sums().repair_ring_entries,
            RING_OPS.count() + 1
        );

        // The ring grows with no settle: the sums keep the old share, and the
        // check for missed wake-ups sees the difference.
        partitions.settle(&second, Some(u64::MAX));
        let journal = &partitions.slots()[1].as_ref().unwrap().partition().log;
        journal
            .journal()
            .inner
            .append(build_send_messages_entry(second, 2, 1))
            .await
            .unwrap();
        journal.journal().inner.evict_prefix_located(1, |op| {
            Some(BodyPosition {
                segment_start: 0,
                position: op,
            })
        });
        assert!(!partitions.sleeper(1, 1).unwrap().gauge_share_current);

        drop(partitions.remove(&second));
        assert_eq!(partitions.gauge_sums(), first_share);
        drop(partitions.remove(&first));
        assert_eq!(partitions.gauge_sums(), GaugeShare::default());
    }
}
