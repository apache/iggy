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

use server_common::sharding::IggyNamespace;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The shed releases bodies until the charge above the floor is this part
/// under what the budget allows. With no gap, a shard at its budget sheds a
/// few bytes on every tick.
pub const LOW_WATERMARK_DIVISOR: usize = 8;

/// Byte budget for the journal memory of the partitions on one shard.
///
/// Each journal charges a lower bound of the heap it holds (`JournalBytes`):
/// its resident entries with their index keys, its held commands, and its
/// repair ring. A ring charges the allocation of each prepare in memory, and
/// the slots of its `EvictedHeader` deque, which keeps one header for each
/// prepare whose body it released to a segment. Past the budget the shard sheds
/// (`IggyPartitions::shed_repair_rings`): it releases ring bodies, asks
/// flushes for the `Relief` they bring, and drops ring entries and
/// checkpointed commands only for the excess that no relief covers.
///
/// Every ring keeps its newest entry, and every journal the command at its
/// commit point, so these entries are a floor that no shed takes. A floor
/// above the budget means the budget is too small for the partitions on the
/// shard: every ring is then down to its newest entry.
/// The uncommitted resident entries sit above the floor, because no shed
/// takes them either: they leave only once committed and flushed. A burst of
/// them takes the rings down to their newest entries.
///
/// Each shard owns one, because a shard cannot shed the ring of another
/// shard. [`RepairMemoryBudget`] is the sibling that the whole node shares.
#[derive(Debug)]
pub struct RepairRingBudget {
    bytes_max: usize,
    used: Cell<JournalBytes>,
    /// The part of `used` that no shed takes.
    floor: Cell<usize>,
    floor_over: Cell<bool>,
    /// Live charges. A queue holds at most one live entry per live charge,
    /// its ticket or the stamp that its relief holds, so a queue longer than
    /// twice this count is at least half stale.
    charges: Cell<usize>,
    next_charge: Cell<u64>,
    /// Rings that took new entries since the shard last released their
    /// bodies, in the order they grew.
    grown: RefCell<VecDeque<RingTicket>>,
    /// Rings with no body left to release but more than their newest entry,
    /// in the order the shard released their bodies.
    released: RefCell<VecDeque<RingTicket>>,
    /// The relief of the journals whose [`Relief`] counts it.
    relief_counted: Cell<usize>,
    /// The part of `relief_counted` that asked flushes bring.
    relief_asked: Cell<usize>,
    /// Journals whose relief rose from zero, or stayed after a flush, in that
    /// order. The shed asks them for a flush.
    relief_waiting: RefCell<VecDeque<RingTicket>>,
    /// The stamp of every [`Relief::Skipped`] and [`Relief::Asked`], oldest
    /// first. A stamp that the journal no longer holds is stale.
    relief_stamps: RefCell<VecDeque<(RingTicket, u64)>>,
    /// Journals whose held commands grew, in that order.
    commands_grown: RefCell<VecDeque<RingTicket>>,
}

/// Where the relief of one journal stands with the shed. The relief is the
/// memory of its committed resident messages, which a flush moves to the
/// ring, where the shed then releases their bodies. Every state but
/// [`Relief::Excluded`] counts it, so the shed drops nothing for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relief {
    /// No flush asked since the last one landed.
    Waiting,
    /// The partition could not take a flush, first at this tick.
    Skipped(u64),
    /// The shed asked a flush at this tick.
    Asked(u64),
    /// No flush comes in time: the skip or the ask is too old, or the
    /// partition cannot flush. The next flush that lands ends it.
    Excluded,
}

impl Relief {
    const fn counts(self) -> bool {
        !matches!(self, Self::Excluded)
    }

    const fn asked(self) -> bool {
        matches!(self, Self::Asked(_))
    }

    const fn stamp(self) -> Option<u64> {
        match self {
            Self::Skipped(stamp) | Self::Asked(stamp) => Some(stamp),
            Self::Waiting | Self::Excluded => None,
        }
    }
}

/// What the journals on a shard hold in memory, by holder.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalBytes {
    /// The repair ring: its bodies in memory and its header slots.
    pub(crate) ring: usize,
    /// The resident entries with their headers and index entries.
    pub(crate) resident: usize,
    /// The held state machine commands.
    pub(crate) commands: usize,
    /// The part of `resident` that committed messages hold, see [`Relief`].
    pub(crate) relief: usize,
}

impl JournalBytes {
    const fn add(self, other: Self) -> Self {
        Self {
            ring: self.ring + other.ring,
            resident: self.resident + other.resident,
            commands: self.commands + other.commands,
            relief: self.relief + other.relief,
        }
    }

    const fn sub(self, other: Self) -> Self {
        Self {
            ring: self.ring.saturating_sub(other.ring),
            resident: self.resident.saturating_sub(other.resident),
            commands: self.commands.saturating_sub(other.commands),
            relief: self.relief.saturating_sub(other.relief),
        }
    }

    const fn total(self) -> usize {
        self.ring + self.resident + self.commands
    }
}

/// Where a [`RepairRingBudget`] stands, for the shard's gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRingUsage {
    pub budget_bytes: usize,
    /// What the repair rings charge.
    pub charged_bytes: usize,
    /// What the resident entries charge.
    pub resident_bytes: usize,
    /// What the held commands charge.
    pub command_bytes: usize,
    /// Resident bytes that flushes free in time, see `Relief`.
    pub relief_bytes: usize,
    /// Committed resident bytes whose flush does not come in time.
    pub relief_excluded_bytes: usize,
    pub floor_bytes: usize,
}

/// What one `IggyPartitions::shed_repair_rings` took from the rings, and
/// where it left the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRingShed {
    /// Entries whose body went back to its segment. Repair reads them from
    /// disk.
    pub bodies_released: usize,
    /// Entries the rings no longer serve.
    pub entries_dropped: usize,
    /// Flushes asked for the relief they bring.
    pub flushes_asked: usize,
    /// Checkpoints asked, so that held commands can leave.
    pub checkpoints_asked: usize,
    /// Checkpointed commands released. Repair no longer serves them.
    pub commands_released: usize,
    pub usage: RepairRingUsage,
}

impl RepairRingBudget {
    #[must_use]
    pub const fn new(bytes_max: usize) -> Self {
        Self {
            bytes_max,
            used: Cell::new(JournalBytes {
                ring: 0,
                resident: 0,
                commands: 0,
                relief: 0,
            }),
            floor: Cell::new(0),
            floor_over: Cell::new(false),
            charges: Cell::new(0),
            next_charge: Cell::new(0),
            grown: RefCell::new(VecDeque::new()),
            released: RefCell::new(VecDeque::new()),
            relief_counted: Cell::new(0),
            relief_asked: Cell::new(0),
            relief_waiting: RefCell::new(VecDeque::new()),
            relief_stamps: RefCell::new(VecDeque::new()),
            commands_grown: RefCell::new(VecDeque::new()),
        }
    }

    #[must_use]
    pub const fn usage(&self) -> RepairRingUsage {
        let used = self.used.get();
        RepairRingUsage {
            budget_bytes: self.bytes_max,
            charged_bytes: used.ring,
            resident_bytes: used.resident,
            command_bytes: used.commands,
            relief_bytes: self.relief_counted.get(),
            relief_excluded_bytes: used.relief.saturating_sub(self.relief_counted.get()),
            floor_bytes: self.floor.get(),
        }
    }

    /// Bytes past the budget that a shed can take, or `None` within it.
    pub(crate) fn excess(&self) -> Option<usize> {
        self.excess_over(self.sheddable_max())
    }

    /// Whether the asked flushes bring less than the excess, so the shed asks
    /// more.
    pub(crate) fn wants_flushes(&self) -> bool {
        self.excess()
            .is_some_and(|excess| excess > self.relief_asked.get())
    }

    /// The excess that no counted relief covers, which only drops and command
    /// releases take.
    pub(crate) fn costly_excess(&self) -> Option<usize> {
        self.excess()?
            .checked_sub(self.relief_counted.get())
            .filter(|excess| *excess > 0)
    }

    /// [`Self::excess`] measured against the low watermark.
    pub(crate) fn excess_over_low_watermark(&self) -> Option<usize> {
        let sheddable_max = self.sheddable_max();
        self.excess_over(sheddable_max - sheddable_max / LOW_WATERMARK_DIVISOR)
    }

    /// `Some(over)` when the floor rose past the budget, or fell back under
    /// its low watermark, since the last call. The gap keeps a floor that
    /// sits at the budget from reporting on every tick.
    pub(crate) fn floor_crossed(&self) -> Option<bool> {
        let limit = if self.floor_over.get() {
            self.bytes_max - self.bytes_max / LOW_WATERMARK_DIVISOR
        } else {
            self.bytes_max
        };
        let over = self.floor.get() > limit;
        (over != self.floor_over.replace(over)).then_some(over)
    }

    pub(crate) fn oldest_grown(&self) -> Option<RingTicket> {
        self.grown.borrow().front().copied()
    }

    pub(crate) fn pop_grown(&self) {
        self.grown.borrow_mut().pop_front();
    }

    pub(crate) fn oldest_released(&self) -> Option<RingTicket> {
        self.released.borrow().front().copied()
    }

    pub(crate) fn pop_released(&self) {
        self.released.borrow_mut().pop_front();
    }

    pub(crate) fn pop_relief_waiting(&self) -> Option<RingTicket> {
        self.relief_waiting.borrow_mut().pop_front()
    }

    pub(crate) fn relief_waiting_len(&self) -> usize {
        self.relief_waiting.borrow().len()
    }

    pub(crate) fn pop_commands_grown(&self) -> Option<RingTicket> {
        self.commands_grown.borrow_mut().pop_front()
    }

    pub(crate) fn commands_grown_len(&self) -> usize {
        self.commands_grown.borrow().len()
    }

    /// The oldest stamp that is `wait_ticks` old at `now`.
    pub(crate) fn pop_expired_stamp(&self, now: u64, wait_ticks: u64) -> Option<(RingTicket, u64)> {
        let mut stamps = self.relief_stamps.borrow_mut();
        stamps
            .front()
            .is_some_and(|(_, stamp)| stamp.saturating_add(wait_ticks) <= now)
            .then(|| stamps.pop_front())
            .flatten()
    }

    /// Drops the stale entries once a queue is twice as long as the live
    /// charges: the tickets whose charge is gone, and the stamps that their
    /// charge no longer holds. `relief_of` gives the relief of the live
    /// charge that a ticket names. A partition that leaves the shard leaves
    /// its tickets behind, and a shard within its budget never visits them,
    /// so without this its queues grow with every partition it ever held. A
    /// stamp waits for its expiry, and every flush that lands leaves one.
    pub(crate) fn retain_live(&self, mut relief_of: impl FnMut(RingTicket) -> Option<Relief>) {
        let limit = 2 * self.charges.get();
        for queue in [
            &self.grown,
            &self.released,
            &self.relief_waiting,
            &self.commands_grown,
        ] {
            let mut queue = queue.borrow_mut();
            if queue.len() > limit {
                queue.retain(|ticket| relief_of(*ticket).is_some());
            }
        }
        let mut stamps = self.relief_stamps.borrow_mut();
        if stamps.len() > limit {
            stamps.retain(|(ticket, stamp)| {
                relief_of(*ticket).and_then(Relief::stamp) == Some(*stamp)
            });
        }
    }

    /// What the budget leaves for the entries a shed can take.
    const fn sheddable_max(&self) -> usize {
        self.bytes_max.saturating_sub(self.floor.get())
    }

    fn excess_over(&self, sheddable_max: usize) -> Option<usize> {
        self.used
            .get()
            .total()
            .saturating_sub(self.floor.get())
            .checked_sub(sheddable_max)
            .filter(|excess| *excess > 0)
    }
}

/// Names one [`RingCharge`] in a budget queue. A ticket outlives its charge
/// when the partition leaves the shard or takes a new charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingTicket {
    pub(crate) namespace: IggyNamespace,
    charge: u64,
}

/// One ring's share of a [`RepairRingBudget`], returned on drop. The ring is
/// in a budget queue exactly while its flag for that queue is set: the shard
/// takes the ticket of a live charge out of a queue only after the ring
/// cleared the flag.
#[derive(Debug)]
pub struct RingCharge {
    budget: Rc<RepairRingBudget>,
    ticket: RingTicket,
    bytes: Cell<JournalBytes>,
    floor: Cell<usize>,
    in_grown: Cell<bool>,
    in_released: Cell<bool>,
    relief: Cell<Relief>,
    in_relief_waiting: Cell<bool>,
    in_commands_grown: Cell<bool>,
}

impl RingCharge {
    pub(crate) fn new(budget: Rc<RepairRingBudget>, namespace: IggyNamespace) -> Self {
        let charge = budget.next_charge.get();
        budget.next_charge.set(charge + 1);
        budget.charges.set(budget.charges.get() + 1);
        Self {
            budget,
            ticket: RingTicket { namespace, charge },
            bytes: Cell::new(JournalBytes::default()),
            floor: Cell::new(0),
            in_grown: Cell::new(false),
            in_released: Cell::new(false),
            relief: Cell::new(Relief::Waiting),
            in_relief_waiting: Cell::new(false),
            in_commands_grown: Cell::new(false),
        }
    }

    pub(crate) const fn ticket(&self) -> RingTicket {
        self.ticket
    }

    /// The `(bytes, floor)` of the last [`Self::update`].
    pub(crate) const fn charged(&self) -> (JournalBytes, usize) {
        (self.bytes.get(), self.floor.get())
    }

    /// Moves this journal's charge to `bytes`, of which no shed takes
    /// `floor`. A ring that took new entries waits for the shard to release
    /// its bodies.
    pub(crate) fn update(&self, bytes: JournalBytes, floor: usize, grew: bool) {
        let budget = &self.budget;
        let previous = self.bytes.replace(bytes);
        budget.used.set(budget.used.get().sub(previous).add(bytes));
        self.shift_relief(self.relief.get(), previous.relief, bytes.relief);
        let previous_floor = self.floor.replace(floor);
        budget
            .floor
            .set(budget.floor.get().saturating_sub(previous_floor) + floor);
        if grew {
            self.queue_grown();
        }
        if previous.relief == 0 && bytes.relief > 0 {
            self.queue_relief();
        }
        if bytes.commands > previous.commands {
            self.queue_commands();
        }
    }

    pub(crate) const fn relief(&self) -> Relief {
        self.relief.get()
    }

    /// The relief of the last [`Self::update`].
    pub(crate) const fn relief_bytes(&self) -> usize {
        self.bytes.get().relief
    }

    /// The held commands of the last [`Self::update`].
    pub(crate) const fn command_bytes(&self) -> usize {
        self.bytes.get().commands
    }

    /// Ends the relief of a skip or an ask that `stamp` names and that the
    /// journal still waits on.
    pub(crate) fn expire_relief(&self, stamp: u64) {
        if self.relief.get().stamp() == Some(stamp) {
            self.set_relief(Relief::Excluded);
        }
    }

    /// Moves the relief of this journal into `state`. A new stamp waits in
    /// the budget for its expiry.
    pub(crate) fn set_relief(&self, state: Relief) {
        let relief = self.bytes.get().relief;
        let previous = self.relief.replace(state);
        self.shift_relief(previous, relief, 0);
        self.shift_relief(state, 0, relief);
        if let Some(stamp) = state.stamp()
            && previous.stamp() != Some(stamp)
        {
            self.budget
                .relief_stamps
                .borrow_mut()
                .push_back((self.ticket, stamp));
        }
    }

    /// A flush landed: whatever the state, the relief that stays waits for
    /// the next ask.
    pub(crate) fn relief_landed(&self) {
        self.set_relief(Relief::Waiting);
        if self.bytes.get().relief > 0 {
            self.queue_relief();
        }
    }

    /// Puts the journal in the queue of relief asks, unless it waits there
    /// already.
    pub(crate) fn queue_relief(&self) {
        if !self.in_relief_waiting.replace(true) {
            self.budget
                .relief_waiting
                .borrow_mut()
                .push_back(self.ticket);
        }
    }

    /// The shard took the journal out of the queue of relief asks.
    pub(crate) fn relief_visited(&self) {
        self.in_relief_waiting.set(false);
    }

    /// Puts the journal in the queue of grown commands, unless it waits
    /// there already.
    pub(crate) fn queue_commands(&self) {
        if !self.in_commands_grown.replace(true) {
            self.budget
                .commands_grown
                .borrow_mut()
                .push_back(self.ticket);
        }
    }

    /// The shard took the journal out of the queue of grown commands.
    pub(crate) fn commands_visited(&self) {
        self.in_commands_grown.set(false);
    }

    /// Moves `remove` out of and `add` into the sums that `state` counts in.
    fn shift_relief(&self, state: Relief, remove: usize, add: usize) {
        let budget = &self.budget;
        if state.counts() {
            budget
                .relief_counted
                .set(budget.relief_counted.get().saturating_sub(remove) + add);
        }
        if state.asked() {
            budget
                .relief_asked
                .set(budget.relief_asked.get().saturating_sub(remove) + add);
        }
    }

    /// Puts the ring in the grown queue, unless it waits there already.
    pub(crate) fn queue_grown(&self) {
        if !self.in_grown.replace(true) {
            self.budget.grown.borrow_mut().push_back(self.ticket);
        }
    }

    /// The shard released every body it could and takes the ring out of the
    /// grown queue. A ring with an entry the shard can drop waits for that.
    pub(crate) fn bodies_released(&self, droppable: bool) {
        self.in_grown.set(false);
        if droppable && !self.in_released.replace(true) {
            self.budget.released.borrow_mut().push_back(self.ticket);
        }
    }

    /// The shard dropped every entry it could and takes the ring out of the
    /// released queue.
    pub(crate) fn entries_dropped(&self) {
        self.in_released.set(false);
    }
}

impl Drop for RingCharge {
    fn drop(&mut self) {
        let budget = &self.budget;
        self.shift_relief(self.relief.get(), self.bytes.get().relief, 0);
        budget.used.set(budget.used.get().sub(self.bytes.get()));
        budget
            .floor
            .set(budget.floor.get().saturating_sub(self.floor.get()));
        budget.charges.set(budget.charges.get().saturating_sub(1));
    }
}

/// Allocation budget shared by unproven repairs and snapshot repair tails.
///
/// Reservations precede journaling and last until the proof finishes or the
/// retained window is discarded. Normal prepare queues have separate limits.
///
/// One for the whole node, shared by its shards: a reservation fails when the
/// node is out of budget, and no shard has to take memory from another.
#[derive(Debug)]
pub struct RepairMemoryBudget {
    bytes_max: usize,
    used: AtomicUsize,
}

impl RepairMemoryBudget {
    #[must_use]
    pub const fn new(bytes_max: usize) -> Self {
        Self {
            bytes_max,
            used: AtomicUsize::new(0),
        }
    }

    pub(crate) fn reservation(self: &Arc<Self>) -> RepairMemoryReservation {
        RepairMemoryReservation {
            budget: Arc::clone(self),
            bytes: 0,
            exhausted: false,
        }
    }
}

pub struct RepairMemoryReservation {
    budget: Arc<RepairMemoryBudget>,
    bytes: usize,
    exhausted: bool,
}

impl RepairMemoryReservation {
    pub(crate) fn sibling(&self) -> Self {
        self.budget.reservation()
    }

    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn try_add(&mut self, bytes: usize) -> bool {
        if self.exhausted {
            return false;
        }
        if bytes == 0 {
            return true;
        }
        if self
            .budget
            .used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.budget.bytes_max)
            })
            .is_err()
        {
            self.exhausted = true;
            return false;
        }
        self.bytes += bytes;
        true
    }

    pub(crate) fn release(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.bytes -= bytes;
        self.budget.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(crate) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn clear(&mut self) {
        self.release(self.bytes);
        self.exhausted = false;
    }
}

impl Drop for RepairMemoryReservation {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    fn ring(bytes: usize) -> JournalBytes {
        JournalBytes {
            ring: bytes,
            ..JournalBytes::default()
        }
    }

    #[test]
    fn given_concurrent_shards_when_reserving_should_share_one_limit_and_release_on_drop() {
        const SHARDS: usize = 8;
        const CHARGE: usize = 1024;
        const CAPACITY: usize = 3 * CHARGE;
        let budget = Arc::new(RepairMemoryBudget::new(CAPACITY));
        let barrier = Arc::new(Barrier::new(SHARDS));
        let accepted = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..SHARDS {
                scope.spawn(|| {
                    let mut reservation = budget.reservation();
                    barrier.wait();
                    if reservation.try_add(CHARGE) {
                        accepted.fetch_add(1, Ordering::Relaxed);
                    }
                    barrier.wait();
                });
            }
        });
        assert_eq!(accepted.load(Ordering::Relaxed), CAPACITY / CHARGE);
        assert_eq!(budget.used.load(Ordering::Relaxed), 0);
        assert!(budget.reservation().try_add(CAPACITY));
    }

    /// Partitions that leave a shard within its budget leave their tickets
    /// queued. Retaining keeps a queue within twice the live charges, and a
    /// charge that leaves returns its bytes.
    #[test]
    fn given_charges_that_leave_while_queued_when_retaining_should_bound_the_queue() {
        const LEAVING: usize = 64;
        const RING_BYTES: usize = 4096;
        const NEWEST_BYTES: usize = 1024;
        let budget = Rc::new(RepairRingBudget::new(usize::MAX));
        let staying = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));
        staying.update(ring(RING_BYTES), NEWEST_BYTES, true);
        for partition in 1..=LEAVING {
            let leaving = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, partition));
            leaving.update(ring(RING_BYTES), NEWEST_BYTES, true);
            drop(leaving);
            budget.retain_live(|ticket| (ticket == staying.ticket()).then_some(staying.relief()));
            let queued = budget.grown.borrow().len();
            assert!(queued <= 2, "{queued} tickets queued for one live charge");
        }
        assert_eq!(
            budget.usage(),
            RepairRingUsage {
                budget_bytes: usize::MAX,
                charged_bytes: RING_BYTES,
                resident_bytes: 0,
                command_bytes: 0,
                relief_bytes: 0,
                relief_excluded_bytes: 0,
                floor_bytes: NEWEST_BYTES,
            }
        );
        assert_eq!(budget.oldest_grown(), Some(staying.ticket()));
    }

    /// Every flush that lands leaves the stamp of its ask until that stamp
    /// expires. Retaining drops those stamps and keeps the one the journal
    /// still waits on, so its relief can still expire.
    #[test]
    fn given_flushes_that_landed_when_retaining_should_keep_only_the_stamp_the_journal_holds() {
        const RELIEF: usize = 800;
        const LANDED_FLUSHES: u64 = 10;
        let budget = Rc::new(RepairRingBudget::new(usize::MAX));
        let charge = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));
        charge.update(
            JournalBytes {
                resident: RELIEF,
                relief: RELIEF,
                ..JournalBytes::default()
            },
            0,
            false,
        );
        for tick in 0..LANDED_FLUSHES {
            charge.set_relief(Relief::Asked(tick));
            charge.relief_landed();
        }
        charge.set_relief(Relief::Asked(LANDED_FLUSHES));

        budget.retain_live(|ticket| (ticket == charge.ticket()).then_some(charge.relief()));
        assert_eq!(
            budget
                .relief_stamps
                .borrow()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![(charge.ticket(), LANDED_FLUSHES)]
        );
    }

    /// The newest entry of every ring stays, so the shed works against what
    /// the budget leaves above those entries, and releases bodies down to a
    /// low watermark.
    #[test]
    fn given_a_floor_when_past_the_budget_should_report_only_what_a_shed_can_take() {
        const BUDGET: usize = 8000;
        const FLOOR: usize = 2000;
        const SHEDDABLE_MAX: usize = BUDGET - FLOOR;
        let budget = Rc::new(RepairRingBudget::new(BUDGET));
        let charge = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));

        charge.update(ring(BUDGET), FLOOR, true);
        assert_eq!(budget.excess(), None);
        assert_eq!(budget.excess_over_low_watermark(), Some(SHEDDABLE_MAX / 8));

        charge.update(ring(BUDGET + 1), FLOOR, true);
        assert_eq!(budget.excess(), Some(1));
        assert_eq!(
            budget.excess_over_low_watermark(),
            Some(SHEDDABLE_MAX / 8 + 1)
        );

        // A floor past the budget leaves nothing for other entries.
        charge.update(ring(BUDGET + FLOOR), BUDGET + 1, true);
        assert_eq!(budget.excess(), Some(FLOOR - 1));
        assert_eq!(budget.excess_over_low_watermark(), Some(FLOOR - 1));
    }

    /// The floor reports once when it passes the budget, and once when it is
    /// back under the low watermark, not on every move around the budget.
    #[test]
    fn given_a_floor_around_the_budget_when_it_moves_should_report_each_crossing_once() {
        const BUDGET: usize = 8000;
        let budget = Rc::new(RepairRingBudget::new(BUDGET));
        let charge = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));

        charge.update(ring(BUDGET), BUDGET, false);
        assert_eq!(budget.floor_crossed(), None);
        charge.update(ring(BUDGET + 1), BUDGET + 1, false);
        assert_eq!(budget.floor_crossed(), Some(true));
        assert_eq!(budget.floor_crossed(), None);
        charge.update(ring(BUDGET), BUDGET, false);
        assert_eq!(budget.floor_crossed(), None);
        charge.update(ring(BUDGET - BUDGET / 8), BUDGET - BUDGET / 8, false);
        assert_eq!(budget.floor_crossed(), Some(false));
        assert_eq!(budget.floor_crossed(), None);
    }

    /// The relief of a journal counts until its skip or ask is too old, and
    /// again once a flush lands. Only the excess that no counted relief
    /// covers is costly, and the shed asks flushes only while the asked
    /// relief is below the excess.
    #[test]
    fn given_relief_states_when_measuring_the_excess_should_leave_only_uncovered_bytes_costly() {
        const BUDGET: usize = 1000;
        const RING: usize = 600;
        const RESIDENT: usize = 800;
        const EXCESS: usize = RING + RESIDENT - BUDGET;
        const WAIT: u64 = 10;
        const ASKED_AT: u64 = 5;
        let budget = Rc::new(RepairRingBudget::new(BUDGET));
        let charge = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));
        charge.update(
            JournalBytes {
                ring: RING,
                resident: RESIDENT,
                commands: 0,
                relief: RESIDENT,
            },
            0,
            false,
        );
        assert_eq!(
            (
                budget.excess(),
                budget.costly_excess(),
                budget.wants_flushes()
            ),
            (Some(EXCESS), None, true)
        );

        charge.set_relief(Relief::Asked(ASKED_AT));
        assert_eq!(
            (budget.costly_excess(), budget.wants_flushes()),
            (None, false)
        );
        assert_eq!(budget.pop_expired_stamp(ASKED_AT + WAIT - 1, WAIT), None);
        assert_eq!(
            budget.pop_expired_stamp(ASKED_AT + WAIT, WAIT),
            Some((charge.ticket(), ASKED_AT))
        );
        charge.expire_relief(ASKED_AT);
        assert_eq!(
            (budget.costly_excess(), budget.usage().relief_excluded_bytes),
            (Some(EXCESS), RESIDENT)
        );

        charge.relief_landed();
        assert_eq!(
            (budget.costly_excess(), budget.wants_flushes()),
            (None, true)
        );
        charge.set_relief(Relief::Asked(ASKED_AT + 1));
        charge.relief_landed();
        charge.set_relief(Relief::Asked(ASKED_AT + 2));
        charge.expire_relief(ASKED_AT + 1);
        assert_eq!(charge.relief(), Relief::Asked(ASKED_AT + 2));

        drop(charge);
        assert_eq!(
            (budget.relief_counted.get(), budget.relief_asked.get()),
            (0, 0)
        );
    }
}
