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

/// Byte budget for the repair rings of the partitions on one shard.
///
/// A ring charges the heap it holds: the allocation of each prepare in
/// memory, and the slots of its `EvictedHeader` deque, which keeps one header
/// for each prepare whose body it released to a segment. The per-partition
/// caps bound one ring, and this budget bounds their sum. Past it, the shard
/// first releases the oldest bodies of the rings that grew earliest, then
/// drops their oldest entries (`IggyPartitions::shed_repair_rings`).
///
/// Every ring keeps its newest entry, so these entries are a floor that no
/// shed takes, and the shed works against what the budget leaves above it. A
/// floor above the budget means the budget is too small for the partitions
/// on the shard: every ring is then down to its newest entry.
///
/// Each shard owns one, because a shard cannot shed the ring of another
/// shard. [`RepairMemoryBudget`] is the sibling that the whole node shares.
#[derive(Debug)]
pub struct RepairRingBudget {
    bytes_max: usize,
    used: Cell<usize>,
    /// The part of `used` that the newest entry of every ring holds.
    floor: Cell<usize>,
    floor_over: Cell<bool>,
    /// Live charges. A queue holds at most one ticket per live charge, so a
    /// queue longer than twice this count is at least half stale.
    charges: Cell<usize>,
    next_charge: Cell<u64>,
    /// Rings that took new entries since the shard last released their
    /// bodies, in the order they grew.
    grown: RefCell<VecDeque<RingTicket>>,
    /// Rings with no body left to release but more than their newest entry,
    /// in the order the shard released their bodies.
    released: RefCell<VecDeque<RingTicket>>,
}

/// Where a [`RepairRingBudget`] stands, for the shard's gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRingUsage {
    pub budget_bytes: usize,
    pub charged_bytes: usize,
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
    pub usage: RepairRingUsage,
}

impl RepairRingBudget {
    #[must_use]
    pub const fn new(bytes_max: usize) -> Self {
        Self {
            bytes_max,
            used: Cell::new(0),
            floor: Cell::new(0),
            floor_over: Cell::new(false),
            charges: Cell::new(0),
            next_charge: Cell::new(0),
            grown: RefCell::new(VecDeque::new()),
            released: RefCell::new(VecDeque::new()),
        }
    }

    #[must_use]
    pub const fn usage(&self) -> RepairRingUsage {
        RepairRingUsage {
            budget_bytes: self.bytes_max,
            charged_bytes: self.used.get(),
            floor_bytes: self.floor.get(),
        }
    }

    /// Bytes past the budget that a shed can take, or `None` within it.
    pub(crate) fn excess(&self) -> Option<usize> {
        self.excess_over(self.sheddable_max())
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

    /// Drops the tickets that `live` rejects once a queue is twice as long as
    /// the live charges. A partition that leaves the shard leaves its tickets
    /// behind, and a shard within its budget never visits them, so without
    /// this its queues grow with every partition it ever held.
    pub(crate) fn retain_live(&self, mut live: impl FnMut(RingTicket) -> bool) {
        let limit = 2 * self.charges.get();
        for queue in [&self.grown, &self.released] {
            let mut queue = queue.borrow_mut();
            if queue.len() > limit {
                queue.retain(|ticket| live(*ticket));
            }
        }
    }

    /// What the budget leaves for the entries a shed can take.
    const fn sheddable_max(&self) -> usize {
        self.bytes_max.saturating_sub(self.floor.get())
    }

    fn excess_over(&self, sheddable_max: usize) -> Option<usize> {
        self.used
            .get()
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
    bytes: Cell<usize>,
    floor: Cell<usize>,
    in_grown: Cell<bool>,
    in_released: Cell<bool>,
}

impl RingCharge {
    pub(crate) fn new(budget: Rc<RepairRingBudget>, namespace: IggyNamespace) -> Self {
        let charge = budget.next_charge.get();
        budget.next_charge.set(charge + 1);
        budget.charges.set(budget.charges.get() + 1);
        Self {
            budget,
            ticket: RingTicket { namespace, charge },
            bytes: Cell::new(0),
            floor: Cell::new(0),
            in_grown: Cell::new(false),
            in_released: Cell::new(false),
        }
    }

    pub(crate) const fn ticket(&self) -> RingTicket {
        self.ticket
    }

    /// The `(bytes, floor)` of the last [`Self::update`].
    pub(crate) const fn charged(&self) -> (usize, usize) {
        (self.bytes.get(), self.floor.get())
    }

    /// Moves this ring's charge to `bytes`, of which its newest entry holds
    /// `floor`. A ring that took new entries waits for the shard to release
    /// its bodies.
    pub(crate) fn update(&self, bytes: usize, floor: usize, grew: bool) {
        let budget = &self.budget;
        let previous = self.bytes.replace(bytes);
        budget
            .used
            .set(budget.used.get().saturating_sub(previous) + bytes);
        let previous = self.floor.replace(floor);
        budget
            .floor
            .set(budget.floor.get().saturating_sub(previous) + floor);
        if grew {
            self.queue_grown();
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
        budget
            .used
            .set(budget.used.get().saturating_sub(self.bytes.get()));
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
        staying.update(RING_BYTES, NEWEST_BYTES, true);
        for partition in 1..=LEAVING {
            let leaving = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, partition));
            leaving.update(RING_BYTES, NEWEST_BYTES, true);
            drop(leaving);
            budget.retain_live(|ticket| ticket == staying.ticket());
            let queued = budget.grown.borrow().len();
            assert!(queued <= 2, "{queued} tickets queued for one live charge");
        }
        assert_eq!(
            budget.usage(),
            RepairRingUsage {
                budget_bytes: usize::MAX,
                charged_bytes: RING_BYTES,
                floor_bytes: NEWEST_BYTES,
            }
        );
        assert_eq!(budget.oldest_grown(), Some(staying.ticket()));
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
        let ring = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));

        ring.update(BUDGET, FLOOR, true);
        assert_eq!(budget.excess(), None);
        assert_eq!(budget.excess_over_low_watermark(), Some(SHEDDABLE_MAX / 8));

        ring.update(BUDGET + 1, FLOOR, true);
        assert_eq!(budget.excess(), Some(1));
        assert_eq!(
            budget.excess_over_low_watermark(),
            Some(SHEDDABLE_MAX / 8 + 1)
        );

        // A floor past the budget leaves nothing for other entries.
        ring.update(BUDGET + FLOOR, BUDGET + 1, true);
        assert_eq!(budget.excess(), Some(FLOOR - 1));
        assert_eq!(budget.excess_over_low_watermark(), Some(FLOOR - 1));
    }

    /// The floor reports once when it passes the budget, and once when it is
    /// back under the low watermark, not on every move around the budget.
    #[test]
    fn given_a_floor_around_the_budget_when_it_moves_should_report_each_crossing_once() {
        const BUDGET: usize = 8000;
        let budget = Rc::new(RepairRingBudget::new(BUDGET));
        let ring = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));

        ring.update(BUDGET, BUDGET, false);
        assert_eq!(budget.floor_crossed(), None);
        ring.update(BUDGET + 1, BUDGET + 1, false);
        assert_eq!(budget.floor_crossed(), Some(true));
        assert_eq!(budget.floor_crossed(), None);
        ring.update(BUDGET, BUDGET, false);
        assert_eq!(budget.floor_crossed(), None);
        ring.update(BUDGET - BUDGET / 8, BUDGET - BUDGET / 8, false);
        assert_eq!(budget.floor_crossed(), Some(false));
        assert_eq!(budget.floor_crossed(), None);
    }
}
