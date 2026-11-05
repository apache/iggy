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

/// Byte budget for the repair rings of the partitions on one shard.
///
/// A ring charges the prepares it holds in memory, plus one `EvictedHeader`
/// for each prepare whose body it released to a segment. The per-partition
/// caps bound one ring, and this budget bounds their sum. Past it, the shard
/// first releases the oldest bodies across its rings, then drops the oldest
/// entries (`IggyPartitions::shed_repair_rings`). Every ring keeps its newest
/// entry, so a written partition always charges at least one prepare.
///
/// Per shard, because a shard cannot evict the ring of another shard.
#[derive(Debug)]
pub struct RepairRingBudget {
    bytes_max: u64,
    used: Cell<u64>,
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

impl RepairRingBudget {
    #[must_use]
    pub const fn new(bytes_max: u64) -> Self {
        Self {
            bytes_max,
            used: Cell::new(0),
            charges: Cell::new(0),
            next_charge: Cell::new(0),
            grown: RefCell::new(VecDeque::new()),
            released: RefCell::new(VecDeque::new()),
        }
    }

    /// Bytes past the budget, or `None` within it.
    pub(crate) fn excess(&self) -> Option<u64> {
        self.used
            .get()
            .checked_sub(self.bytes_max)
            .filter(|excess| *excess > 0)
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
}

/// Names one [`RingCharge`] in a budget queue. A ticket outlives its charge
/// when the partition leaves the shard or takes a new charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingTicket {
    pub(crate) namespace: IggyNamespace,
    charge: u64,
}

/// One ring's share of a [`RepairRingBudget`], returned on drop. The ring is
/// in a budget queue exactly while its flag for that queue is set.
#[derive(Debug)]
pub struct RingCharge {
    budget: Rc<RepairRingBudget>,
    ticket: RingTicket,
    bytes: Cell<u64>,
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
            in_grown: Cell::new(false),
            in_released: Cell::new(false),
        }
    }

    pub(crate) const fn ticket(&self) -> RingTicket {
        self.ticket
    }

    /// Moves this ring's charge to `bytes`. A ring that took new entries
    /// waits for the shard to release its bodies.
    pub(crate) fn update(&self, bytes: u64, grew: bool) {
        let previous = self.bytes.replace(bytes);
        let used = &self.budget.used;
        used.set(used.get().saturating_sub(previous) + bytes);
        if grew && !self.in_grown.replace(true) {
            self.budget.grown.borrow_mut().push_back(self.ticket);
        }
    }

    /// The shard released every body it could and takes the ring out of the
    /// grown queue. A ring that still holds more than its newest entry waits
    /// for the shard to drop its oldest entries.
    pub(crate) fn bodies_released(&self, droppable: bool) {
        self.in_grown.set(false);
        if droppable && !self.in_released.replace(true) {
            self.budget.released.borrow_mut().push_back(self.ticket);
        }
    }

    /// The shard dropped every entry but the newest and takes the ring out of
    /// the released queue.
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
        budget.charges.set(budget.charges.get().saturating_sub(1));
    }
}

/// Allocation budget shared by unproven repairs and snapshot repair tails.
///
/// Reservations precede journaling and last until the proof finishes or the
/// retained window is discarded. Normal prepare queues have separate limits.
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
        const RING_BYTES: u64 = 4096;
        let budget = Rc::new(RepairRingBudget::new(u64::MAX));
        let staying = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, 0));
        staying.update(RING_BYTES, true);
        for partition in 1..=LEAVING {
            let leaving = RingCharge::new(Rc::clone(&budget), IggyNamespace::new(1, 1, partition));
            leaving.update(RING_BYTES, true);
            drop(leaving);
            budget.retain_live(|ticket| ticket == staying.ticket());
            let queued = budget.grown.borrow().len();
            assert!(queued <= 2, "{queued} tickets queued for one live charge");
        }
        assert_eq!(budget.used.get(), RING_BYTES);
        assert_eq!(budget.oldest_grown(), Some(staying.ticket()));
    }
}
