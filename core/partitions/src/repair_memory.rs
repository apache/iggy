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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
}
