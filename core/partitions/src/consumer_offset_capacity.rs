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

use iggy_common::ConsumerKind;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Weak};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableOffsetState {
    pub(crate) committed_offset: u64,
    pub(crate) persisted_high_water: Option<u64>,
}

#[derive(Debug, Default)]
pub struct DurableConsumerOffsets {
    consumers: RefCell<HashMap<u32, DurableOffsetState>>,
    groups: RefCell<HashMap<u32, DurableOffsetState>>,
}

impl DurableConsumerOffsets {
    pub(crate) fn get(&self, kind: ConsumerKind, id: u32) -> Option<DurableOffsetState> {
        self.entries(kind).borrow().get(&id).copied()
    }

    pub(crate) fn contains(&self, kind: ConsumerKind, id: u32) -> bool {
        self.entries(kind).borrow().contains_key(&id)
    }

    pub(crate) fn count(&self, kind: ConsumerKind) -> usize {
        self.entries(kind).borrow().len()
    }

    pub(crate) fn covers(&self, kind: ConsumerKind, id: u32, offset: u64) -> bool {
        self.get(kind, id).is_some_and(|state| {
            state.committed_offset >= offset
                && state
                    .persisted_high_water
                    .is_some_and(|persisted| persisted >= offset)
        })
    }

    pub(crate) fn record_explicit(
        &self,
        kind: ConsumerKind,
        id: u32,
        committed_offset: u64,
        persisted_high_water: Option<u64>,
    ) {
        self.entries(kind).borrow_mut().insert(
            id,
            DurableOffsetState {
                committed_offset,
                persisted_high_water,
            },
        );
    }

    pub(crate) fn record_auto_commit(
        &self,
        kind: ConsumerKind,
        id: u32,
        committed_offset: u64,
        persisted_high_water: u64,
    ) {
        let mut entries = self.entries(kind).borrow_mut();
        let state = entries.entry(id).or_insert(DurableOffsetState {
            committed_offset,
            persisted_high_water: None,
        });
        state.committed_offset = state.committed_offset.max(committed_offset);
        state.persisted_high_water = Some(
            state
                .persisted_high_water
                .unwrap_or(0)
                .max(persisted_high_water),
        );
    }

    pub(crate) fn mark_persisted(&self, kind: ConsumerKind, id: u32, high_water: u64) {
        if let Some(state) = self.entries(kind).borrow_mut().get_mut(&id) {
            state.persisted_high_water = Some(high_water);
        }
    }

    pub(crate) fn remove(&self, kind: ConsumerKind, id: u32) -> bool {
        self.entries(kind).borrow_mut().remove(&id).is_some()
    }

    pub(crate) fn clear(&self) {
        self.consumers.borrow_mut().clear();
        self.groups.borrow_mut().clear();
    }

    pub(crate) fn committed_entries(&self, kind: ConsumerKind) -> Vec<(u32, u64)> {
        self.entries(kind)
            .borrow()
            .iter()
            .map(|(id, state)| (*id, state.committed_offset))
            .collect()
    }

    const fn entries(&self, kind: ConsumerKind) -> &RefCell<HashMap<u32, DurableOffsetState>> {
        match kind {
            ConsumerKind::Consumer => &self.consumers,
            ConsumerKind::ConsumerGroup => &self.groups,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerOffsetCapacityError {
    pub kind: ConsumerKind,
    pub occupied: usize,
    pub limit: usize,
    pub first_in_episode: bool,
    pub uncertain: bool,
}

impl From<ConsumerOffsetCapacityError> for iggy_common::IggyError {
    fn from(error: ConsumerOffsetCapacityError) -> Self {
        if error.uncertain {
            Self::TransientNotAccepted
        } else {
            Self::TooManyConsumerOffsets
        }
    }
}

#[derive(Debug)]
pub struct ConsumerOffsetCapacity {
    kind: ConsumerKind,
    limit: Cell<usize>,
    pending: RefCell<HashMap<u32, usize>>,
    provisional: RefCell<HashMap<u32, Weak<()>>>,
    stranded: RefCell<HashSet<u32>>,
    uncertain: Cell<bool>,
    durable_warned: Cell<bool>,
    map_warned: Cell<bool>,
}

impl ConsumerOffsetCapacity {
    pub(crate) fn new(kind: ConsumerKind, limit: usize) -> Self {
        Self {
            kind,
            limit: Cell::new(limit),
            pending: RefCell::new(HashMap::new()),
            provisional: RefCell::new(HashMap::new()),
            stranded: RefCell::new(HashSet::new()),
            uncertain: Cell::new(false),
            durable_warned: Cell::new(false),
            map_warned: Cell::new(false),
        }
    }

    pub(crate) fn set_limit(&self, limit: usize) {
        self.limit.set(limit);
    }

    pub(crate) const fn limit(&self) -> usize {
        self.limit.get()
    }

    pub(crate) fn try_reserve(
        &self,
        id: u32,
        durable: &DurableConsumerOffsets,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        self.check(id, durable)?;
        *self.pending.borrow_mut().entry(id).or_default() += 1;
        Ok(())
    }

    pub(crate) fn check(
        &self,
        id: u32,
        durable: &DurableConsumerOffsets,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        self.rearm_if_below_limit(durable);
        if durable.contains(self.kind, id)
            || self.pending.borrow().contains_key(&id)
            || self
                .provisional
                .borrow()
                .get(&id)
                .is_some_and(|token| token.strong_count() > 0)
            || self.stranded.borrow().contains(&id)
        {
            return Ok(());
        }
        self.provisional
            .borrow_mut()
            .retain(|_, token| token.strong_count() > 0);
        let occupied = self.occupied(durable);
        let limit = self.limit.get();
        if self.uncertain.get() || occupied >= limit {
            return Err(ConsumerOffsetCapacityError {
                kind: self.kind,
                occupied,
                limit,
                first_in_episode: !self.durable_warned.replace(true),
                uncertain: self.uncertain.get(),
            });
        }
        Ok(())
    }

    pub(crate) fn reserve_provisional(
        self: &Rc<Self>,
        id: u32,
        durable: &Rc<DurableConsumerOffsets>,
    ) -> Result<AutoCommitReservation, ConsumerOffsetCapacityError> {
        self.check(id, durable)?;
        let mut provisional = self.provisional.borrow_mut();
        let token = provisional
            .get(&id)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let token = Arc::new(());
                provisional.insert(id, Arc::downgrade(&token));
                token
            });
        Ok(AutoCommitReservation {
            token,
            kind: self.kind,
            consumer_id: id,
        })
    }

    pub(crate) fn owns(&self, reservation: &AutoCommitReservation) -> bool {
        reservation.kind == self.kind
            && self
                .provisional
                .borrow()
                .get(&reservation.consumer_id)
                .is_some_and(|token| std::ptr::eq(token.as_ptr(), Arc::as_ptr(&reservation.token)))
    }

    pub(crate) fn protects(&self, id: u32, durable: &DurableConsumerOffsets) -> bool {
        durable.contains(self.kind, id)
            || self.pending.borrow().contains_key(&id)
            || self
                .provisional
                .borrow()
                .get(&id)
                .is_some_and(|token| token.strong_count() > 0)
    }

    pub(crate) fn set_pending_count(&self, id: u32, count: usize) {
        if count == 0 {
            self.pending.borrow_mut().remove(&id);
        } else {
            self.pending.borrow_mut().insert(id, count);
        }
    }

    pub(crate) fn release_reservation(&self, id: u32) {
        let mut pending = self.pending.borrow_mut();
        let Some(count) = pending.get_mut(&id) else {
            return;
        };
        if *count == 1 {
            pending.remove(&id);
        } else {
            *count -= 1;
        }
    }

    pub(crate) const fn is_uncertain(&self) -> bool {
        self.uncertain.get()
    }

    pub(crate) fn rebuild(
        &self,
        durable: &DurableConsumerOffsets,
        pending_ids: impl IntoIterator<Item = u32>,
    ) {
        let mut pending = self.pending.borrow_mut();
        pending.clear();
        for id in pending_ids {
            *pending.entry(id).or_default() += 1;
        }
        drop(pending);
        self.uncertain.set(false);
        self.rearm_if_below_limit(durable);
    }

    pub(crate) fn mark_uncertain(&self) {
        self.pending.borrow_mut().clear();
        self.uncertain.set(true);
    }

    pub(crate) fn record_stranded(&self, id: u32) {
        self.stranded.borrow_mut().insert(id);
    }

    pub(crate) fn clear_stranded(&self, id: u32) {
        self.stranded.borrow_mut().remove(&id);
    }

    pub(crate) fn stranded_ids(&self) -> Vec<u32> {
        self.stranded.borrow().iter().copied().collect()
    }

    pub(crate) fn is_stranded(&self, id: u32) -> bool {
        self.stranded.borrow().contains(&id)
    }

    pub(crate) fn rearm_if_below_limit(&self, durable: &DurableConsumerOffsets) {
        if !self.durable_warned.get() || self.uncertain.get() {
            return;
        }
        if self.occupied(durable) < self.limit.get() {
            self.durable_warned.set(false);
        }
    }

    pub(crate) const fn admit_local_map_key(
        &self,
        map_len: usize,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        let limit = self.limit.get();
        if map_len < limit {
            return Ok(());
        }
        Err(ConsumerOffsetCapacityError {
            kind: self.kind,
            occupied: map_len,
            limit,
            first_in_episode: !self.map_warned.replace(true),
            uncertain: false,
        })
    }

    pub(crate) fn rearm_map_if_below_limit(&self, map_len: usize) {
        if map_len < self.limit.get() {
            self.map_warned.set(false);
        }
    }

    pub(crate) fn occupied(&self, durable: &DurableConsumerOffsets) -> usize {
        let pending = self.pending.borrow();
        let provisional = self.provisional.borrow();
        let stranded = self.stranded.borrow();
        durable.count(self.kind)
            + pending
                .keys()
                .chain(
                    provisional
                        .iter()
                        .filter(|(id, token)| !pending.contains_key(id) && token.strong_count() > 0)
                        .map(|(id, _)| id),
                )
                .chain(stranded.iter().filter(|id| {
                    !pending.contains_key(id)
                        && provisional
                            .get(id)
                            .is_none_or(|token| token.strong_count() == 0)
                }))
                .filter(|id| !durable.contains(self.kind, **id))
                .count()
    }
}

/// Keeps a provisional key occupied until the pump admits or drops its request.
#[derive(Debug)]
pub struct AutoCommitReservation {
    token: Arc<()>,
    pub(crate) kind: ConsumerKind,
    pub(crate) consumer_id: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_provisional_and_journal_reservations_when_rebuilt_and_canceled_should_preserve_journal_slot()
     {
        let durable = Rc::new(DurableConsumerOffsets::default());
        let capacity = Rc::new(ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1));
        let provisional = capacity
            .reserve_provisional(7, &durable)
            .expect("reserve poll");
        capacity.rebuild(&durable, [7]);
        drop(provisional);
        assert!(capacity.check(8, &durable).is_err());
        capacity.set_pending_count(7, 0);
        assert!(capacity.check(8, &durable).is_ok());
    }

    #[test]
    fn given_dropped_submit_when_guard_leaves_scope_should_release_only_its_key() {
        let durable = Rc::new(DurableConsumerOffsets::default());
        let capacity = Rc::new(ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2));
        let first = capacity
            .reserve_provisional(7, &durable)
            .expect("reserve first poll");
        let second = capacity
            .reserve_provisional(8, &durable)
            .expect("reserve second poll");
        assert!(capacity.check(9, &durable).is_err());
        drop(first);
        assert!(capacity.check(9, &durable).is_ok());
        assert_eq!(capacity.occupied(&durable), 1);
        drop(second);
        assert_eq!(capacity.occupied(&durable), 0);
    }

    #[test]
    fn given_full_durable_set_when_reserving_new_key_should_reject() {
        let durable = DurableConsumerOffsets::default();
        durable.record_explicit(ConsumerKind::Consumer, 1, 0, Some(0));
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
        let error = capacity
            .try_reserve(2, &durable)
            .expect_err("new key must be rejected");
        assert_eq!(error.occupied, 1);
        assert_eq!(error.limit, 1);
        assert!(error.first_in_episode);
    }

    #[test]
    fn given_same_pending_key_when_reserved_twice_should_consume_one_slot() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
        assert_eq!(capacity.try_reserve(7, &durable), Ok(()));
        assert_eq!(capacity.try_reserve(7, &durable), Ok(()));
        assert!(capacity.try_reserve(8, &durable).is_err());
        capacity.release_reservation(7);
        assert!(
            capacity.try_reserve(8, &durable).is_err(),
            "one of two reservations still owns the slot"
        );
        capacity.release_reservation(7);
        assert!(
            capacity.try_reserve(8, &durable).is_ok(),
            "the slot is released after the last reservation"
        );
    }

    #[test]
    fn given_stranded_file_when_reserving_same_and_different_ids_should_only_reuse_exact_path() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
        capacity.record_stranded(7);
        assert!(capacity.try_reserve(8, &durable).is_err());
        assert!(
            capacity.try_reserve(7, &durable).is_ok(),
            "rewriting the same path does not allocate another file"
        );
    }

    #[test]
    fn given_uncertain_rebuild_when_reserving_new_key_should_fail_closed() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 4);
        capacity.mark_uncertain();
        let error = capacity
            .try_reserve(7, &durable)
            .expect_err("unknown pending state must block new keys");
        assert_eq!(error.occupied, 0);
        assert!(error.uncertain);
        assert!(error.first_in_episode);
        assert!(
            !capacity
                .try_reserve(8, &durable)
                .expect_err("the same uncertain episode stays closed")
                .first_in_episode
        );
    }

    #[test]
    fn given_capacity_episode_when_occupancy_drops_should_rearm_first_warning() {
        let durable = DurableConsumerOffsets::default();
        durable.record_explicit(ConsumerKind::Consumer, 1, 0, Some(0));
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
        assert!(
            capacity
                .try_reserve(2, &durable)
                .expect_err("full table")
                .first_in_episode
        );
        assert!(
            !capacity
                .try_reserve(2, &durable)
                .expect_err("same full table")
                .first_in_episode
        );
        durable.remove(ConsumerKind::Consumer, 1);
        capacity.rearm_if_below_limit(&durable);
        durable.record_explicit(ConsumerKind::Consumer, 3, 0, Some(0));
        assert!(
            capacity
                .try_reserve(4, &durable)
                .expect_err("new full episode")
                .first_in_episode
        );
    }

    #[test]
    fn given_missing_file_state_when_checking_coverage_should_preserve_membership() {
        let durable = DurableConsumerOffsets::default();
        durable.record_explicit(ConsumerKind::Consumer, 3, 11, None);
        assert!(durable.contains(ConsumerKind::Consumer, 3));
        assert_eq!(durable.count(ConsumerKind::Consumer), 1);
        assert_eq!(
            durable.get(ConsumerKind::Consumer, 3),
            Some(DurableOffsetState {
                committed_offset: 11,
                persisted_high_water: None,
            })
        );
    }
}
