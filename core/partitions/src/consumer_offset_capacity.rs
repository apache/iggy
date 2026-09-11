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
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use iggy_common::ConsumerKind;
pub use server_common::poll::AutoCommitReservation;
use server_common::poll::AutoCommitReservationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableOffsetState {
    pub(crate) committed_offset: u64,
    pub(crate) persisted_high_water: u64,
}

#[derive(Debug, Default)]
pub struct DurableConsumerOffsets {
    consumers: RefCell<HashMap<u32, DurableOffsetState>>,
    groups: RefCell<HashMap<u32, DurableOffsetState>>,
    membership_epoch: Cell<u64>,
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
            state.committed_offset >= offset && state.persisted_high_water >= offset
        })
    }

    pub(crate) fn record_explicit(
        &self,
        kind: ConsumerKind,
        id: u32,
        committed_offset: u64,
        persisted_high_water: u64,
    ) -> bool {
        let created = self
            .entries(kind)
            .borrow_mut()
            .insert(
                id,
                DurableOffsetState {
                    committed_offset,
                    persisted_high_water,
                },
            )
            .is_none();
        if created {
            self.bump_membership_epoch();
        }
        created
    }

    pub(crate) fn record_auto_commit(
        &self,
        kind: ConsumerKind,
        id: u32,
        committed_offset: u64,
        persisted_high_water: u64,
    ) {
        let mut entries = self.entries(kind).borrow_mut();
        match entries.entry(id) {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.committed_offset = state.committed_offset.max(committed_offset);
                state.persisted_high_water = state.persisted_high_water.max(persisted_high_water);
            }
            Entry::Vacant(entry) => {
                entry.insert(DurableOffsetState {
                    committed_offset,
                    persisted_high_water,
                });
                drop(entries);
                self.bump_membership_epoch();
            }
        }
    }

    pub(crate) fn remove(&self, kind: ConsumerKind, id: u32) -> bool {
        let removed = self.entries(kind).borrow_mut().remove(&id).is_some();
        if removed {
            self.bump_membership_epoch();
        }
        removed
    }

    pub(crate) fn clear(&self) {
        self.consumers.borrow_mut().clear();
        self.groups.borrow_mut().clear();
        self.bump_membership_epoch();
    }

    #[cfg(any(test, feature = "simulator"))]
    pub(crate) fn committed_entries(&self, kind: ConsumerKind) -> Vec<(u32, u64)> {
        self.entries(kind)
            .borrow()
            .iter()
            .map(|(id, state)| (*id, state.committed_offset))
            .collect()
    }

    pub(crate) fn with_entries<T>(
        &self,
        kind: ConsumerKind,
        read: impl FnOnce(&HashMap<u32, DurableOffsetState>) -> T,
    ) -> T {
        read(&self.entries(kind).borrow())
    }

    const fn entries(&self, kind: ConsumerKind) -> &RefCell<HashMap<u32, DurableOffsetState>> {
        match kind {
            ConsumerKind::Consumer => &self.consumers,
            ConsumerKind::ConsumerGroup => &self.groups,
        }
    }

    fn bump_membership_epoch(&self) {
        self.membership_epoch
            .set(self.membership_epoch.get().wrapping_add(1));
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
    /// Tokens cached by consumer key, which may outlive their last request guard.
    provisional: RefCell<HashMap<u32, Rc<AutoCommitReservationToken>>>,
    /// Tokens that still have guards, excluding inactive entries in the cache.
    active_provisional_keys: Rc<Cell<usize>>,
    stranded: RefCell<HashSet<u32>>,
    uncertain: Cell<bool>,
    durable_warned: Cell<bool>,
    map_warned: Cell<bool>,
    reclaim_epoch: Rc<Cell<u64>>,
    last_reclaim: Cell<Option<(u64, u64)>>,
}

impl ConsumerOffsetCapacity {
    pub(crate) fn new(kind: ConsumerKind, limit: usize) -> Self {
        Self {
            kind,
            limit: Cell::new(limit),
            pending: RefCell::new(HashMap::new()),
            provisional: RefCell::new(HashMap::new()),
            active_provisional_keys: Rc::new(Cell::new(0)),
            stranded: RefCell::new(HashSet::new()),
            uncertain: Cell::new(false),
            durable_warned: Cell::new(false),
            map_warned: Cell::new(false),
            reclaim_epoch: Rc::new(Cell::new(0)),
            last_reclaim: Cell::new(None),
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
        if self.holds(id, durable) || self.stranded.borrow().contains(&id) {
            return Ok(());
        }
        let limit = self.limit.get();
        let durable_count = durable.count(self.kind);
        let fixed = durable_count
            .saturating_add(self.pending.borrow().len())
            .saturating_add(self.stranded.borrow().len());
        let provisional_len = self.active_provisional_keys.get();
        let upper_bound = fixed.saturating_add(provisional_len);
        if !self.uncertain.get() && upper_bound < limit {
            self.durable_warned.set(false);
            return Ok(());
        }
        // A full durable table cannot gain room by pruning provisional keys.
        let occupied = if durable_count >= limit {
            durable_count
        } else {
            self.occupied(durable)
        };
        if self.uncertain.get() || occupied >= limit {
            return Err(ConsumerOffsetCapacityError {
                kind: self.kind,
                occupied,
                limit,
                first_in_episode: !self.durable_warned.replace(true),
                uncertain: self.uncertain.get(),
            });
        }
        self.durable_warned.set(false);
        Ok(())
    }

    /// Check capacity and hold a provisional claim for one automatic commit.
    /// Requests sharing a key share occupancy but retain independent guards.
    pub(crate) fn reserve_provisional(
        &self,
        id: u32,
        durable: &DurableConsumerOffsets,
    ) -> Result<AutoCommitReservation, ConsumerOffsetCapacityError> {
        self.check(id, durable)?;
        let mut provisional = self.provisional.borrow_mut();
        if provisional.len() >= self.limit.get() && !provisional.contains_key(&id) {
            provisional.retain(|_, token| token.active_count() > 0);
        }
        let token = provisional.entry(id).or_insert_with(|| {
            Rc::new(AutoCommitReservationToken::new(
                self.kind,
                id,
                Rc::clone(&self.reclaim_epoch),
                Rc::clone(&self.active_provisional_keys),
            ))
        });
        Ok(token.acquire())
    }

    /// Check that the guard belongs to this tracker's current token for its key.
    /// Matching consumer identifiers cannot validate a guard from another tracker.
    pub(crate) fn owns(&self, reservation: &AutoCommitReservation) -> bool {
        reservation.kind() == self.kind
            && self
                .provisional
                .borrow()
                .get(&reservation.consumer_id())
                .is_some_and(|token| token.owns(reservation))
    }

    pub(crate) fn holds(&self, id: u32, durable: &DurableConsumerOffsets) -> bool {
        durable.contains(self.kind, id)
            || self.pending.borrow().contains_key(&id)
            || self
                .provisional
                .borrow()
                .get(&id)
                .is_some_and(|token| token.active_count() > 0)
    }

    /// Assigns the pending count outright while [`Self::release_reservation`]
    /// decrements it. Both take `&self` and neither locks: they are serialized
    /// by their call sites, which all run under the partition's `&mut self` on
    /// its own shard thread.
    pub(crate) fn set_pending_count(&self, id: u32, count: usize) {
        if count == 0 {
            if self.pending.borrow_mut().remove(&id).is_some() {
                self.note_local_key_change();
            }
        } else {
            self.pending.borrow_mut().insert(id, count);
        }
    }

    /// See [`Self::set_pending_count`] for the serialization contract.
    pub(crate) fn release_reservation(&self, id: u32) {
        let mut pending = self.pending.borrow_mut();
        let Some(count) = pending.get_mut(&id) else {
            return;
        };
        if *count == 1 {
            pending.remove(&id);
            self.note_local_key_change();
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
        self.note_local_key_change();
        self.uncertain.set(false);
        self.rearm_if_below_limit(durable);
    }

    pub(crate) fn mark_uncertain(&self) {
        self.pending.borrow_mut().clear();
        self.uncertain.set(true);
        self.note_local_key_change();
    }

    pub(crate) fn record_stranded(&self, id: u32) {
        self.stranded.borrow_mut().insert(id);
    }

    pub(crate) fn clear_stranded(&self, id: u32) {
        self.stranded.borrow_mut().remove(&id);
    }

    pub(crate) fn is_stranded(&self, id: u32) -> bool {
        self.stranded.borrow().contains(&id)
    }

    /// Keys whose file could not be loaded or unlinked. Cleared only by a
    /// later store or delete of the same key, never by `rebuild` or
    /// `mark_uncertain`, so a permanently unwritable file keeps this above
    /// zero. Exported as a gauge so that refusal has a signal.
    pub(crate) fn stranded_count(&self) -> usize {
        self.stranded.borrow().len()
    }

    pub(crate) fn rearm_if_below_limit(&self, durable: &DurableConsumerOffsets) {
        if !self.durable_warned.get()
            || self.uncertain.get()
            || durable.count(self.kind) >= self.limit.get()
        {
            return;
        }
        if self.occupied(durable) < self.limit.get() {
            self.durable_warned.set(false);
        }
    }

    pub(crate) fn admit_local_map_key(
        &self,
        map_len: usize,
        durable_full: bool,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        let limit = self.limit.get();
        if map_len < limit {
            self.map_warned.set(false);
            return Ok(());
        }
        Err(ConsumerOffsetCapacityError {
            kind: self.kind,
            occupied: map_len,
            limit,
            first_in_episode: !self.map_warned.replace(true),
            uncertain: !durable_full,
        })
    }

    pub(crate) fn note_local_key_change(&self) {
        self.reclaim_epoch
            .set(self.reclaim_epoch.get().wrapping_add(1));
    }

    pub(crate) fn forget_inactive_provisional(&self, id: u32) {
        let mut provisional = self.provisional.borrow_mut();
        if provisional
            .get(&id)
            .is_some_and(|token| token.active_count() == 0)
        {
            provisional.remove(&id);
        }
    }

    pub(crate) fn should_reclaim(&self, durable: &DurableConsumerOffsets) -> bool {
        if self.uncertain.get() {
            return false;
        }
        let epoch = (self.reclaim_epoch.get(), durable.membership_epoch.get());
        self.last_reclaim.replace(Some(epoch)) != Some(epoch)
    }

    /// Keys this kind holds: the durable set plus every pending, active
    /// provisional or stranded key the durable set does not already count.
    pub(crate) fn occupied(&self, durable: &DurableConsumerOffsets) -> usize {
        let pending = self.pending.borrow();
        let provisional = self.provisional.borrow();
        let stranded = self.stranded.borrow();
        let mut local: HashSet<u32> = pending.keys().copied().collect();
        local.extend(
            provisional
                .iter()
                .filter(|(_, token)| token.active_count() > 0)
                .map(|(id, _)| *id),
        );
        local.extend(stranded.iter().copied());
        durable.count(self.kind)
            + local
                .into_iter()
                .filter(|id| !durable.contains(self.kind, *id))
                .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_cached_inactive_tokens_when_admitting_should_count_only_active_keys() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
        let first = capacity.reserve_provisional(1, &durable).unwrap();
        let repeated = capacity.reserve_provisional(1, &durable).unwrap();
        assert_eq!(capacity.active_provisional_keys.get(), 1);
        drop(first);
        assert_eq!(capacity.active_provisional_keys.get(), 1);
        drop(repeated);
        assert_eq!(capacity.active_provisional_keys.get(), 0);
        assert_eq!(capacity.provisional.borrow().len(), 1);
        capacity.check(2, &durable).unwrap();
        let second = capacity.reserve_provisional(2, &durable).unwrap();
        assert_eq!(capacity.active_provisional_keys.get(), 1);
        drop(second);
        capacity.check(3, &durable).unwrap();
        assert_eq!(capacity.active_provisional_keys.get(), 0);
    }

    #[test]
    fn given_low_occupancy_when_accounting_is_uncertain_should_reject_new_keys() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 100);
        capacity.try_reserve(1, &durable).unwrap();
        capacity.mark_uncertain();
        assert!(capacity.check(2, &durable).unwrap_err().uncertain);
        capacity.rebuild(&durable, [1]);
        capacity.check(2, &durable).unwrap();
    }

    #[test]
    fn given_overlapping_accounting_sets_when_sum_exceeds_limit_should_use_exact_occupancy() {
        let durable = DurableConsumerOffsets::default();
        durable.record_explicit(ConsumerKind::Consumer, 1, 0, 0);
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
        capacity.try_reserve(1, &durable).unwrap();
        capacity.record_stranded(1);
        capacity.check(2, &durable).unwrap();
    }

    #[test]
    fn given_unchanged_protection_when_reclaim_repeats_should_skip_until_guard_drops() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
        let held = capacity.reserve_provisional(7, &durable).unwrap();
        assert!(capacity.should_reclaim(&durable));
        for _ in 0..100 {
            assert!(!capacity.should_reclaim(&durable));
        }
        drop(held);
        assert!(capacity.should_reclaim(&durable));
        assert!(!capacity.should_reclaim(&durable));
        capacity.note_local_key_change();
        assert!(capacity.should_reclaim(&durable));
    }

    #[test]
    fn given_repeated_reservation_for_same_key_should_reuse_token_allocation() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
        let first = capacity.reserve_provisional(7, &durable).unwrap();
        let token = Rc::clone(capacity.provisional.borrow().get(&7).unwrap());
        drop(first);
        drop(capacity.reserve_provisional(8, &durable).unwrap());
        assert_eq!(capacity.provisional.borrow().len(), 2);
        let second = capacity.reserve_provisional(7, &durable).unwrap();
        assert!(token.owns(&second));
    }

    #[test]
    fn given_inactive_token_cache_at_limit_when_new_key_arrives_should_prune_it() {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
        drop(capacity.reserve_provisional(7, &durable).unwrap());
        drop(capacity.reserve_provisional(8, &durable).unwrap());
        assert_eq!(capacity.provisional.borrow().len(), 2);
        let _third = capacity.reserve_provisional(9, &durable).unwrap();
        assert_eq!(capacity.provisional.borrow().len(), 1);
        assert!(capacity.provisional.borrow().contains_key(&9));
    }

    #[test]
    fn given_local_map_pressure_when_durable_has_room_should_return_transient_error() {
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
        assert!(matches!(
            iggy_common::IggyError::from(capacity.admit_local_map_key(1, false).unwrap_err()),
            iggy_common::IggyError::TransientNotAccepted
        ));
        assert!(matches!(
            iggy_common::IggyError::from(capacity.admit_local_map_key(1, true).unwrap_err()),
            iggy_common::IggyError::TooManyConsumerOffsets
        ));
    }

    #[test]
    fn given_provisional_and_journal_reservations_when_rebuilt_and_canceled_should_preserve_journal_slot()
     {
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 1);
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
        let durable = DurableConsumerOffsets::default();
        let capacity = ConsumerOffsetCapacity::new(ConsumerKind::Consumer, 2);
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
        durable.record_explicit(ConsumerKind::Consumer, 1, 0, 0);
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
        durable.record_explicit(ConsumerKind::Consumer, 1, 0, 0);
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
        durable.record_explicit(ConsumerKind::Consumer, 3, 0, 0);
        assert!(
            capacity
                .try_reserve(4, &durable)
                .expect_err("new full episode")
                .first_in_episode
        );
    }

    #[test]
    fn given_persisted_state_when_checking_coverage_should_preserve_membership() {
        let durable = DurableConsumerOffsets::default();
        durable.record_explicit(ConsumerKind::Consumer, 3, 11, 11);
        assert!(durable.contains(ConsumerKind::Consumer, 3));
        assert_eq!(durable.count(ConsumerKind::Consumer), 1);
        assert_eq!(
            durable.get(ConsumerKind::Consumer, 3),
            Some(DurableOffsetState {
                committed_offset: 11,
                persisted_high_water: 11,
            })
        );
    }
}
