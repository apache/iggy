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

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use iggy_common::ConsumerKind;

/// Identity of one serviceable message history. It is never serialized.
///
/// The allocation identifies the history even if a rebuilt partition reuses
/// its namespace and offsets. Pending reads keep it alive, preventing address
/// reuse while they can still complete. `Arc` also keeps completion frames
/// `Send`, as required by the shard inbox.
/// `Default` creates a fresh identity. Only clones compare equal.
#[derive(Clone, Debug, Default)]
pub struct PollHistoryId(Arc<()>);

impl PartialEq for PollHistoryId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PollHistoryId {}

/// Lifetime accounting for one provisional consumer key.
///
/// Requests for the same key hold separate guards, so dropping one request
/// cannot release capacity still held by another.
///
/// Shared only on the owning shard thread. Acquisition and release update
/// the current counters without yielding or reentering task execution, so
/// `Rc` and `Cell` suffice even when guards outlive an async suspension.
#[derive(Debug)]
pub struct AutoCommitReservationToken {
    kind: ConsumerKind,
    consumer_id: u32,
    /// Shared change stamp advanced on the last guard drop to retry reclamation.
    /// Wrapping is allowed.
    reclaim_epoch: Rc<Cell<u64>>,
    /// Number of tokens with outstanding guards in this capacity tracker.
    active_keys: Rc<Cell<usize>>,
    /// Outstanding guards for this key, excluding cached handles to the token.
    active: Cell<usize>,
}

impl AutoCommitReservationToken {
    /// Create an inactive token using counters shared by one capacity tracker.
    /// Reuse the token for concurrent reservations of the same key. Construction
    /// neither checks the configured limit nor occupies capacity.
    #[must_use]
    pub fn new(
        kind: ConsumerKind,
        consumer_id: u32,
        reclaim_epoch: Rc<Cell<u64>>,
        active_keys: Rc<Cell<usize>>,
    ) -> Self {
        Self {
            kind,
            consumer_id,
            reclaim_epoch,
            active_keys,
            active: Cell::new(0),
        }
    }

    /// Hold this key until the returned guard is dropped.
    /// The first guard increments the shared key count. Capacity admission must
    /// already have succeeded, without yielding between that check and this call.
    #[must_use]
    pub fn acquire(self: &Rc<Self>) -> AutoCommitReservation {
        let active = self.active.get();
        self.active.set(active.wrapping_add(1));
        if active == 0 {
            self.active_keys.set(self.active_keys.get().wrapping_add(1));
        }
        AutoCommitReservation {
            token: Rc::clone(self),
        }
    }

    /// Count outstanding guards, excluding cached handles to this token.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.get()
    }

    /// Test token identity, not just the consumer kind and ID.
    #[must_use]
    pub fn owns(self: &Rc<Self>, reservation: &AutoCommitReservation) -> bool {
        Rc::ptr_eq(self, &reservation.token)
    }
}

/// Guard for provisional capacity while a request waits or enters replication.
/// Dropping the last guard releases the key's provisional occupancy and enables
/// reclamation retries. Durable membership and pending prepare reservations
/// for the same key remain unchanged.
///
/// The owner creates this guard when accepting a poll result, after any disk
/// completion has crossed the inbox. Local request entries or replication
/// continuations retain it. It never enters a shard channel.
#[derive(Debug)]
pub struct AutoCommitReservation {
    token: Rc<AutoCommitReservationToken>,
}

impl AutoCommitReservation {
    #[must_use]
    pub fn kind(&self) -> ConsumerKind {
        self.token.kind
    }

    #[must_use]
    pub fn consumer_id(&self) -> u32 {
        self.token.consumer_id
    }
}

impl Drop for AutoCommitReservation {
    fn drop(&mut self) {
        let active = self.token.active.get();
        self.token.active.set(active.wrapping_sub(1));
        if active == 1 {
            self.token
                .active_keys
                .set(self.token.active_keys.get().wrapping_sub(1));
            self.token
                .reclaim_epoch
                .set(self.token.reclaim_epoch.get().wrapping_add(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histories_match_only_their_own_clones() {
        let history = PollHistoryId::default();
        assert_eq!(history, history.clone());
        let other_history = PollHistoryId::default();
        assert_ne!(history, other_history);
    }

    #[test]
    fn last_reservation_releases_the_key() {
        let consumer_id = 7;
        let reclaim_epoch = Rc::new(Cell::new(0));
        let active_keys = Rc::new(Cell::new(0));
        let token = Rc::new(AutoCommitReservationToken::new(
            ConsumerKind::Consumer,
            consumer_id,
            Rc::clone(&reclaim_epoch),
            Rc::clone(&active_keys),
        ));

        // Two requests for the same consumer occupy one capacity slot.
        let first_reservation = token.acquire();
        let second_reservation = token.acquire();
        assert_eq!(first_reservation.kind(), ConsumerKind::Consumer);
        assert_eq!(first_reservation.consumer_id(), consumer_id);
        assert_eq!(active_keys.get(), 1);

        drop(first_reservation);
        assert_eq!(
            active_keys.get(),
            1,
            "the second request still holds the key"
        );

        drop(second_reservation);
        assert_eq!(active_keys.get(), 0);
        assert_eq!(
            reclaim_epoch.get(),
            1,
            "releasing the key enables reclamation"
        );
    }

    #[test]
    fn last_reservation_wraps_reclaim_epoch() {
        let group_id = 7;
        let reclaim_epoch = Rc::new(Cell::new(u64::MAX));
        let active_keys = Rc::new(Cell::new(0));
        let token = Rc::new(AutoCommitReservationToken::new(
            ConsumerKind::ConsumerGroup,
            group_id,
            Rc::clone(&reclaim_epoch),
            Rc::clone(&active_keys),
        ));
        let reservation = token.acquire();
        assert_eq!(reservation.kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(reservation.consumer_id(), group_id);

        // The guard retains the token until release, even after its cached
        // handle is gone. Advancing reclamation past u64::MAX must wrap.
        drop(token);
        drop(reservation);
        assert_eq!(active_keys.get(), 0);
        assert_eq!(reclaim_epoch.get(), 0);
    }
}
