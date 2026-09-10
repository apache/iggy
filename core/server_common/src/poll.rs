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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Identity of one serviceable message history. It is never serialized.
#[derive(Clone, Debug, Default)]
pub struct PollHistoryId(Arc<()>);

impl PartialEq for PollHistoryId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PollHistoryId {}

/// Lifetime accounting for one provisional consumer key.
#[derive(Debug)]
pub struct AutoCommitReservationToken {
    reclaim_epoch: Arc<AtomicU64>,
    active_keys: Arc<AtomicUsize>,
    active: AtomicUsize,
}

impl AutoCommitReservationToken {
    #[must_use]
    pub fn new(reclaim_epoch: Arc<AtomicU64>, active_keys: Arc<AtomicUsize>) -> Self {
        Self {
            reclaim_epoch,
            active_keys,
            active: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn acquire(
        self: &Arc<Self>,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> AutoCommitReservation {
        if self.active.fetch_add(1, Ordering::Relaxed) == 0 {
            self.active_keys.fetch_add(1, Ordering::Relaxed);
        }
        AutoCommitReservation {
            token: Arc::clone(self),
            kind,
            consumer_id,
        }
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn owns(self: &Arc<Self>, reservation: &AutoCommitReservation) -> bool {
        Arc::ptr_eq(self, &reservation.token)
    }
}

/// Holds a provisional key until its request is admitted or dropped.
#[derive(Debug)]
pub struct AutoCommitReservation {
    token: Arc<AutoCommitReservationToken>,
    kind: ConsumerKind,
    consumer_id: u32,
}

impl AutoCommitReservation {
    #[must_use]
    pub const fn kind(&self) -> ConsumerKind {
        self.kind
    }

    #[must_use]
    pub const fn consumer_id(&self) -> u32 {
        self.consumer_id
    }
}

impl Drop for AutoCommitReservation {
    fn drop(&mut self) {
        if self.token.active.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.token.active_keys.fetch_sub(1, Ordering::Relaxed);
            self.token.reclaim_epoch.fetch_add(1, Ordering::Relaxed);
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
        assert_ne!(history, PollHistoryId::default());
    }

    #[test]
    fn last_reservation_releases_the_key() {
        let epoch = Arc::new(AtomicU64::new(0));
        let keys = Arc::new(AtomicUsize::new(0));
        let token = Arc::new(AutoCommitReservationToken::new(
            Arc::clone(&epoch),
            Arc::clone(&keys),
        ));
        let first = token.acquire(ConsumerKind::Consumer, 7);
        let second = token.acquire(ConsumerKind::Consumer, 7);
        assert_eq!(keys.load(Ordering::Relaxed), 1);
        drop(first);
        assert_eq!(keys.load(Ordering::Relaxed), 1);
        drop(second);
        assert_eq!(keys.load(Ordering::Relaxed), 0);
        assert_eq!(epoch.load(Ordering::Relaxed), 1);
    }
}
