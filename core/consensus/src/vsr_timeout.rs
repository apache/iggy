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

//! A deterministic, tick-based timeout mechanism for VSR consensus.
//! This module provides deterministic timeouts driven by logical ticks rather than
//! wall-clock time, enabling:
//! - Deterministic testing
//! - Predictable behavior in consensus
//! - Per-replica PRNG seeding for jitter
//!
//! Two-phase tick model:
//!
//! 1. Phase 1 (tick): All timeouts are advanced by one tick
//! 2. Phase 2 (check & handle): Check which timeouts fired and invoke handlers

/// A deterministic, tick-based timeout.
///
/// Timeouts count down from an initial duration (`after`) and fire when
/// reaching zero. They support exponential backoff with jitter for retries.
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Timeout {
    pub id: u128,
    after: u64,
    ticks_remaining: u64,
    pub ticking: bool,
    pub attempts: u32,
}

impl Timeout {
    pub fn new(id: u128, after: u64) -> Self {
        Self {
            id,
            after,
            ticks_remaining: 0,
            ticking: false,
            attempts: 0,
        }
    }

    pub fn start(&mut self) {
        self.ticks_remaining = self.after;
        self.ticking = true;
        self.attempts = 0;
    }

    pub fn stop(&mut self) {
        self.ticking = false;
        self.ticks_remaining = 0;
        self.attempts = 0;
    }

    pub fn reset(&mut self) {
        self.ticks_remaining = self.after;
        self.attempts = 0;
    }

    pub fn tick(&mut self) {
        if self.ticking {
            self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        }
    }

    pub fn fired(&self) -> bool {
        self.ticking && self.ticks_remaining == 0
    }

    pub fn backoff(&mut self, prng: &mut Xoshiro256Plus) {
        self.attempts = self.attempts.wrapping_add(1);
        let max_backoff = self.after.saturating_mul(16);
        let shift = self.attempts.min(4);
        let backoff = self.after.saturating_mul(1 << shift).min(max_backoff);
        let jitter = prng.random_range(0..=backoff);
        self.ticks_remaining = backoff.saturating_add(jitter);
    }
}

/// Timeout types in VSR.
#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeoutKind {
    Ping,
    Prepare,
    CommitMessage,
    NormalHeartbeat,
    StartViewChangeMessage,
    ViewChangeStatus,
    DoViewChangeMessage,
    RequestStartViewMessage,
}

/// Manager for all VSR timeouts of a replica.
#[allow(unused)]
#[derive(Debug)]
pub struct TimeoutManager {
    ping: Timeout,
    prepare: Timeout,
    commit_message: Timeout,
    normal_heartbeat: Timeout,
    start_view_change_message: Timeout,
    do_view_change_message: Timeout,
    view_change_status: Timeout,
    request_start_view_message: Timeout,
    prng: Xoshiro256Plus,
}

#[allow(unused)]
impl TimeoutManager {
    // Timeout durations in ticks (10ms per tick). Values are taken from TB.
    // TODO define 10ms per tick in a separate constant.
    const PING_TICKS: u64 = 100;
    const PREPARE_TICKS: u64 = 25;
    const COMMIT_MESSAGE_TICKS: u64 = 50;
    const NORMAL_HEARTBEAT_TICKS: u64 = 500;
    const START_VIEW_CHANGE_MESSAGE_TICKS: u64 = 50;
    const VIEW_CHANGE_STATUS_TICKS: u64 = 500;
    const DO_VIEW_CHANGE_MESSAGE_TICKS: u64 = 50;
    const REQUEST_START_VIEW_MESSAGE_TICKS: u64 = 100;

    pub fn new(replica_id: u128) -> Self {
        Self {
            ping: Timeout::new(replica_id, Self::PING_TICKS),
            prepare: Timeout::new(replica_id, Self::PREPARE_TICKS),
            commit_message: Timeout::new(replica_id, Self::COMMIT_MESSAGE_TICKS),
            normal_heartbeat: Timeout::new(replica_id, Self::NORMAL_HEARTBEAT_TICKS),
            start_view_change_message: Timeout::new(
                replica_id,
                Self::START_VIEW_CHANGE_MESSAGE_TICKS,
            ),
            do_view_change_message: Timeout::new(replica_id, Self::DO_VIEW_CHANGE_MESSAGE_TICKS),
            view_change_status: Timeout::new(replica_id, Self::VIEW_CHANGE_STATUS_TICKS),
            request_start_view_message: Timeout::new(
                replica_id,
                Self::REQUEST_START_VIEW_MESSAGE_TICKS,
            ),
            prng: Xoshiro256Plus::seed_from_u64(replica_id as u64),
        }
    }

    /// Tick all timeouts
    /// This is the first phase of the two-phase tick-based timeout mechanism.
    /// 2nd phase is checking which timeouts have fired and calling the appropriate handlers.
    pub fn tick(&mut self) {
        self.ping.tick();
        self.prepare.tick();
        self.commit_message.tick();
        self.normal_heartbeat.tick();
        self.start_view_change_message.tick();
        self.do_view_change_message.tick();
        self.request_start_view_message.tick();
    }

    pub fn fired(&self, kind: TimeoutKind) -> bool {
        self.get(kind).fired()
    }

    pub fn get(&self, kind: TimeoutKind) -> &Timeout {
        match kind {
            TimeoutKind::Ping => &self.ping,
            TimeoutKind::Prepare => &self.prepare,
            TimeoutKind::CommitMessage => &self.commit_message,
            TimeoutKind::NormalHeartbeat => &self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &self.request_start_view_message,
        }
    }

    pub fn get_mut(&mut self, kind: TimeoutKind) -> &mut Timeout {
        match kind {
            TimeoutKind::Ping => &mut self.ping,
            TimeoutKind::Prepare => &mut self.prepare,
            TimeoutKind::CommitMessage => &mut self.commit_message,
            TimeoutKind::NormalHeartbeat => &mut self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &mut self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &mut self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &mut self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &mut self.request_start_view_message,
        }
    }

    pub fn start(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).start();
    }

    pub fn stop(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).stop();
    }

    pub fn reset(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).reset();
    }

    pub fn backoff(&mut self, kind: TimeoutKind) {
        let timeout = match kind {
            TimeoutKind::Ping => &mut self.ping,
            TimeoutKind::Prepare => &mut self.prepare,
            TimeoutKind::CommitMessage => &mut self.commit_message,
            TimeoutKind::NormalHeartbeat => &mut self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &mut self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &mut self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &mut self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &mut self.request_start_view_message,
        };
        timeout.backoff(&mut self.prng);
    }

    pub fn is_ticking(&self, kind: TimeoutKind) -> bool {
        self.get(kind).ticking
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_lifecycle() {
        let mut timeout = Timeout::new(0, 10);

        assert!(!timeout.ticking);
        assert!(!timeout.fired());

        timeout.start();
        assert!(timeout.ticking);

        for _ in 0..9 {
            timeout.tick();
            assert!(!timeout.fired());
        }

        timeout.tick();
        assert!(timeout.fired());
    }

    #[test]
    fn test_timeout_reset() {
        let mut timeout = Timeout::new(0, 10);
        timeout.start();

        for _ in 0..5 {
            timeout.tick();
        }

        timeout.reset();
        assert!(timeout.ticking);

        for _ in 0..9 {
            timeout.tick();
            assert!(!timeout.fired());
        }

        timeout.tick();
        assert!(timeout.fired());
    }

    #[test]
    fn test_timeout_stop() {
        let mut timeout = Timeout::new(0, 10);
        timeout.start();

        for _ in 0..5 {
            timeout.tick();
        }

        timeout.stop();
        assert!(!timeout.ticking);

        for _ in 0..10 {
            timeout.tick();
        }
        assert!(!timeout.fired());
    }

    #[test]
    fn test_backoff_increases() {
        let mut timeout = Timeout::new(0, 10);
        let mut prng = Xoshiro256Plus::seed_from_u64(42);

        timeout.start();
        let initial = timeout.ticks_remaining;

        timeout.backoff(&mut prng);
        // After backoff, ticks_remaining should be larger than initial with some jitter.
        assert!(timeout.ticks_remaining >= initial);
    }
}
