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
use std::time::Duration;

use iggy_binary_protocol::HeartbeatState;

use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;

/// Wall-clock length of one consensus tick.
///
/// Every tick count in this module is a multiple of this unit: a timeout of N
/// ticks fires after N * `TICK_INTERVAL`, provided the owner drives
/// [`TimeoutManager::tick`] at this cadence.
pub const TICK_INTERVAL: Duration = Duration::from_millis(10);

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
    #[must_use]
    pub const fn new(id: u128, after: u64) -> Self {
        Self {
            id,
            after,
            ticks_remaining: 0,
            ticking: false,
            attempts: 0,
        }
    }

    pub const fn start(&mut self) {
        self.ticks_remaining = self.after;
        self.ticking = true;
        self.attempts = 0;
    }

    pub const fn stop(&mut self) {
        self.ticking = false;
        self.ticks_remaining = 0;
        self.attempts = 0;
    }

    /// Start the timer for `ticks` this one time. The period of later
    /// [`Self::start`] and [`Self::reset`] calls does not change.
    pub const fn start_with(&mut self, ticks: u64) {
        self.ticks_remaining = ticks;
        self.ticking = true;
        self.attempts = 0;
    }

    /// Restart a running timer's interval, clearing any accumulated backoff.
    ///
    /// # Panics
    /// In debug builds, if the timer is not ticking. `reset` leaves `ticking` alone,
    /// so on a stopped timer it yields an armed-but-dead state that never fires and
    /// reads as healthy. Callers that cannot prove the timer is running want
    /// [`Self::start`], identical to this on a running one.
    pub const fn reset(&mut self) {
        debug_assert!(
            self.ticking,
            "reset on a stopped timeout leaves it armed-but-dead; use start"
        );
        self.ticks_remaining = self.after;
        self.attempts = 0;
    }

    pub const fn tick(&mut self) {
        if self.ticking {
            self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        }
    }

    #[must_use]
    pub const fn fired(&self) -> bool {
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
    Prepare,
    CommitMessage,
    CheckQuorum,
    NormalHeartbeat,
    StartViewChangeMessage,
    ViewChangeStatus,
    DoViewChangeMessage,
    /// Backup re-requesting the current view's `StartView` from its
    /// primary: fired while a recovering replica probes for the view.
    RequestStartViewMessage,
}

/// Manager for all VSR timeouts of a replica.
#[allow(unused)]
#[derive(Debug)]
pub struct TimeoutManager {
    prepare: Timeout,
    commit_message: Timeout,
    check_quorum: Timeout,
    normal_heartbeat: Timeout,
    start_view_change_message: Timeout,
    do_view_change_message: Timeout,
    view_change_status: Timeout,
    request_start_view_message: Timeout,
    prng: Xoshiro256Plus,
}

#[allow(unused)]
impl TimeoutManager {
    // Timeout durations in ticks of [`TICK_INTERVAL`].
    /// Public so the runtime can pin its `[cluster] prepare_retransmit_interval`
    /// config default against this built-in.
    pub const PREPARE_TICKS: u64 = 25;
    /// Public so the runtime can pin its `[cluster] commit_broadcast_interval`
    /// config default against this built-in.
    pub const COMMIT_MESSAGE_TICKS: u64 = 50;
    /// Public so the runtime can pin its config default (`[cluster]
    /// heartbeat_timeout`) against this built-in with a static assert.
    pub const NORMAL_HEARTBEAT_TICKS: u64 = 500;
    /// Public so the runtime can pin its `[cluster]
    /// view_change_retransmit_interval` config default against this built-in.
    /// Deliberately equal to [`Self::DO_VIEW_CHANGE_MESSAGE_TICKS`]: one config
    /// knob drives both retransmit timers.
    pub const START_VIEW_CHANGE_MESSAGE_TICKS: u64 = 50;
    /// Public so the runtime can pin its `[cluster] view_change_status_timeout`
    /// config default against this built-in.
    pub const VIEW_CHANGE_STATUS_TICKS: u64 = 500;
    /// Public so the runtime can pin its `[cluster]
    /// view_change_retransmit_interval` config default against this built-in.
    /// Deliberately equal to [`Self::START_VIEW_CHANGE_MESSAGE_TICKS`]: one
    /// config knob drives both retransmit timers.
    pub const DO_VIEW_CHANGE_MESSAGE_TICKS: u64 = 50;
    /// Public so the runtime can pin its `[cluster]
    /// request_start_view_retransmit_interval` config default against this
    /// built-in.
    pub const REQUEST_START_VIEW_MESSAGE_TICKS: u64 = 100;
    /// Ticks between grid ticks (`[cluster] commit_heartbeat_grid`).
    pub const HEARTBEAT_GRID_TICKS: u64 = 10;
    /// Ticks between the heartbeats of an idle group (`[cluster]
    /// commit_idle_heartbeat_interval`).
    pub const IDLE_HEARTBEAT_TICKS: u64 = 100;
    /// Ticks that the backups of an idle group wait (`[cluster]
    /// commit_idle_heartbeat_timeout`).
    pub const IDLE_HEARTBEAT_TIMEOUT_TICKS: u64 = 1200;

    #[must_use]
    pub fn new(replica_id: u128) -> Self {
        Self {
            prepare: Timeout::new(replica_id, Self::PREPARE_TICKS),
            commit_message: Timeout::new(replica_id, Self::COMMIT_MESSAGE_TICKS),
            check_quorum: Timeout::new(replica_id, Self::NORMAL_HEARTBEAT_TICKS),
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
            // Upper bits of replica_id are not critical for PRNG seed diversity.
            #[allow(clippy::cast_possible_truncation)]
            prng: Xoshiro256Plus::seed_from_u64(replica_id as u64),
        }
    }

    /// Override the backup's primary-liveness and primary's quorum window (`[cluster]
    /// heartbeat_timeout`). Replaces the timeout object, so any countdown
    /// already in flight is discarded: call before the replica starts
    /// ticking (i.e. before `init` / `init_as_backup`).
    pub const fn set_normal_heartbeat_ticks(&mut self, ticks: u64) {
        self.normal_heartbeat = Timeout::new(self.normal_heartbeat.id, ticks);
        self.check_quorum = Timeout::new(self.check_quorum.id, ticks);
    }

    /// Override the primary's commit-broadcast interval (`[cluster]
    /// commit_broadcast_interval`). Replaces the timeout object, so any
    /// countdown already in flight is discarded: call before the replica
    /// starts ticking (i.e. before `init`).
    pub const fn set_commit_message_ticks(&mut self, ticks: u64) {
        self.commit_message = Timeout::new(self.commit_message.id, ticks);
    }

    /// Override the primary's prepare-retransmit interval (`[cluster]
    /// prepare_retransmit_interval`). Replaces the timeout object, so any
    /// countdown already in flight is discarded: call before the replica
    /// starts ticking (i.e. before `init`).
    pub const fn set_prepare_ticks(&mut self, ticks: u64) {
        self.prepare = Timeout::new(self.prepare.id, ticks);
    }

    /// Override the view-change retransmit interval (`[cluster]
    /// view_change_retransmit_interval`), in consensus ticks. Drives BOTH the
    /// `StartViewChange` and `DoViewChange` retransmit timers - kept equal by
    /// design so a view change retransmits both messages at one cadence.
    /// Replaces the timeout objects, so any countdown already in flight is
    /// discarded: call before the replica starts ticking (i.e. before `init`).
    pub const fn set_view_change_retransmit_ticks(&mut self, ticks: u64) {
        self.start_view_change_message = Timeout::new(self.start_view_change_message.id, ticks);
        self.do_view_change_message = Timeout::new(self.do_view_change_message.id, ticks);
    }

    /// Override the view-change status backstop (`[cluster]
    /// view_change_status_timeout`), in consensus ticks. A view change stalled
    /// this long escalates to a fresh cluster-wide election. Replaces the
    /// timeout object, so any countdown already in flight is discarded: call
    /// before the replica starts ticking (i.e. before `init`).
    pub const fn set_view_change_status_ticks(&mut self, ticks: u64) {
        self.view_change_status = Timeout::new(self.view_change_status.id, ticks);
    }

    /// Override the request-start-view retransmit interval (`[cluster]
    /// request_start_view_retransmit_interval`), in consensus ticks: how often
    /// a recovering or view-change backup re-requests the current view's
    /// `StartView`. Replaces the timeout object, so any countdown already in
    /// flight is discarded: call before the replica starts ticking (i.e. before
    /// `init`).
    pub const fn set_request_start_view_ticks(&mut self, ticks: u64) {
        self.request_start_view_message = Timeout::new(self.request_start_view_message.id, ticks);
    }

    /// Tick all timeouts
    /// This is the first phase of the two-phase tick-based timeout mechanism.
    /// 2nd phase is checking which timeouts have fired and calling the appropriate handlers.
    pub const fn tick(&mut self) {
        self.prepare.tick();
        self.commit_message.tick();
        self.check_quorum.tick();
        self.normal_heartbeat.tick();
        self.start_view_change_message.tick();
        self.do_view_change_message.tick();
        self.request_start_view_message.tick();
        self.view_change_status.tick();
    }

    #[must_use]
    pub const fn fired(&self, kind: TimeoutKind) -> bool {
        self.get(kind).fired()
    }

    #[must_use]
    pub const fn get(&self, kind: TimeoutKind) -> &Timeout {
        match kind {
            TimeoutKind::Prepare => &self.prepare,
            TimeoutKind::CommitMessage => &self.commit_message,
            TimeoutKind::CheckQuorum => &self.check_quorum,
            TimeoutKind::NormalHeartbeat => &self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &self.request_start_view_message,
        }
    }

    pub const fn get_mut(&mut self, kind: TimeoutKind) -> &mut Timeout {
        match kind {
            TimeoutKind::Prepare => &mut self.prepare,
            TimeoutKind::CommitMessage => &mut self.commit_message,
            TimeoutKind::CheckQuorum => &mut self.check_quorum,
            TimeoutKind::NormalHeartbeat => &mut self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &mut self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &mut self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &mut self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &mut self.request_start_view_message,
        }
    }

    pub const fn start(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).start();
    }

    pub const fn start_with(&mut self, kind: TimeoutKind, ticks: u64) {
        self.get_mut(kind).start_with(ticks);
    }

    pub const fn stop(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).stop();
    }

    pub const fn reset(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).reset();
    }

    pub fn backoff(&mut self, kind: TimeoutKind) {
        let timeout = match kind {
            TimeoutKind::Prepare => &mut self.prepare,
            TimeoutKind::CommitMessage => &mut self.commit_message,
            TimeoutKind::CheckQuorum => &mut self.check_quorum,
            TimeoutKind::NormalHeartbeat => &mut self.normal_heartbeat,
            TimeoutKind::StartViewChangeMessage => &mut self.start_view_change_message,
            TimeoutKind::ViewChangeStatus => &mut self.view_change_status,
            TimeoutKind::DoViewChangeMessage => &mut self.do_view_change_message,
            TimeoutKind::RequestStartViewMessage => &mut self.request_start_view_message,
        };
        timeout.backoff(&mut self.prng);
    }

    #[must_use]
    pub const fn is_ticking(&self, kind: TimeoutKind) -> bool {
        self.get(kind).ticking
    }
}

/// The Commit heartbeat schedule of a group, in shard ticks.
///
/// The fields come from the five `[cluster] commit_*heartbeat*` options.
/// Boot makes sure that they follow the schedule rules of the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatSchedule {
    /// Ticks between grid ticks. Heartbeats leave only on grid ticks.
    pub grid_ticks: u64,
    /// Ticks between the heartbeats of a busy group.
    pub busy_interval_ticks: u64,
    /// Ticks that the backups of a busy group wait. It is also the quiet time
    /// before a group turns idle, and the quorum window of a busy group.
    pub busy_timeout_ticks: u64,
    /// Ticks between the heartbeats of an idle group.
    pub idle_interval_ticks: u64,
    /// Ticks that the backups of an idle group wait, at least the busy wait.
    pub idle_timeout_ticks: u64,
}

impl Default for HeartbeatSchedule {
    fn default() -> Self {
        Self {
            grid_ticks: TimeoutManager::HEARTBEAT_GRID_TICKS,
            busy_interval_ticks: TimeoutManager::COMMIT_MESSAGE_TICKS,
            busy_timeout_ticks: TimeoutManager::NORMAL_HEARTBEAT_TICKS,
            idle_interval_ticks: TimeoutManager::IDLE_HEARTBEAT_TICKS,
            idle_timeout_ticks: TimeoutManager::IDLE_HEARTBEAT_TIMEOUT_TICKS,
        }
    }
}

impl HeartbeatSchedule {
    /// Whether shard tick `shard_tick` is a grid tick.
    #[must_use]
    pub const fn is_grid_tick(self, shard_tick: u64) -> bool {
        shard_tick.is_multiple_of(if self.grid_ticks == 0 {
            1
        } else {
            self.grid_ticks
        })
    }

    /// Ticks between the heartbeats of a group in this state.
    #[must_use]
    pub const fn heartbeat_interval_ticks(self, idle: bool) -> u64 {
        if idle {
            self.idle_interval_ticks
        } else {
            self.busy_interval_ticks
        }
    }

    /// Ticks that a backup waits for the next Commit after a Commit in `state`.
    #[must_use]
    pub const fn backup_wait_ticks(self, state: HeartbeatState) -> u64 {
        match state {
            HeartbeatState::Idle => {
                if self.idle_timeout_ticks > self.busy_timeout_ticks {
                    self.idle_timeout_ticks
                } else {
                    self.busy_timeout_ticks
                }
            }
            HeartbeatState::Busy | HeartbeatState::Eager => self.busy_timeout_ticks,
        }
    }

    /// Ticks of the quorum window of a primary. The idle window never uses the
    /// idle timeout, because a primary cut off from its backups serves reads
    /// for up to one window.
    #[must_use]
    pub const fn quorum_window_ticks(self, idle: bool) -> u64 {
        let idle_floor = self
            .idle_interval_ticks
            .saturating_add(self.busy_interval_ticks);
        if idle && idle_floor > self.busy_timeout_ticks {
            idle_floor
        } else {
            self.busy_timeout_ticks
        }
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
