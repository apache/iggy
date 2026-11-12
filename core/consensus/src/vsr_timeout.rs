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
//! - Jitter that is a pure function of the group, the replica, the attempt and the tick
//!
//! Each timeout stores an absolute deadline on a [`TickClock`]. The driver
//! advances the clock once per tick, then checks which timeouts fired and
//! invokes their handlers. A group that is not visited keeps its deadlines.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use iggy_binary_protocol::HeartbeatState;
use twox_hash::XxHash3_64;

/// Wall-clock length of one consensus tick.
///
/// Every tick count in this module is a multiple of this unit: a timeout of N
/// ticks fires after N * `TICK_INTERVAL`, provided the driver advances the
/// [`TickClock`] at this cadence.
pub const TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Deadline of a stopped timeout. No tick reaches it.
const STOPPED: u64 = u64::MAX;

/// A prepare backoff doubles the period at most this many times, so a
/// retransmit waits at most 16 periods plus jitter.
const PREPARE_BACKOFF_SHIFT_MAX: u32 = 4;

/// Consensus ticks so far. Clones share one count, so every group of one
/// driver reads the same time.
#[derive(Debug, Clone, Default)]
pub struct TickClock(Rc<Cell<u64>>);

impl TickClock {
    #[must_use]
    pub fn now(&self) -> u64 {
        self.0.get()
    }

    /// Only the driver of the groups on this clock calls it, once per tick.
    pub fn advance(&self) {
        self.0.set(self.0.get().wrapping_add(1));
    }
}

/// A timeout of `after` ticks with an absolute deadline on the clock of its
/// [`TimeoutManager`]. It fires once the clock reaches the deadline and stays
/// fired until it is started, reset, backed off or stopped.
#[derive(Debug, Clone)]
pub struct Timeout {
    after: u64,
    deadline: u64,
}

impl Timeout {
    #[must_use]
    pub const fn new(after: u64) -> Self {
        Self {
            after,
            deadline: STOPPED,
        }
    }

    pub fn start(&mut self, now: u64) {
        self.deadline = deadline_after(now, self.after);
    }

    pub const fn stop(&mut self) {
        self.deadline = STOPPED;
    }

    /// Start the timer for `ticks` this one time. The period of later
    /// [`Self::start`] and [`Self::reset`] calls does not change.
    pub fn start_with(&mut self, now: u64, ticks: u64) {
        self.deadline = deadline_after(now, ticks);
    }

    /// Restart a running timer's interval.
    ///
    /// # Panics
    /// In debug builds, if the timer is stopped. A stopped timer stays stopped,
    /// so callers that cannot prove the timer is running want [`Self::start`].
    pub fn reset(&mut self, now: u64) {
        self.rearm(now, self.after);
    }

    #[must_use]
    pub const fn is_ticking(&self) -> bool {
        self.deadline != STOPPED
    }

    #[must_use]
    pub const fn fired(&self, now: u64) -> bool {
        self.deadline <= now
    }

    fn rearm(&mut self, now: u64, ticks: u64) {
        debug_assert!(
            self.is_ticking(),
            "re-arming a stopped timeout leaves it stopped; use start"
        );
        if self.is_ticking() {
            self.deadline = deadline_after(now, ticks);
        }
    }

    /// Keep the ticks left on a clock that reads `old_now` for one that reads
    /// `new_now`. A fired timer stays fired.
    fn rebase(&mut self, old_now: u64, new_now: u64) {
        if self.is_ticking() {
            self.deadline = deadline_after(new_now, self.deadline.saturating_sub(old_now));
        }
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
#[derive(Debug)]
pub struct TimeoutManager {
    clock: TickClock,
    prepare: Timeout,
    commit_message: Timeout,
    check_quorum: Timeout,
    normal_heartbeat: Timeout,
    start_view_change_message: Timeout,
    do_view_change_message: Timeout,
    view_change_status: Timeout,
    request_start_view_message: Timeout,
    /// Prepare backoffs since the prepare timeout last started, reset or
    /// stopped. Only Prepare backs off, so one counter serves.
    prepare_attempts: u32,
}

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
    pub const fn new(clock: TickClock) -> Self {
        Self {
            clock,
            prepare: Timeout::new(Self::PREPARE_TICKS),
            commit_message: Timeout::new(Self::COMMIT_MESSAGE_TICKS),
            check_quorum: Timeout::new(Self::NORMAL_HEARTBEAT_TICKS),
            normal_heartbeat: Timeout::new(Self::NORMAL_HEARTBEAT_TICKS),
            start_view_change_message: Timeout::new(Self::START_VIEW_CHANGE_MESSAGE_TICKS),
            do_view_change_message: Timeout::new(Self::DO_VIEW_CHANGE_MESSAGE_TICKS),
            view_change_status: Timeout::new(Self::VIEW_CHANGE_STATUS_TICKS),
            request_start_view_message: Timeout::new(Self::REQUEST_START_VIEW_MESSAGE_TICKS),
            prepare_attempts: 0,
        }
    }

    /// Override the backup's primary-liveness and primary's quorum window (`[cluster]
    /// heartbeat_timeout`). Replaces the timeout object, so any countdown
    /// already in flight is discarded: call before the replica starts
    /// ticking (i.e. before `init` / `init_as_backup`).
    pub const fn set_normal_heartbeat_ticks(&mut self, ticks: u64) {
        self.normal_heartbeat = Timeout::new(ticks);
        self.check_quorum = Timeout::new(ticks);
    }

    /// Override the primary's commit-broadcast interval (`[cluster]
    /// commit_broadcast_interval`). Replaces the timeout object, so any
    /// countdown already in flight is discarded: call before the replica
    /// starts ticking (i.e. before `init`).
    pub const fn set_commit_message_ticks(&mut self, ticks: u64) {
        self.commit_message = Timeout::new(ticks);
    }

    /// Override the primary's prepare-retransmit interval (`[cluster]
    /// prepare_retransmit_interval`). Replaces the timeout object, so any
    /// countdown already in flight is discarded: call before the replica
    /// starts ticking (i.e. before `init`).
    pub const fn set_prepare_ticks(&mut self, ticks: u64) {
        self.prepare = Timeout::new(ticks);
        self.prepare_attempts = 0;
    }

    /// Override the view-change retransmit interval (`[cluster]
    /// view_change_retransmit_interval`), in consensus ticks. Drives BOTH the
    /// `StartViewChange` and `DoViewChange` retransmit timers - kept equal by
    /// design so a view change retransmits both messages at one cadence.
    /// Replaces the timeout objects, so any countdown already in flight is
    /// discarded: call before the replica starts ticking (i.e. before `init`).
    pub const fn set_view_change_retransmit_ticks(&mut self, ticks: u64) {
        self.start_view_change_message = Timeout::new(ticks);
        self.do_view_change_message = Timeout::new(ticks);
    }

    /// Override the view-change status backstop (`[cluster]
    /// view_change_status_timeout`), in consensus ticks. A view change stalled
    /// this long escalates to a fresh cluster-wide election. Replaces the
    /// timeout object, so any countdown already in flight is discarded: call
    /// before the replica starts ticking (i.e. before `init`).
    pub const fn set_view_change_status_ticks(&mut self, ticks: u64) {
        self.view_change_status = Timeout::new(ticks);
    }

    /// Override the request-start-view retransmit interval (`[cluster]
    /// request_start_view_retransmit_interval`), in consensus ticks: how often
    /// a recovering or view-change backup re-requests the current view's
    /// `StartView`. Replaces the timeout object, so any countdown already in
    /// flight is discarded: call before the replica starts ticking (i.e. before
    /// `init`).
    pub const fn set_request_start_view_ticks(&mut self, ticks: u64) {
        self.request_start_view_message = Timeout::new(ticks);
    }

    #[must_use]
    pub const fn clock(&self) -> &TickClock {
        &self.clock
    }

    /// Move every running timeout onto `clock` with the ticks it has left.
    /// Stopped timeouts stay stopped.
    pub fn install_clock(&mut self, clock: TickClock) {
        let (old_now, new_now) = (self.clock.now(), clock.now());
        for timeout in [
            &mut self.prepare,
            &mut self.commit_message,
            &mut self.check_quorum,
            &mut self.normal_heartbeat,
            &mut self.start_view_change_message,
            &mut self.do_view_change_message,
            &mut self.view_change_status,
            &mut self.request_start_view_message,
        ] {
            timeout.rebase(old_now, new_now);
        }
        self.clock = clock;
    }

    #[must_use]
    pub fn fired(&self, kind: TimeoutKind) -> bool {
        self.get(kind).fired(self.clock.now())
    }

    const fn get(&self, kind: TimeoutKind) -> &Timeout {
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

    /// Private: a caller that starts the prepare timeout through it skips
    /// the reset of `prepare_attempts`.
    const fn get_mut(&mut self, kind: TimeoutKind) -> &mut Timeout {
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

    pub fn start(&mut self, kind: TimeoutKind) {
        let now = self.clock.now();
        self.get_mut(kind).start(now);
        self.clear_backoff(kind);
    }

    pub fn start_with(&mut self, kind: TimeoutKind, ticks: u64) {
        let now = self.clock.now();
        self.get_mut(kind).start_with(now, ticks);
        self.clear_backoff(kind);
    }

    pub const fn stop(&mut self, kind: TimeoutKind) {
        self.get_mut(kind).stop();
        self.clear_backoff(kind);
    }

    /// Restart the interval of the running timer of `kind`.
    ///
    /// # Panics
    /// In debug builds, if that timer is stopped. A stopped timer stays
    /// stopped, so callers that cannot prove it runs want [`Self::start`].
    pub fn reset(&mut self, kind: TimeoutKind) {
        let now = self.clock.now();
        self.get_mut(kind).reset(now);
        self.clear_backoff(kind);
    }

    /// Re-arm the running prepare timeout after the backoff of its next
    /// attempt, plus jitter drawn with `salt` (see `jitter_salt`).
    ///
    /// # Panics
    /// In debug builds, if the prepare timeout is stopped.
    pub fn backoff_prepare(&mut self, salt: u64) {
        self.prepare_attempts = self.prepare_attempts.wrapping_add(1);
        let shift = self.prepare_attempts.min(PREPARE_BACKOFF_SHIFT_MAX);
        let backoff = self.prepare.after.saturating_mul(1 << shift);
        let now = self.clock.now();
        let jitter = jitter(salt, self.prepare_attempts, now, backoff);
        self.prepare.rearm(now, backoff.saturating_add(jitter));
    }

    #[must_use]
    pub const fn is_ticking(&self, kind: TimeoutKind) -> bool {
        self.get(kind).is_ticking()
    }

    const fn clear_backoff(&mut self, kind: TimeoutKind) {
        if matches!(kind, TimeoutKind::Prepare) {
            self.prepare_attempts = 0;
        }
    }
}

/// Salt of the prepare jitter of member `replica` in `group`. The XOR cannot
/// collide where it matters: the replicas of one group differ in it, and so
/// do the groups of one replica.
#[must_use]
pub fn jitter_salt(group: u64, replica: u8) -> u64 {
    group ^ u64::from(replica)
}

/// Jitter in `0..=span`. A pure function of its inputs, so a seeded run
/// replays it, which a per-process hasher would not.
fn jitter(salt: u64, attempt: u32, now: u64, span: u64) -> u64 {
    let input = ((u128::from(attempt) << u64::BITS) | u128::from(now)).to_le_bytes();
    let hash = XxHash3_64::oneshot_with_seed(salt, &input);
    span.checked_add(1).map_or(hash, |range| hash % range)
}

/// Below [`STOPPED`], so a deadline past the end of the clock still runs.
fn deadline_after(now: u64, ticks: u64) -> u64 {
    now.saturating_add(ticks).min(STOPPED - 1)
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

    const AFTER: u64 = 10;
    const GROUP: u64 = 0x0001_0001_0000_0007;
    const REPLICA: u8 = 1;
    /// Far beyond what any test here waits for.
    const WAIT_MAX: u64 = 1 << 16;

    fn manager() -> (TickClock, TimeoutManager) {
        let clock = TickClock::default();
        let mut timeouts = TimeoutManager::new(clock.clone());
        timeouts.set_prepare_ticks(AFTER);
        (clock, timeouts)
    }

    fn advance(clock: &TickClock, ticks: u64) {
        for _ in 0..ticks {
            clock.advance();
        }
    }

    /// Advances `clock` until `kind` fires, and returns how many ticks that took.
    fn ticks_until_fired(clock: &TickClock, timeouts: &TimeoutManager, kind: TimeoutKind) -> u64 {
        let mut waited = 0;
        while !timeouts.fired(kind) {
            assert!(waited < WAIT_MAX, "{kind:?} never fired");
            clock.advance();
            waited += 1;
        }
        waited
    }

    #[test]
    fn given_a_started_timeout_when_the_clock_reaches_its_deadline_should_fire_and_not_before() {
        let (clock, mut timeouts) = manager();
        advance(&clock, 3);
        assert!(!timeouts.is_ticking(TimeoutKind::Prepare));
        assert!(!timeouts.fired(TimeoutKind::Prepare));

        timeouts.start(TimeoutKind::Prepare);
        assert!(timeouts.is_ticking(TimeoutKind::Prepare));
        for _ in 1..AFTER {
            clock.advance();
            assert!(!timeouts.fired(TimeoutKind::Prepare));
        }
        clock.advance();
        assert!(timeouts.fired(TimeoutKind::Prepare));
        clock.advance();
        assert!(
            timeouts.fired(TimeoutKind::Prepare),
            "a fired timeout stays fired until handled"
        );
    }

    #[test]
    fn given_a_running_timeout_when_reset_should_wait_a_full_period_from_the_reset() {
        let (clock, mut timeouts) = manager();
        timeouts.start(TimeoutKind::Prepare);
        advance(&clock, AFTER / 2);

        timeouts.reset(TimeoutKind::Prepare);

        assert_eq!(
            ticks_until_fired(&clock, &timeouts, TimeoutKind::Prepare),
            AFTER
        );
    }

    #[test]
    fn given_a_running_timeout_when_stopped_should_never_fire() {
        let (clock, mut timeouts) = manager();
        timeouts.start(TimeoutKind::Prepare);
        advance(&clock, AFTER / 2);

        timeouts.stop(TimeoutKind::Prepare);
        advance(&clock, 10 * AFTER);

        assert!(!timeouts.is_ticking(TimeoutKind::Prepare));
        assert!(!timeouts.fired(TimeoutKind::Prepare));
    }

    #[test]
    fn given_a_period_past_the_end_of_the_clock_when_started_should_run_and_never_fire() {
        let (clock, mut timeouts) = manager();
        timeouts.set_commit_message_ticks(u64::MAX);
        advance(&clock, 1);

        timeouts.start(TimeoutKind::CommitMessage);
        advance(&clock, AFTER);

        assert!(timeouts.is_ticking(TimeoutKind::CommitMessage));
        assert!(!timeouts.fired(TimeoutKind::CommitMessage));
    }

    #[test]
    fn given_repeated_prepare_backoffs_should_grow_the_wait_until_the_cap() {
        let (clock, mut timeouts) = manager();
        timeouts.start(TimeoutKind::Prepare);
        let salt = jitter_salt(GROUP, REPLICA);
        for attempt in 1..=PREPARE_BACKOFF_SHIFT_MAX + 2 {
            let backoff = AFTER << attempt.min(PREPARE_BACKOFF_SHIFT_MAX);
            timeouts.backoff_prepare(salt);
            let waited = ticks_until_fired(&clock, &timeouts, TimeoutKind::Prepare);
            assert!(
                (backoff..=2 * backoff).contains(&waited),
                "attempt {attempt} waited {waited} ticks, outside {backoff}..={}",
                2 * backoff
            );
        }
    }

    #[test]
    fn given_a_backed_off_prepare_timeout_when_only_it_restarts_should_drop_its_backoff() {
        let (clock, mut timeouts) = manager();
        timeouts.start(TimeoutKind::Prepare);
        let salt = jitter_salt(GROUP, REPLICA);
        timeouts.backoff_prepare(salt);
        timeouts.backoff_prepare(salt);

        timeouts.start(TimeoutKind::CommitMessage);
        timeouts.reset(TimeoutKind::CommitMessage);
        timeouts.stop(TimeoutKind::CommitMessage);
        timeouts.backoff_prepare(salt);
        let third = AFTER << 3;
        let waited = ticks_until_fired(&clock, &timeouts, TimeoutKind::Prepare);
        assert!(
            (third..=2 * third).contains(&waited),
            "another timer cleared the prepare backoff: waited {waited} ticks"
        );

        timeouts.start(TimeoutKind::Prepare);
        assert_eq!(
            ticks_until_fired(&clock, &timeouts, TimeoutKind::Prepare),
            AFTER
        );
    }

    #[test]
    fn given_the_same_inputs_when_drawing_jitter_should_repeat_it_within_its_span() {
        let salt = jitter_salt(GROUP, REPLICA);
        for span in [0, 1, AFTER, u64::MAX] {
            for now in 0..64 {
                let drawn = jitter(salt, 1, now, span);
                assert!(drawn <= span, "{drawn} above {span}");
                assert_eq!(drawn, jitter(salt, 1, now, span));
            }
        }
    }

    #[test]
    fn given_two_groups_or_two_replicas_when_drawing_jitter_should_draw_apart() {
        const SPAN: u64 = 1 << 20;
        let draws = |group: u64, replica: u8| {
            (0..16)
                .map(|now| jitter(jitter_salt(group, replica), 1, now, SPAN))
                .collect::<Vec<_>>()
        };
        assert_ne!(draws(GROUP, REPLICA), draws(GROUP + 1, REPLICA));
        assert_ne!(draws(GROUP, REPLICA), draws(GROUP, REPLICA + 1));
    }

    #[test]
    fn given_timeouts_on_one_clock_when_moved_to_another_should_keep_the_ticks_left() {
        const ELAPSED: u64 = 3;
        let (clock, mut timeouts) = manager();
        timeouts.set_commit_message_ticks(1);
        timeouts.start(TimeoutKind::CommitMessage);
        timeouts.start(TimeoutKind::Prepare);
        timeouts.start(TimeoutKind::NormalHeartbeat);
        timeouts.stop(TimeoutKind::NormalHeartbeat);
        advance(&clock, ELAPSED);
        assert!(timeouts.fired(TimeoutKind::CommitMessage));
        assert!(!timeouts.fired(TimeoutKind::Prepare));

        let ahead = TickClock::default();
        advance(&ahead, WAIT_MAX);
        timeouts.install_clock(ahead.clone());

        assert!(
            timeouts.fired(TimeoutKind::CommitMessage),
            "a fired timeout stays fired"
        );
        assert!(
            !timeouts.is_ticking(TimeoutKind::NormalHeartbeat),
            "a stopped timeout stays stopped"
        );
        advance(&clock, AFTER);
        assert!(
            !timeouts.fired(TimeoutKind::Prepare),
            "the old clock no longer drives the timeouts"
        );
        assert_eq!(
            ticks_until_fired(&ahead, &timeouts, TimeoutKind::Prepare),
            AFTER - ELAPSED
        );
    }
}
