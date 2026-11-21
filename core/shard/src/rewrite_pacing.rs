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

//! When a shard rewrites the placed batches of each of its partitions into
//! a long-term segment.
//!
//! Another cluster reads a message only once a long-term segment holds it,
//! so a short wait between two rewrites keeps that cluster close. Every
//! rewrite stores one segment, so a short wait at a low load also leaves
//! many small objects. Each partition therefore has its own wait, between
//! [`REWRITE_INTERVAL_MIN`] and the ceiling of its shard,
//! `[geo] long_term_rewrite_max_interval`, [`REWRITE_INTERVAL_MAX`] by
//! default. After every rewrite it moves toward the wait at which a segment
//! comes out [`REWRITE_FILL_TARGET_PERMILLE`] full: quickly when that wait
//! is shorter, slowly when it is longer.
//!
//! A segment that ended at its byte cap left committed messages behind.
//! The pass for them starts when the segment's command commits, since a
//! rewrite continues where the long-term log ends. A rewrite stores its
//! segment only once the log holds its registration, so the pass that sent
//! one is followed by the next when it commits too.
//!
//! Nothing ties the sends to a partition to its rewrites, so producers that
//! outrun them grow its backlog: the bytes it committed that no long-term
//! segment holds, and the fresh objects that hold them. Past a limit, the
//! shard delays each send to that partition by [`backlog_delay`], and its
//! next pass starts at once.
//!
//! Every decision takes the time it is made at, so nothing here reads a
//! clock.

use partitions::{PlannedSegment, RewriteError};
use server_common::sharding::IggyNamespace;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

/// The shortest wait between two rewrites of one partition.
///
/// It is the shortest time between two stores of the partition's manifest,
/// through which another cluster finds each segment. A segment cut at its
/// byte cap does not wait for it.
pub const REWRITE_INTERVAL_MIN: Duration = partitions::MANIFEST_STORE_INTERVAL;

/// The longest wait between two rewrites of one partition when the shard was
/// given no other, and so how far behind its committed messages the
/// long-term segments of a partition with little load are.
pub const REWRITE_INTERVAL_MAX: Duration = Duration::from_secs(10);

/// The wait before the second rewrite of a partition.
pub const REWRITE_INTERVAL_START: Duration = Duration::from_secs(1);

/// How full a rewrite aims to make a segment, in thousandths of the bytes a
/// segment holds at most.
pub const REWRITE_FILL_TARGET_PERMILLE: u64 = 800;

/// How far the wait moves toward a shorter one after a rewrite, in
/// thousandths of the way.
pub const REWRITE_SPEEDUP_PERMILLE: u64 = 900;

/// How far the wait moves toward a longer one after a rewrite, in
/// thousandths of the way. Less than [`REWRITE_SPEEDUP_PERMILLE`], so one
/// small segment in a burst does not let the long-term log fall behind.
pub const REWRITE_SLOWDOWN_PERMILLE: u64 = 400;

/// The backlog of a partition past which a shard that was given no limit
/// delays the sends to it, in bytes of messages it committed that no
/// long-term segment holds.
pub const LONG_TERM_BACKLOG_LIMIT: u64 = 1024 * 1024 * 1024;

/// How long a send waits just past the backlog limit.
pub const BACKLOG_DELAY_MIN: Duration = Duration::from_millis(10);

/// How long a send waits at most, from twice the backlog limit on. Long
/// enough to slow a producer that waits for each answer, and far below the
/// time after which a client gives a request up.
pub const BACKLOG_DELAY_MAX: Duration = Duration::from_millis(250);

/// How long a send to a partition with `backlog` bytes to rewrite waits
/// before the shard takes it, when sends are delayed past `limit`.
///
/// From [`BACKLOG_DELAY_MIN`] just past the limit, growing with the bytes
/// past it, to [`BACKLOG_DELAY_MAX`] at twice the limit. `None` at or below
/// the limit, and for a limit of zero, which turns the delay off.
#[must_use]
pub fn backlog_delay(backlog: u64, limit: u64) -> Option<Duration> {
    if limit == 0 || backlog <= limit {
        return None;
    }
    let past = u128::from((backlog - limit).min(limit));
    let (min, max) = (BACKLOG_DELAY_MIN.as_micros(), BACKLOG_DELAY_MAX.as_micros());
    let delay = min + (max - min) * past / u128::from(limit);
    Some(Duration::from_micros(
        u64::try_from(delay).unwrap_or(u64::MAX),
    ))
}

/// A partition incarnation: slab ids are reused after a delete, so the
/// namespace alone can name a later partition.
pub type RewriteKey = (IggyNamespace, u64);

/// What a walk of a partition's log counted of its backlog, for the
/// pump's next tick to hand to the partition.
#[derive(Debug)]
pub struct BacklogCount {
    pub key: RewriteKey,
    /// The number the partition gave the walk.
    pub walk: u64,
    /// The bytes, `None` when the walk could not read the log.
    pub bytes: Option<u64>,
}

/// Where a plan of a partition's next segment found it ends, for the
/// pump's next tick to hand to the partition.
#[derive(Debug)]
pub struct FinishedPlan {
    pub key: RewriteKey,
    /// The number the partition gave the plan.
    pub id: u64,
    pub outcome: Result<PlannedSegment, RewriteError>,
}

/// What a rewrite pass over a partition ended as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteResult {
    /// It stored a segment of this many bytes, of the `cap` a segment holds
    /// at most. `reached_commit` is false for a segment that ended before
    /// the commit point its rewrite started at.
    Stored {
        bytes: u64,
        cap: u64,
        reached_commit: bool,
    },
    /// Nothing was there to rewrite, or this replica is not the primary.
    Idle,
    /// The pass only sent a stored segment's command again.
    Resent,
    /// The pass sent the registration of the segment the next rewrite
    /// stores, which starts once it commits.
    Registering,
    /// It failed.
    Unchanged,
}

#[derive(Debug, Clone, Copy)]
struct Pace {
    interval_micros: u64,
    /// The longest wait that the partition's topic sets for itself, `None`
    /// for the shard's.
    max_interval_micros: Option<u64>,
    /// When the next pass is due, in the caller's microseconds. `None`
    /// while a rewrite runs: its result sets it.
    due_at: Option<u64>,
    /// Messages the partition committed that no long-term segment holds, as
    /// its last pass found.
    lag: u64,
    /// Whether the next pass waits for the command of a stored segment that
    /// left committed messages behind.
    awaits_commit: bool,
    /// When the last pass was settled, in the caller's microseconds.
    settled_at: u64,
    /// Whether the last pass failed.
    failed: bool,
}

/// The waits of the partitions of one shard.
#[derive(Debug)]
pub struct RewritePacing {
    partitions: HashMap<RewriteKey, Pace>,
    /// The longest wait, never below [`REWRITE_INTERVAL_MIN`].
    max_interval_micros: u64,
    /// The backlog past which the sends to a partition are delayed, zero
    /// when they are not.
    backlog_limit: u64,
    /// Counts of backlogs that walks handed back since the last tick.
    backlog_counts: Rc<RefCell<Vec<BacklogCount>>>,
    /// Ends of segments that plans found since the last tick.
    plans: Rc<RefCell<Vec<FinishedPlan>>>,
    /// Whether the shard has yet to look over all its partitions for the
    /// ones to pace. A partition that sleeps is not looked at otherwise.
    sweep_wanted: bool,
}

impl Default for RewritePacing {
    fn default() -> Self {
        Self {
            partitions: HashMap::new(),
            max_interval_micros: micros(REWRITE_INTERVAL_MAX),
            backlog_limit: LONG_TERM_BACKLOG_LIMIT,
            backlog_counts: Rc::default(),
            plans: Rc::default(),
            sweep_wanted: false,
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// The wait after a rewrite that stored `bytes` of a segment of at most
/// `cap` bytes, when the wait before it was `interval_micros`, up to
/// `max_micros`. Zero bytes is a pass that found nothing.
fn paced_interval(interval_micros: u64, bytes: u64, cap: u64, max_micros: u64) -> u64 {
    let min = micros(REWRITE_INTERVAL_MIN);
    let max = max_micros.max(min);
    let interval = u128::from(interval_micros);
    let ideal = if bytes == 0 {
        u128::from(max)
    } else {
        let target =
            u128::from(cap).saturating_mul(u128::from(REWRITE_FILL_TARGET_PERMILLE)) / 1000;
        interval.saturating_mul(target) / u128::from(bytes)
    };
    let blend = u128::from(if ideal < interval {
        REWRITE_SPEEDUP_PERMILLE
    } else {
        REWRITE_SLOWDOWN_PERMILLE
    });
    let blended = interval
        .saturating_mul(1000 - blend)
        .saturating_add(ideal.saturating_mul(blend))
        / 1000;
    u64::try_from(blended).unwrap_or(u64::MAX).clamp(min, max)
}

impl RewritePacing {
    /// Whether `key` has a wait here, so its passes start themselves.
    #[must_use]
    pub fn paces(&self, key: &RewriteKey) -> bool {
        self.partitions.contains_key(key)
    }

    /// Give `key` a wait at `now`, after which its first pass is due. A
    /// partition that has a wait keeps it.
    ///
    /// The first pass is not due at once: a segment of the first batch alone
    /// would say nothing of the load, and lengthen the wait to its longest.
    pub fn enter(&mut self, key: RewriteKey, now: u64) {
        if !self.paces(&key) {
            let pace = self.pace(key);
            pace.due_at = Some(now.saturating_add(pace.interval_micros));
        }
    }

    /// The shard can rewrite again: it looks over all its partitions for
    /// the ones to pace.
    pub const fn want_sweep(&mut self) {
        self.sweep_wanted = true;
    }

    /// Whether [`Self::want_sweep`] was called since the last call.
    pub const fn take_sweep_wanted(&mut self) -> bool {
        std::mem::replace(&mut self.sweep_wanted, false)
    }

    /// The partitions whose pass is due at `now`. Each one waits for
    /// [`Self::settle`] or [`Self::forget`] from then on.
    pub fn take_due(&mut self, now: u64) -> Vec<RewriteKey> {
        let mut due = Vec::new();
        for (key, pace) in &mut self.partitions {
            if pace.due_at.is_some_and(|due_at| due_at <= now) {
                pace.due_at = None;
                due.push(*key);
            }
        }
        due
    }

    /// A rewrite of `key` runs, and `lag` messages wait for it. No pass is
    /// due until its result is settled.
    pub fn started(&mut self, key: RewriteKey, lag: u64) {
        let pace = self.pace(key);
        pace.due_at = None;
        pace.lag = lag;
        pace.awaits_commit = false;
    }

    /// Take the result of a pass over `key` at `now`, and set when the next
    /// one is due. A segment that ended before the commit point has more
    /// behind it, so its pass is also due once [`Self::committed`] says the
    /// segment's command committed. The wait set here then only covers a
    /// command that got lost.
    pub fn settle(&mut self, key: RewriteKey, result: RewriteResult, now: u64) {
        let shard_max = self.max_interval_micros;
        let pace = self.pace(key);
        let max = pace.max_interval_micros.unwrap_or(shard_max);
        pace.settled_at = now;
        pace.failed = result == RewriteResult::Unchanged;
        let wait = match result {
            RewriteResult::Stored {
                bytes,
                cap,
                reached_commit,
            } => {
                pace.interval_micros = paced_interval(pace.interval_micros, bytes, cap, max);
                pace.awaits_commit = !reached_commit;
                if reached_commit {
                    pace.lag = 0;
                }
                pace.interval_micros
            }
            RewriteResult::Idle => {
                pace.interval_micros = paced_interval(pace.interval_micros, 0, 0, max);
                pace.awaits_commit = false;
                pace.lag = 0;
                pace.interval_micros
            }
            RewriteResult::Resent => micros(REWRITE_INTERVAL_MIN),
            RewriteResult::Registering => {
                pace.awaits_commit = true;
                micros(REWRITE_INTERVAL_MIN)
            }
            RewriteResult::Unchanged => {
                pace.awaits_commit = false;
                pace.interval_micros
            }
        };
        pace.due_at = Some(now.saturating_add(wait));
    }

    /// A command of the long-term log of `key` committed at `now`. The pass
    /// that waited for it is due.
    pub fn committed(&mut self, key: &RewriteKey, now: u64) {
        if let Some(pace) = self.partitions.get_mut(key)
            && std::mem::take(&mut pace.awaits_commit)
        {
            pace.due_at = Some(now);
        }
    }

    /// The backlog of `key` is past the limit at `now`: its next pass is
    /// due at once, and no sooner than [`REWRITE_INTERVAL_MIN`] after the
    /// last one. A pass that runs, waits for a command, or follows one that
    /// failed keeps its time, so a store that refuses is not asked in a
    /// loop.
    pub fn hurry(&mut self, key: &RewriteKey, now: u64) {
        if let Some(pace) = self.partitions.get_mut(key)
            && !pace.awaits_commit
            && !pace.failed
            && let Some(due_at) = pace.due_at
        {
            let soonest = pace.settled_at.saturating_add(micros(REWRITE_INTERVAL_MIN));
            pace.due_at = Some(due_at.min(now.max(soonest)));
        }
    }

    /// Set the longest wait between two rewrites of a partition. A wait
    /// below [`REWRITE_INTERVAL_MIN`] counts as it. A partition keeps the
    /// wait it has until its next pass settles.
    pub fn set_max_interval(&mut self, interval: Duration) {
        self.max_interval_micros = micros(interval).max(micros(REWRITE_INTERVAL_MIN));
    }

    /// See [`Self::set_max_interval`].
    #[must_use]
    pub const fn max_interval(&self) -> Duration {
        Duration::from_micros(self.max_interval_micros)
    }

    /// Set the longest wait of `key` alone, as the topic of the partition
    /// sets it, or `None` to follow [`Self::set_max_interval`]. A wait below
    /// [`REWRITE_INTERVAL_MIN`] counts as it. The partition keeps the wait it
    /// has until its next pass settles.
    pub fn set_ceiling(&mut self, key: RewriteKey, ceiling: Option<Duration>) {
        self.pace(key).max_interval_micros =
            ceiling.map(|ceiling| micros(ceiling).max(micros(REWRITE_INTERVAL_MIN)));
    }

    /// Set the backlog past which the sends to a partition are delayed.
    /// Zero turns the delay off.
    pub const fn set_backlog_limit(&mut self, bytes: u64) {
        self.backlog_limit = bytes;
    }

    /// See [`Self::set_backlog_limit`].
    #[must_use]
    pub const fn backlog_limit(&self) -> u64 {
        self.backlog_limit
    }

    /// Where a walk of a partition's log leaves its count, from a task
    /// apart from the pump.
    #[must_use]
    pub fn backlog_counts(&self) -> Rc<RefCell<Vec<BacklogCount>>> {
        Rc::clone(&self.backlog_counts)
    }

    /// The counts the walks left since the last call.
    #[must_use]
    pub fn take_backlog_counts(&self) -> Vec<BacklogCount> {
        std::mem::take(&mut *self.backlog_counts.borrow_mut())
    }

    /// Where a plan of a partition's next segment leaves what it found,
    /// from a task apart from the pump.
    #[must_use]
    pub fn plans(&self) -> Rc<RefCell<Vec<FinishedPlan>>> {
        Rc::clone(&self.plans)
    }

    /// What the plans found since the last call.
    #[must_use]
    pub fn take_plans(&self) -> Vec<FinishedPlan> {
        std::mem::take(&mut *self.plans.borrow_mut())
    }

    /// The partitions that have a wait here.
    pub fn paced(&self) -> impl Iterator<Item = &RewriteKey> {
        self.partitions.keys()
    }

    /// Record how many messages of `key` wait for a rewrite.
    pub fn set_lag(&mut self, key: RewriteKey, lag: u64) {
        self.pace(key).lag = lag;
    }

    /// Drop `key`: its partition is gone, or the shard cannot rewrite now.
    pub fn forget(&mut self, key: &RewriteKey) {
        self.partitions.remove(key);
    }

    /// The most messages one partition here committed that no long-term
    /// segment holds.
    #[must_use]
    pub fn worst_lag(&self) -> u64 {
        self.partitions
            .values()
            .map(|pace| pace.lag)
            .max()
            .unwrap_or(0)
    }

    /// The wait between two passes over `key`, `None` for a partition
    /// without one.
    #[must_use]
    pub fn interval(&self, key: &RewriteKey) -> Option<Duration> {
        self.partitions
            .get(key)
            .map(|pace| Duration::from_micros(pace.interval_micros))
    }

    fn pace(&mut self, key: RewriteKey) -> &mut Pace {
        self.partitions.entry(key).or_insert_with(|| Pace {
            interval_micros: micros(REWRITE_INTERVAL_START),
            max_interval_micros: None,
            due_at: None,
            lag: 0,
            awaits_commit: false,
            settled_at: 0,
            failed: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A round number, so the waits below come out whole.
    const CAP: u64 = 64_000_000;
    const TARGET: u64 = CAP * REWRITE_FILL_TARGET_PERMILLE / 1000;

    fn key() -> RewriteKey {
        (IggyNamespace::new(1, 1, 0), 7)
    }

    /// A segment of `bytes` that holds every message its rewrite found
    /// committed.
    fn stored(bytes: u64) -> RewriteResult {
        RewriteResult::Stored {
            bytes,
            cap: CAP,
            reached_commit: true,
        }
    }

    /// A segment that ended at the byte cap, with committed messages left.
    fn cut_at_the_cap() -> RewriteResult {
        RewriteResult::Stored {
            bytes: CAP,
            cap: CAP,
            reached_commit: false,
        }
    }

    fn interval_micros(pacing: &RewritePacing) -> u64 {
        micros(pacing.interval(&key()).unwrap_or(REWRITE_INTERVAL_START))
    }

    /// Settle `passes` rewrites of a partition that commits `rate` bytes
    /// per second, each storing what one wait let through, and return the
    /// wait they end at.
    fn wait_at_a_steady_load(pacing: &mut RewritePacing, rate: u64, passes: usize) -> u64 {
        for _ in 0..passes {
            let bytes = (u128::from(rate) * u128::from(interval_micros(pacing)) / 1_000_000)
                .min(u128::from(CAP));
            pacing.settle(key(), stored(u64::try_from(bytes).unwrap()), 0);
        }
        interval_micros(pacing)
    }

    #[test]
    fn given_a_full_segment_when_settling_should_move_nine_tenths_of_the_way_to_the_shorter_wait() {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), RewriteResult::Idle, 0);
        assert_eq!(pacing.interval(&key()), Some(Duration::from_millis(4600)));

        pacing.settle(key(), stored(CAP), 0);

        // The ideal wait is 3.68 s: 460 ms of the old wait, 3.312 s of it.
        assert_eq!(pacing.interval(&key()), Some(Duration::from_millis(3772)));
    }

    #[test]
    fn given_a_segment_half_the_target_when_settling_should_move_four_tenths_of_the_way_to_the_longer_wait()
     {
        let mut pacing = RewritePacing::default();

        pacing.settle(key(), stored(TARGET / 2), 0);

        // The ideal wait is 2 s: 600 ms of the old wait, 800 ms of it.
        assert_eq!(pacing.interval(&key()), Some(Duration::from_millis(1400)));
    }

    #[test]
    fn given_a_segment_at_the_target_when_settling_should_keep_the_wait() {
        let mut pacing = RewritePacing::default();

        pacing.settle(key(), stored(TARGET), 0);

        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_START));
    }

    #[test]
    fn given_a_steady_load_when_the_wait_starts_below_its_target_should_converge_to_it() {
        // The target fills in four seconds.
        let rate = TARGET / 4;
        let mut pacing = RewritePacing::default();
        assert!(micros(REWRITE_INTERVAL_START) < 4_000_000);

        let wait = wait_at_a_steady_load(&mut pacing, rate, 40);

        assert!((3_999_000..=4_001_000).contains(&wait), "{wait}");
    }

    #[test]
    fn given_a_steady_load_when_the_wait_starts_above_its_target_should_converge_to_it() {
        // The target fills in two seconds.
        let rate = TARGET / 2;
        let mut pacing = RewritePacing::default();
        for _ in 0..20 {
            pacing.settle(key(), RewriteResult::Idle, 0);
        }
        assert!(interval_micros(&pacing) > 9_000_000);

        let wait = wait_at_a_steady_load(&mut pacing, rate, 40);

        assert!((1_999_000..=2_001_000).contains(&wait), "{wait}");
    }

    #[test]
    fn given_full_segments_when_settling_should_shorten_the_wait_down_to_the_minimum() {
        let mut pacing = RewritePacing::default();
        let mut intervals = Vec::new();
        for _ in 0..8 {
            pacing.settle(key(), stored(CAP), 0);
            intervals.push(interval_micros(&pacing));
        }

        assert!(intervals.is_sorted_by(|earlier, later| earlier >= later));
        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_MIN));
    }

    #[test]
    fn given_a_tiny_segment_when_settling_should_lengthen_the_wait_to_the_maximum() {
        let mut pacing = RewritePacing::default();

        pacing.settle(key(), stored(1), 0);

        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_MAX));
    }

    #[test]
    fn given_idle_passes_when_settling_should_move_the_wait_toward_the_maximum() {
        let mut pacing = RewritePacing::default();
        let mut intervals = Vec::new();
        for _ in 0..3 {
            pacing.settle(key(), RewriteResult::Idle, 0);
            intervals.push(pacing.interval(&key()).unwrap().as_millis());
        }

        assert_eq!(intervals, [4600, 6760, 8056]);
        for _ in 0..40 {
            pacing.settle(key(), RewriteResult::Idle, 0);
        }
        let wait = interval_micros(&pacing);
        assert!((9_999_000..=10_000_000).contains(&wait), "{wait}");
    }

    #[test]
    fn given_a_failure_or_a_resend_when_settling_should_keep_the_wait() {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), stored(CAP), 0);
        let wait = pacing.interval(&key());

        pacing.settle(key(), RewriteResult::Unchanged, 0);
        pacing.settle(key(), RewriteResult::Resent, 0);

        assert_eq!(pacing.interval(&key()), wait);
    }

    #[test]
    fn given_extreme_sizes_and_waits_when_pacing_should_stay_between_one_and_ten_seconds() {
        let (min, max) = (1_000_000, 10_000_000);

        for interval in [0, 1, min, max, u64::MAX] {
            for bytes in [0, 1, CAP, u64::MAX] {
                for cap in [0, 1, CAP, u64::MAX] {
                    let paced = paced_interval(interval, bytes, cap, max);
                    assert!(
                        (min..=max).contains(&paced),
                        "{interval} {bytes} {cap}: {paced}"
                    );
                }
            }
        }
        assert_eq!(paced_interval(u64::MAX, 1, u64::MAX, max), max);
        assert_eq!(paced_interval(min, u64::MAX, 1, max), min);
    }

    #[test]
    fn given_a_ceiling_of_two_seconds_when_pacing_should_stay_between_one_and_two_seconds() {
        let (min, max) = (1_000_000, 2_000_000);
        let mut pacing = RewritePacing::default();
        pacing.set_max_interval(Duration::from_secs(2));

        for interval in [0, 1, min, max, 10_000_000, u64::MAX] {
            for bytes in [0, 1, CAP, u64::MAX] {
                for cap in [0, 1, CAP, u64::MAX] {
                    let paced = paced_interval(interval, bytes, cap, max);
                    assert!(
                        (min..=max).contains(&paced),
                        "{interval} {bytes} {cap}: {paced}"
                    );
                }
            }
        }
        pacing.settle(key(), stored(1), 0);
        assert_eq!(pacing.interval(&key()), Some(Duration::from_secs(2)));
        for _ in 0..40 {
            pacing.settle(key(), RewriteResult::Idle, 0);
        }
        assert_eq!(pacing.interval(&key()), Some(Duration::from_secs(2)));
        assert_eq!(pacing.take_due(1_999_999), []);
        assert_eq!(pacing.take_due(2_000_000), [key()]);
        for _ in 0..8 {
            pacing.settle(key(), stored(CAP), 0);
        }
        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_MIN));
    }

    #[test]
    fn given_partitions_with_their_own_ceilings_when_quiet_should_wait_each_its_own() {
        let partition = |id: usize| (IggyNamespace::new(1, 1, id), 7);
        let (quick, slow, unset) = (partition(0), partition(1), partition(2));
        let mut pacing = RewritePacing::default();
        pacing.set_ceiling(quick, Some(Duration::from_secs(2)));
        pacing.set_ceiling(slow, Some(Duration::from_secs(30)));

        for key in [quick, slow, unset] {
            pacing.settle(key, stored(1), 0);
            for _ in 0..8 {
                pacing.settle(key, RewriteResult::Idle, 0);
            }
        }

        assert_eq!(pacing.interval(&quick), Some(Duration::from_secs(2)));
        assert_eq!(pacing.interval(&slow), Some(Duration::from_secs(30)));
        assert_eq!(pacing.interval(&unset), Some(REWRITE_INTERVAL_MAX));
        assert_eq!(pacing.take_due(1_999_999), []);
        assert_eq!(pacing.take_due(2_000_000), [quick]);
        assert_eq!(pacing.take_due(29_999_999), [unset]);
        assert_eq!(pacing.take_due(30_000_000), [slow]);
        // Below the floor, a ceiling counts as the floor.
        pacing.set_ceiling(quick, Some(Duration::from_millis(10)));
        pacing.settle(quick, RewriteResult::Idle, 0);
        assert_eq!(pacing.interval(&quick), Some(REWRITE_INTERVAL_MIN));
    }

    #[test]
    fn given_a_ceiling_of_one_second_when_settling_any_pass_should_wait_one_second() {
        let mut pacing = RewritePacing::default();
        pacing.set_max_interval(Duration::from_secs(1));
        let passes = [
            stored(1),
            RewriteResult::Idle,
            stored(CAP),
            stored(TARGET / 2),
            RewriteResult::Unchanged,
            RewriteResult::Resent,
            RewriteResult::Registering,
            stored(TARGET),
        ];

        for (index, pass) in (0_u64..).zip(passes) {
            let now = index * 10_000_000;
            pacing.settle(key(), pass, now);
            assert_eq!(
                pacing.interval(&key()),
                Some(Duration::from_secs(1)),
                "{pass:?}"
            );
            assert_eq!(pacing.take_due(now + 999_999), [], "{pass:?}");
            assert_eq!(pacing.take_due(now + 1_000_000), [key()], "{pass:?}");
        }
    }

    #[test]
    fn given_a_ceiling_below_a_second_when_setting_it_should_take_a_second() {
        let mut pacing = RewritePacing::default();
        assert_eq!(pacing.max_interval(), REWRITE_INTERVAL_MAX);

        pacing.set_max_interval(Duration::from_millis(500));
        pacing.settle(key(), stored(1), 0);

        assert_eq!(pacing.max_interval(), REWRITE_INTERVAL_MIN);
        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_MIN));
        assert_eq!(
            paced_interval(u64::MAX, 1, CAP, 0),
            micros(REWRITE_INTERVAL_MIN)
        );
    }

    #[test]
    fn given_a_segment_cut_at_the_cap_when_its_command_commits_should_make_the_next_pass_due_then()
    {
        let mut pacing = RewritePacing::default();
        pacing.started(key(), 10);
        assert_eq!(pacing.take_due(u64::MAX), []);

        pacing.settle(key(), cut_at_the_cap(), 1_000);
        assert_eq!(pacing.take_due(1_000), [], "its command is not committed");
        assert_eq!(pacing.take_due(2_000), []);

        pacing.committed(&key(), 3_000);

        assert_eq!(pacing.take_due(3_000), [key()]);
        assert_eq!(pacing.take_due(u64::MAX), [], "a pass is taken once");
    }

    #[test]
    fn given_a_segment_cut_at_the_cap_when_its_command_is_lost_should_make_the_next_pass_due_after_the_wait()
     {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), cut_at_the_cap(), 1_000);
        let wait = interval_micros(&pacing);

        assert_eq!(pacing.take_due(1_000 + wait - 1), []);
        assert_eq!(pacing.take_due(1_000 + wait), [key()]);
    }

    #[test]
    fn given_a_segment_that_reached_the_commit_point_when_its_command_commits_should_wait_the_interval()
     {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), stored(TARGET), 1_000);

        pacing.committed(&key(), 2_000);

        assert_eq!(pacing.take_due(1_000_999), []);
        assert_eq!(pacing.take_due(1_001_000), [key()]);
    }

    #[test]
    fn given_a_running_rewrite_when_an_earlier_command_commits_should_not_make_a_pass_due() {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), cut_at_the_cap(), 0);
        pacing.started(key(), 10);

        pacing.committed(&key(), 1_000);

        assert_eq!(pacing.take_due(u64::MAX), []);
        assert!(!pacing.paces(&(IggyNamespace::new(1, 1, 1), 7)));
        pacing.committed(&(IggyNamespace::new(1, 1, 1), 7), 1_000);
        assert!(!pacing.paces(&(IggyNamespace::new(1, 1, 1), 7)));
    }

    /// A rewrite stores its segment once the registration commits, so the
    /// commit makes the pass due, and a registration that got lost is sent
    /// again after the minimum.
    #[test]
    fn given_a_registration_in_flight_when_it_commits_should_make_the_next_pass_due_at_once() {
        let mut pacing = RewritePacing::default();
        let min = micros(REWRITE_INTERVAL_MIN);
        pacing.settle(key(), RewriteResult::Registering, 1_000);
        assert_eq!(pacing.take_due(1_000 + min - 1), []);

        pacing.committed(&key(), 2_000);

        assert_eq!(pacing.take_due(2_000), [key()]);
        pacing.settle(key(), RewriteResult::Registering, 3_000);
        assert_eq!(pacing.take_due(3_000 + min), [key()]);
    }

    #[test]
    fn given_a_partition_without_a_wait_when_entering_should_make_its_first_pass_due_after_the_first_wait()
     {
        let mut pacing = RewritePacing::default();
        let wait = micros(REWRITE_INTERVAL_START);

        pacing.enter(key(), 5_000);

        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_START));
        assert_eq!(pacing.take_due(5_000 + wait - 1), []);
        assert_eq!(pacing.take_due(5_000 + wait), [key()]);
    }

    #[test]
    fn given_a_partition_with_a_wait_when_entering_should_keep_its_wait() {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), stored(TARGET), 1_000);
        pacing.started(key(), 10);

        pacing.enter(key(), 2_000);

        assert_eq!(pacing.take_due(u64::MAX), [], "its rewrite runs");
        pacing.settle(key(), stored(TARGET), 3_000);
        pacing.enter(key(), 4_000);
        assert_eq!(pacing.take_due(4_000), []);
        assert_eq!(pacing.take_due(1_003_000), [key()]);
    }

    #[test]
    fn given_a_wanted_sweep_when_taking_it_should_give_it_once() {
        let mut pacing = RewritePacing::default();
        assert!(!pacing.take_sweep_wanted());

        pacing.want_sweep();

        assert!(pacing.take_sweep_wanted());
        assert!(!pacing.take_sweep_wanted());
    }

    #[test]
    fn given_a_resent_command_when_settling_should_make_the_next_pass_due_after_the_minimum() {
        let mut pacing = RewritePacing::default();
        let min = micros(REWRITE_INTERVAL_MIN);

        pacing.settle(key(), RewriteResult::Resent, 5_000_000);

        assert_eq!(pacing.take_due(5_000_000 + min - 1), []);
        assert_eq!(pacing.take_due(5_000_000 + min), [key()]);
    }

    #[test]
    fn given_a_resent_command_of_a_segment_cut_at_the_cap_when_it_commits_should_make_the_next_pass_due_then()
     {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), cut_at_the_cap(), 0);
        pacing.settle(key(), RewriteResult::Resent, 1_000);

        pacing.committed(&key(), 2_000);

        assert_eq!(pacing.take_due(2_000), [key()]);
    }

    #[test]
    fn given_a_failed_pass_when_its_wait_has_not_passed_should_not_be_due() {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), RewriteResult::Unchanged, 5_000_000);

        assert_eq!(pacing.take_due(5_999_999), []);
        assert_eq!(pacing.take_due(6_000_000), [key()]);
    }

    #[test]
    fn given_a_backlog_at_or_below_the_limit_or_no_limit_when_delaying_a_send_should_not_delay_it()
    {
        assert_eq!(backlog_delay(0, 1_000), None);
        assert_eq!(backlog_delay(1_000, 1_000), None);
        assert_eq!(backlog_delay(u64::MAX, 0), None, "zero turns the delay off");
    }

    #[test]
    fn given_a_backlog_past_the_limit_when_delaying_a_send_should_grow_with_it_up_to_the_most() {
        let limit = 1_000_000;

        let delays: Vec<_> = (1..=40)
            .map(|step| backlog_delay(limit + step * limit / 20, limit).unwrap())
            .collect();

        assert_eq!(backlog_delay(limit + 1, limit), Some(BACKLOG_DELAY_MIN));
        assert_eq!(
            backlog_delay(limit + limit / 2, limit),
            Some(Duration::from_millis(130))
        );
        assert!(delays.is_sorted());
        assert_eq!(delays[19], BACKLOG_DELAY_MAX, "twice the limit");
        assert!(delays[20..].iter().all(|delay| *delay == BACKLOG_DELAY_MAX));
        assert_eq!(backlog_delay(u64::MAX, limit), Some(BACKLOG_DELAY_MAX));
        assert_eq!(
            backlog_delay(u64::MAX, u64::MAX - 1),
            Some(BACKLOG_DELAY_MIN)
        );
    }

    #[test]
    fn given_a_backlog_past_the_limit_when_its_partition_waits_out_its_interval_should_make_the_pass_due_at_once()
     {
        let mut pacing = RewritePacing::default();
        pacing.settle(key(), stored(1), 1_000);
        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_MAX));
        assert_eq!(pacing.take_due(2_000_000), []);

        pacing.hurry(&key(), 2_000_000);

        assert_eq!(pacing.take_due(2_000_000), [key()]);
    }

    #[test]
    fn given_a_pass_that_settled_a_moment_ago_when_hurried_should_wait_the_shortest_interval() {
        let mut pacing = RewritePacing::default();
        let soonest = 1_000 + micros(REWRITE_INTERVAL_MIN);
        pacing.settle(key(), stored(1), 1_000);

        pacing.hurry(&key(), 2_000);

        assert_eq!(pacing.take_due(soonest - 1), []);
        assert_eq!(pacing.take_due(soonest), [key()]);
    }

    #[test]
    fn given_a_failed_pass_or_one_that_waits_for_its_command_or_runs_when_hurried_should_keep_its_time()
     {
        let other = (IggyNamespace::new(1, 1, 1), 7);
        let running = (IggyNamespace::new(1, 1, 2), 7);
        let mut pacing = RewritePacing::default();
        // Waits long enough that a hurry would bring them forward.
        for key in [key(), other] {
            pacing.settle(key, stored(1), 0);
        }
        pacing.settle(key(), RewriteResult::Unchanged, 0);
        pacing.settle(other, cut_at_the_cap(), 0);
        pacing.started(running, 10);
        let now = 2 * micros(REWRITE_INTERVAL_MIN);

        for key in [key(), other, running] {
            pacing.hurry(&key, now);
        }

        assert_eq!(pacing.take_due(now), []);
        let unpaced = (IggyNamespace::new(1, 1, 3), 7);
        pacing.hurry(&unpaced, now);
        assert!(!pacing.paces(&unpaced));
    }

    #[test]
    fn given_no_limit_set_when_asking_for_it_should_give_the_default() {
        let mut pacing = RewritePacing::default();
        assert_eq!(pacing.backlog_limit(), LONG_TERM_BACKLOG_LIMIT);

        pacing.set_backlog_limit(0);

        assert_eq!(pacing.backlog_limit(), 0);
    }

    #[test]
    fn given_the_lags_of_two_partitions_when_asking_for_the_worst_should_return_the_larger() {
        let mut pacing = RewritePacing::default();
        let other = (IggyNamespace::new(1, 1, 1), 7);
        pacing.set_lag(key(), 5);
        pacing.started(other, 40);
        assert_eq!(pacing.worst_lag(), 40);

        pacing.settle(other, cut_at_the_cap(), 0);
        assert_eq!(pacing.worst_lag(), 40, "a cut segment leaves a backlog");
        pacing.settle(other, stored(1), 0);
        assert_eq!(pacing.worst_lag(), 5);

        pacing.forget(&key());
        assert_eq!(pacing.worst_lag(), 0);
        assert!(!pacing.paces(&key()));
    }
}
