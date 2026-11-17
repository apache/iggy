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
//! [`REWRITE_INTERVAL_MIN`] and [`REWRITE_INTERVAL_MAX`]. After every
//! rewrite it moves toward the wait at which a segment comes out
//! [`REWRITE_FILL_TARGET_PERMILLE`] full: quickly when that wait is
//! shorter, slowly when it is longer.
//!
//! A segment that ended at its byte cap left committed messages behind.
//! The pass for them starts when the segment's command commits, since a
//! rewrite continues where the long-term log ends.
//!
//! Every decision takes the time it is made at, so nothing here reads a
//! clock.

use server_common::sharding::IggyNamespace;
use std::collections::HashMap;
use std::time::Duration;

/// The shortest wait between two rewrites of one partition.
pub const REWRITE_INTERVAL_MIN: Duration = Duration::from_millis(250);

/// The longest wait between two rewrites of one partition, and so how far
/// behind its committed messages the long-term segments of a partition with
/// little load are.
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

/// A partition incarnation: slab ids are reused after a delete, so the
/// namespace alone can name a later partition.
pub type RewriteKey = (IggyNamespace, u64);

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
    /// It failed.
    Unchanged,
}

#[derive(Debug, Clone, Copy)]
struct Pace {
    interval_micros: u64,
    /// When the next pass is due, in the caller's microseconds. `None`
    /// while a rewrite runs: its result sets it.
    due_at: Option<u64>,
    /// Messages the partition committed that no long-term segment holds, as
    /// its last pass found.
    lag: u64,
    /// Whether the next pass waits for the command of a stored segment that
    /// left committed messages behind.
    awaits_commit: bool,
}

/// The waits of the partitions of one shard.
#[derive(Debug, Default)]
pub struct RewritePacing {
    partitions: HashMap<RewriteKey, Pace>,
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// The wait after a rewrite that stored `bytes` of a segment of at most
/// `cap` bytes, when the wait before it was `interval_micros`. Zero bytes
/// is a pass that found nothing.
fn paced_interval(interval_micros: u64, bytes: u64, cap: u64) -> u64 {
    let (min, max) = (micros(REWRITE_INTERVAL_MIN), micros(REWRITE_INTERVAL_MAX));
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
        let pace = self.pace(key);
        let wait = match result {
            RewriteResult::Stored {
                bytes,
                cap,
                reached_commit,
            } => {
                pace.interval_micros = paced_interval(pace.interval_micros, bytes, cap);
                pace.awaits_commit = !reached_commit;
                if reached_commit {
                    pace.lag = 0;
                }
                pace.interval_micros
            }
            RewriteResult::Idle => {
                pace.interval_micros = paced_interval(pace.interval_micros, 0, 0);
                pace.awaits_commit = false;
                pace.lag = 0;
                pace.interval_micros
            }
            RewriteResult::Resent => micros(REWRITE_INTERVAL_MIN),
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
            due_at: None,
            lag: 0,
            awaits_commit: false,
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

        pacing.settle(key(), stored(CAP), 0);

        // The ideal wait is 800 ms: 100 ms of the old wait, 720 ms of it.
        assert_eq!(pacing.interval(&key()), Some(Duration::from_millis(820)));
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
        // The target fills in half a second.
        let rate = TARGET * 2;
        let mut pacing = RewritePacing::default();
        for _ in 0..20 {
            pacing.settle(key(), RewriteResult::Idle, 0);
        }
        assert!(interval_micros(&pacing) > 9_000_000);

        let wait = wait_at_a_steady_load(&mut pacing, rate, 40);

        assert!((499_000..=501_000).contains(&wait), "{wait}");
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
    fn given_extreme_sizes_and_waits_when_pacing_should_stay_between_the_bounds() {
        let (min, max) = (micros(REWRITE_INTERVAL_MIN), micros(REWRITE_INTERVAL_MAX));

        for interval in [0, 1, min, max, u64::MAX] {
            for bytes in [0, 1, CAP, u64::MAX] {
                for cap in [0, 1, CAP, u64::MAX] {
                    let paced = paced_interval(interval, bytes, cap);
                    assert!(
                        (min..=max).contains(&paced),
                        "{interval} {bytes} {cap}: {paced}"
                    );
                }
            }
        }
        assert_eq!(paced_interval(u64::MAX, 1, u64::MAX), max);
        assert_eq!(paced_interval(min, u64::MAX, 1), min);
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
