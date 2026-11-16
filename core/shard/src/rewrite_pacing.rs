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
//! many small objects. Each partition therefore has its own wait: it halves
//! when a rewrite stored a segment that is nearly full, and doubles when
//! the segment was small or there was nothing to rewrite, between
//! [`REWRITE_INTERVAL_MIN`] and [`REWRITE_INTERVAL_MAX`].
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

/// A partition incarnation: slab ids are reused after a delete, so the
/// namespace alone can name a later partition.
pub type RewriteKey = (IggyNamespace, u64);

/// What a rewrite pass over a partition ended as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteResult {
    /// It stored a segment of this many bytes, of the `target` a segment
    /// holds at most.
    Stored { bytes: u64, target: u64 },
    /// Nothing was there to rewrite, or this replica is not the primary.
    Idle,
    /// It failed, or the pass only sent a stored segment's command again.
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
}

/// The waits of the partitions of one shard.
#[derive(Debug, Default)]
pub struct RewritePacing {
    partitions: HashMap<RewriteKey, Pace>,
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
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
    }

    /// Take the result of a pass over `key` at `now`, and set when the next
    /// one is due. A segment that came out nearly full has more behind it,
    /// so the next pass is due at once.
    pub fn settle(&mut self, key: RewriteKey, result: RewriteResult, now: u64) {
        let (min, max) = (micros(REWRITE_INTERVAL_MIN), micros(REWRITE_INTERVAL_MAX));
        let pace = self.pace(key);
        let mut wait = None;
        match result {
            RewriteResult::Stored { bytes, target } if bytes >= target / 4 * 3 => {
                pace.interval_micros = (pace.interval_micros / 2).max(min);
                wait = Some(0);
            }
            // A segment that is not full holds every message the pass
            // found committed.
            RewriteResult::Stored { bytes, target } if bytes >= target / 4 => pace.lag = 0,
            RewriteResult::Stored { .. } | RewriteResult::Idle => {
                pace.interval_micros = pace.interval_micros.saturating_mul(2).min(max);
                pace.lag = 0;
            }
            RewriteResult::Unchanged => {}
        }
        pace.due_at = Some(now.saturating_add(wait.unwrap_or(pace.interval_micros)));
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: u64 = 64 * 1024 * 1024;

    fn key() -> RewriteKey {
        (IggyNamespace::new(1, 1, 0), 7)
    }

    fn stored(bytes: u64) -> RewriteResult {
        RewriteResult::Stored {
            bytes,
            target: TARGET,
        }
    }

    #[test]
    fn given_nearly_full_segments_when_settling_should_halve_the_wait_down_to_the_minimum() {
        let mut pacing = RewritePacing::default();
        let mut intervals = Vec::new();
        for _ in 0..4 {
            pacing.settle(key(), stored(TARGET), 0);
            intervals.push(pacing.interval(&key()).unwrap());
        }

        assert_eq!(
            intervals,
            [
                Duration::from_millis(500),
                REWRITE_INTERVAL_MIN,
                REWRITE_INTERVAL_MIN,
                REWRITE_INTERVAL_MIN
            ]
        );
    }

    #[test]
    fn given_a_nearly_full_segment_when_settling_should_make_the_next_pass_due_at_once() {
        let mut pacing = RewritePacing::default();
        pacing.started(key(), 10);
        assert_eq!(pacing.take_due(u64::MAX), []);

        pacing.settle(key(), stored(TARGET / 4 * 3), 1_000);

        assert_eq!(pacing.take_due(1_000), [key()]);
        assert_eq!(pacing.take_due(u64::MAX), [], "a pass is taken once");
    }

    #[test]
    fn given_small_segments_or_idle_passes_when_settling_should_double_the_wait_up_to_the_maximum()
    {
        let mut pacing = RewritePacing::default();
        let mut intervals = Vec::new();
        for result in [
            stored(1),
            RewriteResult::Idle,
            stored(TARGET / 4 - 1),
            RewriteResult::Idle,
            RewriteResult::Idle,
        ] {
            pacing.settle(key(), result, 0);
            intervals.push(pacing.interval(&key()).unwrap().as_secs());
        }

        assert_eq!(intervals, [2, 4, 8, 10, 10]);
    }

    #[test]
    fn given_a_segment_of_a_middling_size_or_a_failure_when_settling_should_keep_the_wait() {
        let mut pacing = RewritePacing::default();

        pacing.settle(key(), stored(TARGET / 2), 0);
        pacing.settle(key(), RewriteResult::Unchanged, 0);

        assert_eq!(pacing.interval(&key()), Some(REWRITE_INTERVAL_START));
    }

    #[test]
    fn given_a_settled_pass_when_its_wait_has_not_passed_should_not_be_due() {
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

        pacing.settle(other, stored(TARGET), 0);
        assert_eq!(pacing.worst_lag(), 40, "a full segment leaves a backlog");
        pacing.settle(other, stored(1), 0);
        assert_eq!(pacing.worst_lag(), 5);

        pacing.forget(&key());
        assert_eq!(pacing.worst_lag(), 0);
        assert!(!pacing.paces(&key()));
    }
}
