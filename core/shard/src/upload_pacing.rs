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

//! What decides whether a shard starts an upload now: a byte budget shared
//! by every partition of the shard, and a wait per partition after a failed
//! upload. Every decision takes the time it is made at, so nothing here reads
//! a clock.

use server_common::sharding::IggyNamespace;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Bytes per second a shard's uploads may average.
pub const UPLOAD_RATE_BYTES_PER_SEC: u64 = 64 * 1024 * 1024;

/// Bytes a shard may upload at once after a quiet period.
pub const UPLOAD_BURST_BYTES: u64 = 256 * 1024 * 1024;

/// The wait after a first transient failure, doubled by each one that follows.
pub const UPLOAD_RETRY_BASE: Duration = Duration::from_secs(1);

/// The longest wait between two attempts, and the wait after a failure that
/// no retry is expected to change.
pub const UPLOAD_RETRY_CAP: Duration = Duration::from_secs(60);

/// The upload lag of one partition from which it is reported.
pub const UPLOAD_LAG_ALARM: Duration = Duration::from_mins(10);

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// A budget in bytes that refills at a fixed rate up to a capacity.
#[derive(Debug)]
pub struct TokenBucket {
    rate_per_sec: u64,
    capacity: u64,
    /// Below zero after an upload larger than the bucket: the debt is repaid
    /// at the rate before anything else is admitted.
    tokens: i128,
    refilled_at: Instant,
}

impl TokenBucket {
    pub fn full(rate_per_sec: u64, capacity: u64, now: Instant) -> Self {
        Self {
            rate_per_sec,
            capacity,
            tokens: i128::from(capacity),
            refilled_at: now,
        }
    }

    /// Take the tokens of an upload of `bytes`, or none when the bucket holds
    /// too few. An upload larger than the bucket is admitted once the bucket
    /// is full instead of never, and leaves the bytes the bucket did not hold
    /// as a debt, so every byte it sends counts against the rate.
    pub fn try_take(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if self.tokens < i128::from(bytes.min(self.capacity)) {
            return false;
        }
        self.tokens -= i128::from(bytes);
        true
    }

    #[cfg(test)]
    pub fn tokens(&mut self, now: Instant) -> i128 {
        self.refill(now);
        self.tokens
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        let earned = u128::from(self.rate_per_sec) * elapsed.as_nanos() / NANOS_PER_SEC;
        if earned == 0 {
            return;
        }
        let earned = i128::try_from(earned).unwrap_or(i128::MAX);
        let tokens = self.tokens.saturating_add(earned);
        if tokens >= i128::from(self.capacity) {
            self.tokens = i128::from(self.capacity);
            self.refilled_at = now;
            return;
        }
        // Only the time that the whole tokens cost leaves the clock, so calls
        // closer together than one token do not starve the bucket.
        let paid = earned.unsigned_abs() * NANOS_PER_SEC / u128::from(self.rate_per_sec);
        self.tokens = tokens;
        self.refilled_at += Duration::from_nanos(u64::try_from(paid).unwrap_or(u64::MAX));
    }
}

/// The wait of one partition after its uploads failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadBackoff {
    wait: Duration,
    retry_at: Instant,
}

impl UploadBackoff {
    /// The backoff after a failure at `now` that follows `previous`.
    pub fn after_failure(previous: Option<Self>, transient: bool, now: Instant) -> Self {
        let wait = match previous {
            _ if !transient => UPLOAD_RETRY_CAP,
            None => UPLOAD_RETRY_BASE,
            Some(previous) => previous.wait.saturating_mul(2).min(UPLOAD_RETRY_CAP),
        };
        Self {
            wait,
            retry_at: now + wait,
        }
    }

    pub fn allows(&self, now: Instant) -> bool {
        now >= self.retry_at
    }
}

/// A change in whether a partition's upload lag is over the alarm line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagChange {
    Raised,
    Recovered,
}

#[derive(Debug)]
pub struct UploadPacing {
    pub bucket: TokenBucket,
    /// Keyed with the partition's creation revision: slab ids are reused, and
    /// a new partition does not inherit the wait of the one it replaced.
    backoffs: HashMap<IggyNamespace, (u64, UploadBackoff)>,
    /// The partitions whose lag was last seen over the alarm line, so the
    /// crossing is reported once and not on every pass.
    lagging: HashSet<IggyNamespace>,
}

impl UploadPacing {
    pub fn new(now: Instant) -> Self {
        Self {
            bucket: TokenBucket::full(UPLOAD_RATE_BYTES_PER_SEC, UPLOAD_BURST_BYTES, now),
            backoffs: HashMap::new(),
            lagging: HashSet::new(),
        }
    }

    /// Whether the partition may start an upload of `bytes` at `now`. An
    /// admitted upload has paid for its bytes. A partition that waits out a
    /// failure is refused before the bucket is asked, so it takes no tokens
    /// from the others.
    pub fn admit(
        &mut self,
        namespace: IggyNamespace,
        created_revision: u64,
        bytes: u64,
        now: Instant,
    ) -> bool {
        let waits = self
            .backoffs
            .get(&namespace)
            .is_some_and(|(revision, backoff)| {
                *revision == created_revision && !backoff.allows(now)
            });
        !waits && self.bucket.try_take(bytes, now)
    }

    pub fn upload_failed(
        &mut self,
        namespace: IggyNamespace,
        created_revision: u64,
        transient: bool,
        now: Instant,
    ) {
        let previous = self
            .backoffs
            .get(&namespace)
            .filter(|(revision, _)| *revision == created_revision)
            .map(|(_, backoff)| *backoff);
        self.backoffs.insert(
            namespace,
            (
                created_revision,
                UploadBackoff::after_failure(previous, transient, now),
            ),
        );
    }

    pub fn upload_stored(&mut self, namespace: IggyNamespace) {
        self.backoffs.remove(&namespace);
    }

    /// Take the lag a timed pass measured for a partition. Returns the change
    /// to report, if this lag crossed the alarm line either way.
    pub fn observe_lag(&mut self, namespace: IggyNamespace, lag: Duration) -> Option<LagChange> {
        if lag >= UPLOAD_LAG_ALARM {
            self.lagging.insert(namespace).then_some(LagChange::Raised)
        } else {
            self.lagging
                .remove(&namespace)
                .then_some(LagChange::Recovered)
        }
    }

    /// Drop what is kept for partitions this shard no longer hosts.
    pub fn retain(&mut self, hosted: &[IggyNamespace]) {
        let hosted: HashSet<_> = hosted.iter().collect();
        self.backoffs
            .retain(|namespace, _| hosted.contains(namespace));
        self.lagging.retain(|namespace| hosted.contains(namespace));
    }

    #[cfg(test)]
    pub fn is_lagging(&self, namespace: IggyNamespace) -> bool {
        self.lagging.contains(&namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RATE: u64 = 100;
    const CAPACITY: u64 = 1_000;

    fn namespace() -> IggyNamespace {
        IggyNamespace::new(0, 0, 0)
    }

    fn drained(now: Instant) -> TokenBucket {
        let mut bucket = TokenBucket::full(RATE, CAPACITY, now);
        assert!(bucket.try_take(CAPACITY, now));
        bucket
    }

    #[test]
    fn given_a_drained_bucket_when_time_passes_should_refill_at_the_rate() {
        let start = Instant::now();
        let mut bucket = drained(start);

        assert_eq!(bucket.tokens(start), 0);
        assert_eq!(
            bucket.tokens(start + Duration::from_secs(2)),
            i128::from(2 * RATE)
        );
        assert!(!bucket.try_take(2 * RATE + 1, start + Duration::from_secs(2)));
        assert!(bucket.try_take(2 * RATE, start + Duration::from_secs(2)));
        assert_eq!(bucket.tokens(start + Duration::from_secs(2)), 0);
    }

    #[test]
    fn given_calls_closer_than_one_token_when_refilling_should_not_lose_the_time_between_them() {
        let start = Instant::now();
        let mut bucket = drained(start);
        let step = Duration::from_millis(4);

        for call in 1..=5 {
            bucket.tokens(start + step * call);
        }

        assert_eq!(bucket.tokens(start + step * 5), 2);
    }

    #[test]
    fn given_a_long_quiet_period_when_refilling_should_stop_at_the_capacity() {
        let start = Instant::now();
        let mut bucket = drained(start);

        assert_eq!(
            bucket.tokens(start + Duration::from_secs(3_600)),
            i128::from(CAPACITY)
        );
    }

    #[test]
    fn given_an_upload_larger_than_the_bucket_when_taking_should_admit_it_only_from_a_full_bucket()
    {
        let start = Instant::now();
        let mut bucket = drained(start);
        let nearly_full = start + Duration::from_millis(9_990);
        let full = start + Duration::from_secs(10);

        assert!(!bucket.try_take(10 * CAPACITY, nearly_full));
        assert!(bucket.try_take(10 * CAPACITY, full));
    }

    #[test]
    fn given_an_upload_larger_than_the_bucket_when_admitted_should_hold_the_next_until_its_bytes_are_paid()
     {
        let start = Instant::now();
        let mut bucket = TokenBucket::full(RATE, CAPACITY, start);
        let nearly_paid = start + Duration::from_millis(89_999);
        let paid = start + Duration::from_secs(90);

        assert!(bucket.try_take(10 * CAPACITY, start));

        assert_eq!(bucket.tokens(start), -i128::from(9 * CAPACITY));
        assert!(!bucket.try_take(1, nearly_paid));
        assert_eq!(bucket.tokens(paid), 0);
        assert!(!bucket.try_take(1, paid));
        assert!(bucket.try_take(1, paid + Duration::from_millis(10)));
    }

    #[test]
    fn given_uploads_larger_than_the_bucket_when_they_follow_each_other_should_average_the_rate() {
        let start = Instant::now();
        let mut bucket = TokenBucket::full(RATE, CAPACITY, start);
        let mut sent = 0;

        for second in 0..=1_000 {
            if bucket.try_take(10 * CAPACITY, start + Duration::from_secs(second)) {
                sent += 10 * CAPACITY;
            }
        }

        assert_eq!(
            sent,
            11 * 10 * CAPACITY,
            "one upload every 100 s, the time its bytes take at the rate"
        );
    }

    #[test]
    fn given_a_refused_upload_when_taking_should_leave_the_balance_as_it_was() {
        let start = Instant::now();
        let mut bucket = TokenBucket::full(RATE, CAPACITY, start);
        assert!(bucket.try_take(CAPACITY - 10, start));

        assert!(!bucket.try_take(11, start));

        assert_eq!(bucket.tokens(start), 10);
        assert!(bucket.try_take(10, start));
        assert!(!bucket.try_take(1, start));
    }

    #[test]
    fn given_a_time_before_the_last_refill_when_taking_should_not_refill() {
        let start = Instant::now();
        let later = start + Duration::from_secs(1);
        let mut bucket = drained(later);

        assert_eq!(bucket.tokens(start), 0);
    }

    #[test]
    fn given_transient_failures_when_they_repeat_should_double_the_wait_up_to_the_cap() {
        let start = Instant::now();
        let mut backoff = None;
        let mut waits = Vec::new();

        for _ in 0..8 {
            let next = UploadBackoff::after_failure(backoff, true, start);
            waits.push(next.wait.as_secs());
            backoff = Some(next);
        }

        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60]);
    }

    #[test]
    fn given_a_failure_when_its_wait_has_not_passed_should_refuse_the_attempt() {
        let start = Instant::now();
        let backoff = UploadBackoff::after_failure(None, true, start);

        assert!(!backoff.allows(start));
        assert!(
            !backoff.allows(start + UPLOAD_RETRY_BASE.saturating_sub(Duration::from_millis(1)))
        );
        assert!(backoff.allows(start + UPLOAD_RETRY_BASE));
    }

    #[test]
    fn given_a_permanent_failure_when_it_is_the_first_should_wait_the_cap() {
        let start = Instant::now();
        let backoff = UploadBackoff::after_failure(None, false, start);

        assert!(!backoff.allows(start + UPLOAD_RETRY_CAP.saturating_sub(Duration::from_millis(1))));
        assert!(backoff.allows(start + UPLOAD_RETRY_CAP));
    }

    #[test]
    fn given_a_failed_partition_when_an_upload_is_stored_should_start_the_wait_over() {
        let start = Instant::now();
        let mut pacing = UploadPacing::new(start);
        pacing.upload_failed(namespace(), 7, true, start);
        pacing.upload_failed(namespace(), 7, true, start);
        assert!(!pacing.admit(namespace(), 7, 1, start + UPLOAD_RETRY_BASE));
        assert!(pacing.admit(namespace(), 7, 1, start + UPLOAD_RETRY_BASE * 2));

        pacing.upload_stored(namespace());

        assert!(pacing.admit(namespace(), 7, 1, start));
        pacing.upload_failed(namespace(), 7, true, start);
        assert!(pacing.admit(namespace(), 7, 1, start + UPLOAD_RETRY_BASE));
    }

    #[test]
    fn given_a_failed_partition_when_another_one_asks_should_admit_it() {
        let start = Instant::now();
        let mut pacing = UploadPacing::new(start);
        pacing.upload_failed(namespace(), 7, true, start);

        assert!(!pacing.admit(namespace(), 7, 1, start));
        assert!(pacing.admit(IggyNamespace::new(0, 0, 1), 7, 1, start));
    }

    #[test]
    fn given_a_failed_partition_when_its_slot_holds_a_new_one_should_not_make_that_one_wait() {
        let start = Instant::now();
        let mut pacing = UploadPacing::new(start);
        pacing.upload_failed(namespace(), 7, false, start);

        assert!(pacing.admit(namespace(), 8, 1, start));
        pacing.upload_failed(namespace(), 8, true, start);
        assert!(pacing.admit(namespace(), 8, 1, start + UPLOAD_RETRY_BASE));
    }

    #[test]
    fn given_a_waiting_partition_when_it_asks_should_take_no_tokens() {
        let start = Instant::now();
        let mut pacing = UploadPacing::new(start);
        pacing.upload_failed(namespace(), 7, true, start);

        assert!(!pacing.admit(namespace(), 7, UPLOAD_BURST_BYTES, start));

        assert_eq!(pacing.bucket.tokens(start), i128::from(UPLOAD_BURST_BYTES));
    }

    #[test]
    fn given_a_partition_no_longer_hosted_when_retaining_should_forget_its_wait_and_alarm() {
        let start = Instant::now();
        let mut pacing = UploadPacing::new(start);
        pacing.upload_failed(namespace(), 7, false, start);
        pacing.observe_lag(namespace(), UPLOAD_LAG_ALARM);

        pacing.retain(&[IggyNamespace::new(0, 0, 1)]);

        assert!(pacing.admit(namespace(), 7, 1, start));
        assert_eq!(
            pacing.observe_lag(namespace(), UPLOAD_LAG_ALARM),
            Some(LagChange::Raised)
        );
    }

    #[test]
    fn given_a_lag_over_the_alarm_line_when_it_is_seen_again_should_report_each_crossing_once() {
        let mut pacing = UploadPacing::new(Instant::now());
        let under = UPLOAD_LAG_ALARM.saturating_sub(Duration::from_secs(1));

        assert_eq!(pacing.observe_lag(namespace(), under), None);
        assert_eq!(
            pacing.observe_lag(namespace(), UPLOAD_LAG_ALARM),
            Some(LagChange::Raised)
        );
        assert_eq!(pacing.observe_lag(namespace(), UPLOAD_LAG_ALARM * 2), None);
        assert_eq!(
            pacing.observe_lag(namespace(), under),
            Some(LagChange::Recovered)
        );
        assert_eq!(pacing.observe_lag(namespace(), Duration::ZERO), None);
    }
}
