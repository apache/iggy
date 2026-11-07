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

//! Estimated usage of the volume holding the data directory.
//!
//! A periodic `statvfs` sample sets the base and the sites that allocate disk
//! space add what they write, so the estimate follows a burst between two
//! samples without a syscall on the write path. Each sample re-bases the
//! estimate, so anything the write sites cannot see (hard links, temporary
//! files replaced by rename, logs, other processes, deleted segments) is wrong
//! for at most one sampling interval.
//!
//! The write sites add to a total of their own that samples never touch, and
//! a sample only moves the base under it. The total then says what Iggy
//! charged, apart from what the volume did, which is how a test checks that
//! every write site charges while the sampler keeps running.
//!
//! A write site charges before it writes, so a sample taken while the write is
//! in flight does not hold its bytes yet. Each charge therefore returns a
//! [`Charge`], and a sample only takes over the charges whose [`Charge`] was
//! dropped, once their writes finished or failed.

use crossbeam::utils::CachePadded;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The process-wide estimate. It counts from zero until the first sample.
pub static DISK_USAGE: DiskUsage = DiskUsage::new();

#[derive(Debug)]
pub struct DiskUsage {
    // Every shard adds to both, so each gets a cache line of its own.
    charged: CachePadded<AtomicU64>,
    /// The part of `charged` whose writes finished or failed.
    settled: CachePadded<AtomicU64>,
    /// The last sample minus what was settled before it. Charges outgrow the
    /// volume once files are deleted, so it wraps below zero and the
    /// estimate adds the charges back with wrapping arithmetic.
    base: AtomicU64,
    measured: AtomicBool,
}

impl DiskUsage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            charged: CachePadded::new(AtomicU64::new(0)),
            settled: CachePadded::new(AtomicU64::new(0)),
            base: AtomicU64::new(0),
            measured: AtomicBool::new(false),
        }
    }

    /// Adds `bytes` that Iggy is about to write to, or has reserved on, the
    /// volume. Hold the returned [`Charge`] until the write finishes or fails.
    #[inline]
    pub fn charge(&self, bytes: u64) -> Charge<'_> {
        if bytes > 0 {
            self.charged.fetch_add(bytes, Ordering::Relaxed);
        }
        Charge { usage: self, bytes }
    }

    /// Estimated bytes used on the volume: the last sample plus the bytes
    /// charged since.
    #[must_use]
    #[inline]
    pub fn used_bytes(&self) -> u64 {
        self.base
            .load(Ordering::Relaxed)
            .wrapping_add(self.charged.load(Ordering::Relaxed))
    }

    /// Every byte charged since the process started. Samples do not move it.
    #[must_use]
    #[inline]
    pub fn charged_bytes(&self) -> u64 {
        self.charged.load(Ordering::Relaxed)
    }

    /// Whether a sample has set the base. Until then [`Self::used_bytes`]
    /// holds only the bytes charged since the process started.
    #[must_use]
    pub fn is_measured(&self) -> bool {
        self.measured.load(Ordering::Acquire)
    }

    /// Runs `measure` and re-bases the estimate on the used bytes it returns,
    /// passing its sample through. Call it from one thread only.
    ///
    /// The base subtracts only the charges settled before `measure` runs,
    /// because their writes are on the volume when it is measured. A charge
    /// whose write is still in flight stays in the estimate, and so does a
    /// charge that lands after `measure` returns. A write that settles while
    /// `measure` runs is counted twice until the next sample, which errs
    /// towards a fuller disk. When `measure` returns `None` the estimate keeps
    /// counting from the previous base.
    pub fn rebase<T>(&self, measure: impl FnOnce() -> Option<(T, u64)>) -> Option<T> {
        // Pairs with the release in `Charge::drop`: every write settled here
        // finished before the volume is measured.
        let settled = self.settled.load(Ordering::Acquire);
        let (sample, used_bytes) = measure()?;
        self.base
            .store(used_bytes.wrapping_sub(settled), Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
        Some(sample)
    }
}

impl Default for DiskUsage {
    fn default() -> Self {
        Self::new()
    }
}

/// Bytes charged before they are written. Dropping it, once the write finished
/// or failed, lets the next sample take them over: a finished write is on the
/// volume, and a failed one allocated only what the volume shows.
#[derive(Debug)]
#[must_use = "a charge settles when dropped, so hold it until the write finishes"]
pub struct Charge<'a> {
    usage: &'a DiskUsage,
    bytes: u64,
}

impl Drop for Charge<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.usage.settled.fetch_add(self.bytes, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn given_no_sample_when_charging_should_count_from_zero() {
        let usage = DiskUsage::new();

        drop(usage.charge(3 * MIB));

        assert_eq!(usage.used_bytes(), 3 * MIB);
        assert!(!usage.is_measured());
    }

    #[test]
    fn given_charges_when_rebasing_should_replace_them_with_the_sample() {
        let usage = DiskUsage::new();
        drop(usage.charge(5 * MIB));

        let sample = usage.rebase(|| Some(("sample", 100 * MIB)));

        assert_eq!(sample, Some("sample"));
        assert_eq!(usage.used_bytes(), 100 * MIB);
        assert!(usage.is_measured());
    }

    #[test]
    fn given_a_sample_below_the_estimate_when_rebasing_should_lower_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        drop(usage.charge(40 * MIB));

        usage.rebase(|| Some(((), 90 * MIB)));

        assert_eq!(
            usage.used_bytes(),
            90 * MIB,
            "space freed since the last sample must lower the estimate"
        );
    }

    #[test]
    fn given_a_charge_from_another_thread_after_the_measurement_when_rebasing_should_keep_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        let measured = Barrier::new(2);
        let charged = Barrier::new(2);

        thread::scope(|scope| {
            scope.spawn(|| {
                measured.wait();
                drop(usage.charge(MIB));
                charged.wait();
            });
            usage.rebase(|| {
                // The volume is measured before the writer charges, so the
                // sample cannot hold its bytes; the base is stored after.
                let sample = 100 * MIB;
                measured.wait();
                charged.wait();
                Some(((), sample))
            });
        });

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "storing the base must not drop a charge that landed after the measurement"
        );
        assert_eq!(usage.charged_bytes(), MIB);
    }

    #[test]
    fn given_a_charge_during_the_sample_when_rebasing_should_count_it_until_the_next_sample() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));

        usage.rebase(|| {
            drop(usage.charge(MIB));
            Some(((), 100 * MIB))
        });

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "a charge the sample may have missed must still be counted"
        );
    }

    #[test]
    fn given_a_write_in_flight_when_rebasing_should_keep_its_charge_until_it_settles() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        let write = usage.charge(MIB);

        // The write has not reached the volume, so the sample does not hold it.
        usage.rebase(|| Some(((), 100 * MIB)));

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "a sample taken while a write is in flight must not drop its charge"
        );

        drop(write);
        usage.rebase(|| Some(((), 101 * MIB)));

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "the first sample that holds a settled write must replace its charge"
        );
    }

    #[test]
    fn given_a_write_in_flight_on_another_thread_when_rebasing_should_keep_its_charge() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        let charged = Barrier::new(2);
        let sampled = Barrier::new(2);

        thread::scope(|scope| {
            scope.spawn(|| {
                let write = usage.charge(MIB);
                charged.wait();
                // The bytes reach the volume only after the sample.
                sampled.wait();
                drop(write);
            });
            charged.wait();
            usage.rebase(|| Some(((), 100 * MIB)));
            sampled.wait();
        });

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "a sample must not drop the charge of a write that was still in flight"
        );
    }

    #[test]
    fn given_a_failed_write_when_its_charge_settles_should_let_the_next_sample_remove_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        let write = usage.charge(MIB);

        // The write failed before allocating anything.
        drop(write);
        usage.rebase(|| Some(((), 100 * MIB)));

        assert_eq!(usage.used_bytes(), 100 * MIB);
        assert_eq!(usage.charged_bytes(), MIB);
    }

    #[test]
    fn given_a_failed_sample_when_rebasing_should_keep_counting_from_the_previous_base() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        drop(usage.charge(MIB));

        let sample: Option<()> = usage.rebase(|| None);

        assert!(sample.is_none());
        assert_eq!(usage.used_bytes(), 101 * MIB);
    }

    #[test]
    fn given_samples_between_charges_when_reading_the_charged_total_should_count_only_charges() {
        let usage = DiskUsage::new();
        drop(usage.charge(5 * MIB));
        usage.rebase(|| Some(((), 100 * MIB)));
        drop(usage.charge(2 * MIB));
        usage.rebase(|| Some(((), 50 * MIB)));

        assert_eq!(usage.charged_bytes(), 7 * MIB);
        assert_eq!(usage.used_bytes(), 50 * MIB);
    }

    #[test]
    fn given_charges_past_u64_max_when_reading_the_charged_total_should_wrap_instead_of_panicking()
    {
        let usage = DiskUsage::new();
        drop(usage.charge(u64::MAX));

        drop(usage.charge(2));

        assert_eq!(
            usage.charged_bytes(),
            1,
            "the total wraps like a counter reset"
        );
    }

    #[test]
    fn given_the_charged_total_wraps_after_a_sample_when_reading_the_estimate_should_keep_the_charges()
     {
        let usage = DiskUsage::new();
        drop(usage.charge(u64::MAX - MIB));
        usage.rebase(|| Some(((), 100 * MIB)));

        drop(usage.charge(2 * MIB));

        assert!(
            usage.charged_bytes() < MIB,
            "the total must have wrapped for this test to mean anything"
        );
        assert_eq!(usage.used_bytes(), 102 * MIB);
    }

    #[test]
    fn given_the_charged_total_wrapped_before_a_sample_when_rebasing_should_count_from_the_sample()
    {
        let usage = DiskUsage::new();
        drop(usage.charge(u64::MAX));
        drop(usage.charge(5 * MIB));

        usage.rebase(|| Some(((), 100 * MIB)));
        drop(usage.charge(MIB));

        assert_eq!(usage.used_bytes(), 101 * MIB);
    }

    #[test]
    fn given_more_charged_than_the_volume_holds_when_rebasing_should_wrap_the_base() {
        let usage = DiskUsage::new();
        drop(usage.charge(30000 * MIB));

        usage.rebase(|| Some(((), 100 * MIB)));
        drop(usage.charge(MIB));

        assert_eq!(usage.used_bytes(), 101 * MIB);
        assert_eq!(usage.charged_bytes(), 30001 * MIB);
    }
}
