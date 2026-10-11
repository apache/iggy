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
//! count, so anything the write sites cannot see (hard links, temporary files
//! replaced by rename, logs, other processes, deleted segments) is wrong for
//! at most one sampling interval.

use crossbeam::utils::CachePadded;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The process-wide estimate. It counts from zero until the first sample.
pub static DISK_USAGE: DiskUsage = DiskUsage::new();

#[derive(Debug)]
pub struct DiskUsage {
    // Every shard adds to it, so it gets a cache line of its own.
    used_bytes: CachePadded<AtomicU64>,
    measured: AtomicBool,
}

impl DiskUsage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            used_bytes: CachePadded::new(AtomicU64::new(0)),
            measured: AtomicBool::new(false),
        }
    }

    /// Adds `bytes` that Iggy has just written to, or reserved on, the volume.
    #[inline]
    pub fn charge(&self, bytes: u64) {
        self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Estimated bytes used on the volume: the last sample plus the bytes
    /// charged since.
    #[must_use]
    #[inline]
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
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
    /// The correction is added instead of stored, so a charge that lands after
    /// `measure` returns is kept. A charge that lands while `measure` runs is
    /// counted twice until the next sample, which errs towards a fuller disk.
    /// When `measure` returns `None` the estimate keeps counting from the
    /// previous base.
    pub fn rebase<T>(&self, measure: impl FnOnce() -> Option<(T, u64)>) -> Option<T> {
        let counted = self.used_bytes.load(Ordering::Relaxed);
        let (sample, used_bytes) = measure()?;
        self.used_bytes
            .fetch_add(used_bytes.wrapping_sub(counted), Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
        Some(sample)
    }
}

impl Default for DiskUsage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn given_no_sample_when_charging_should_count_from_zero() {
        let usage = DiskUsage::new();

        usage.charge(3 * MIB);

        assert_eq!(usage.used_bytes(), 3 * MIB);
        assert!(!usage.is_measured());
    }

    #[test]
    fn given_charges_when_rebasing_should_replace_them_with_the_sample() {
        let usage = DiskUsage::new();
        usage.charge(5 * MIB);

        let sample = usage.rebase(|| Some(("sample", 100 * MIB)));

        assert_eq!(sample, Some("sample"));
        assert_eq!(usage.used_bytes(), 100 * MIB);
        assert!(usage.is_measured());
    }

    #[test]
    fn given_a_sample_below_the_estimate_when_rebasing_should_lower_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        usage.charge(40 * MIB);

        usage.rebase(|| Some(((), 90 * MIB)));

        assert_eq!(
            usage.used_bytes(),
            90 * MIB,
            "space freed since the last sample must lower the estimate"
        );
    }

    #[test]
    fn given_a_charge_after_the_sample_when_rebasing_should_keep_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));

        usage.rebase(|| Some(((), 100 * MIB)));
        usage.charge(MIB);

        assert_eq!(usage.used_bytes(), 101 * MIB);
    }

    #[test]
    fn given_a_charge_during_the_sample_when_rebasing_should_count_it_until_the_next_sample() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));

        usage.rebase(|| {
            usage.charge(MIB);
            Some(((), 100 * MIB))
        });

        assert_eq!(
            usage.used_bytes(),
            101 * MIB,
            "a charge the sample may have missed must still be counted"
        );
    }

    #[test]
    fn given_a_failed_sample_when_rebasing_should_keep_counting_from_the_previous_base() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), 100 * MIB)));
        usage.charge(MIB);

        let sample: Option<()> = usage.rebase(|| None);

        assert!(sample.is_none());
        assert_eq!(usage.used_bytes(), 101 * MIB);
    }
}
