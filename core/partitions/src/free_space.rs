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

use server_common::DiskUsage;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The free space a node keeps on its data disk, shared by every partition of
/// the node and by the sampler that measures the disk.
///
/// Reclaim writes before it frees, and a node whose WAL write fails stops. So
/// a primary refuses new messages while the disk is short, and everything
/// that frees space keeps running.
///
/// A sample alone is a second old, and a burst can write through the reserve
/// in less. So the free space is the last sample minus what the disk usage
/// estimate grew by since, which the write sites charge as they write.
///
/// A write is charged when it starts, and a partition accepts messages before
/// that. So the free space also leaves out the pending bytes: accepted, and
/// not written yet.
#[derive(Debug)]
pub struct FreeSpaceReserve {
    reserve_bytes: u64,
    usage: &'static DiskUsage,
    /// Free plus used bytes of the last sample, or `u64::MAX` before the
    /// first. One value, so a reader never pairs the free bytes of one sample
    /// with the used bytes of another.
    sampled_total_bytes: AtomicU64,
    pending_bytes: AtomicU64,
    /// Whether the last sample was short, for the sampler to log a crossing
    /// once. Admission reads the estimate instead.
    sampled_short: AtomicBool,
    refused_requests: AtomicU64,
}

/// What a sample changed, for the sampler to log once per crossing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreeSpaceChange {
    Unchanged,
    FellShort,
    Recovered,
}

impl FreeSpaceReserve {
    /// A reserve of 0 never refuses. `usage` is the estimate of the volume
    /// that the samples describe.
    #[must_use]
    pub const fn new(reserve_bytes: u64, usage: &'static DiskUsage) -> Self {
        Self {
            reserve_bytes,
            usage,
            sampled_total_bytes: AtomicU64::new(u64::MAX),
            pending_bytes: AtomicU64::new(0),
            sampled_short: AtomicBool::new(false),
            refused_requests: AtomicU64::new(0),
        }
    }

    /// Records a sample of the data disk: its free bytes, and the used bytes
    /// that the same measurement re-based `usage` on.
    pub fn observe(&self, free_bytes: u64, used_bytes: u64) -> FreeSpaceChange {
        // `u64::MAX` stays the mark of no sample.
        self.sampled_total_bytes.store(
            free_bytes.saturating_add(used_bytes).min(u64::MAX - 1),
            Ordering::Relaxed,
        );
        let short = free_bytes < self.reserve_bytes;
        match (self.sampled_short.swap(short, Ordering::Relaxed), short) {
            (false, true) => FreeSpaceChange::FellShort,
            (true, false) => FreeSpaceChange::Recovered,
            _ => FreeSpaceChange::Unchanged,
        }
    }

    /// Whether the estimated free space is below the reserve.
    #[must_use]
    pub fn is_short(&self) -> bool {
        self.free_bytes() < self.reserve_bytes
    }

    /// Admits `bytes` of new messages if the reserve still stands once they
    /// are written, and counts them as pending.
    ///
    /// The bytes are added before the check, so partitions on different
    /// shards cannot each see the same room and take it. The caller hands
    /// them back with [`Self::release_pending`] once its WAL counts them.
    #[must_use]
    pub fn admit(&self, bytes: u64) -> bool {
        self.add_pending(bytes);
        let admitted = !self.is_short();
        if !admitted {
            self.release_pending(bytes);
        }
        admitted
    }

    /// Counts a send that a primary refused for the reserve.
    pub fn count_refusal(&self) {
        self.refused_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts bytes that are accepted and will be written to the data disk.
    pub fn add_pending(&self, bytes: u64) {
        self.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Takes back what [`Self::add_pending`] or [`Self::admit`] counted, once
    /// the bytes are written, dropped or counted elsewhere.
    pub fn release_pending(&self, bytes: u64) {
        self.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    #[must_use]
    pub const fn reserve_bytes(&self) -> u64 {
        self.reserve_bytes
    }

    /// The last sample minus what was written since and what is pending, or
    /// `u64::MAX` before the first sample. Space freed since the sample shows
    /// with the next one.
    #[must_use]
    pub fn free_bytes(&self) -> u64 {
        let sampled_total = self.sampled_total_bytes.load(Ordering::Relaxed);
        if sampled_total == u64::MAX {
            return u64::MAX;
        }
        // `observe` requires a re-based estimate, so this is before the
        // first sample too.
        let Some(used) = self.usage.used_bytes() else {
            return u64::MAX;
        };
        sampled_total
            .saturating_sub(used)
            .saturating_sub(self.pending_bytes())
    }

    #[must_use]
    pub fn refused_requests(&self) -> u64 {
        self.refused_requests.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const RESERVE: u64 = 1024;
    const USED: u64 = 1 << 30;

    #[test]
    fn given_no_sample_when_asked_should_not_refuse() {
        static USAGE: DiskUsage = DiskUsage::new();
        let reserve = FreeSpaceReserve::new(RESERVE, &USAGE);

        assert!(reserve.admit(1));
    }

    #[test]
    fn given_free_space_below_the_reserve_when_sampled_should_refuse_until_space_returns() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), USED)));
        let reserve = FreeSpaceReserve::new(RESERVE, &USAGE);

        assert_eq!(reserve.observe(RESERVE, USED), FreeSpaceChange::Unchanged);
        assert!(!reserve.is_short());
        assert_eq!(
            reserve.observe(RESERVE - 1, USED),
            FreeSpaceChange::FellShort
        );
        assert_eq!(reserve.observe(0, USED), FreeSpaceChange::Unchanged);
        assert!(!reserve.admit(0));
        reserve.count_refusal();
        assert_eq!(reserve.refused_requests(), 1);
        assert_eq!(reserve.observe(RESERVE, USED), FreeSpaceChange::Recovered);
        assert!(reserve.admit(0));
        assert_eq!(reserve.refused_requests(), 1);
        assert_eq!(reserve.free_bytes(), RESERVE);
    }

    /// The sampler runs once a second, and a burst can write through the
    /// reserve in less.
    #[test]
    fn given_writes_since_the_sample_when_they_cross_the_reserve_should_refuse_before_the_next_sample()
     {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), USED)));
        let reserve = FreeSpaceReserve::new(RESERVE, &USAGE);
        reserve.observe(RESERVE + 100, USED);

        drop(USAGE.charge(100));
        assert_eq!(reserve.free_bytes(), RESERVE);
        assert!(!reserve.is_short());
        let in_flight = USAGE.charge(1);
        assert!(
            reserve.is_short(),
            "a write still in flight already takes its space"
        );
        drop(in_flight);

        // The next sample holds the writes, so they are not counted twice.
        USAGE.rebase(|| Some(((), USED + 101)));
        assert_eq!(
            reserve.observe(RESERVE - 1, USED + 101),
            FreeSpaceChange::FellShort
        );
        assert_eq!(reserve.free_bytes(), RESERVE - 1);

        // Deleted segments show with the sample that measures them.
        USAGE.rebase(|| Some(((), USED)));
        assert_eq!(
            reserve.observe(RESERVE + 100, USED),
            FreeSpaceChange::Recovered
        );
        assert!(!reserve.is_short());
    }

    /// Accepted messages wait in a WAL queue before their write is charged,
    /// and every partition of the node has such a queue.
    #[test]
    fn given_accepted_bytes_not_written_yet_when_they_would_cross_the_reserve_should_refuse() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), USED)));
        let reserve = FreeSpaceReserve::new(RESERVE, &USAGE);
        reserve.observe(RESERVE + 100, USED);

        assert!(reserve.admit(60));
        assert!(
            !reserve.admit(41),
            "the first send has not been written, and still takes its space"
        );
        assert_eq!(reserve.pending_bytes(), 60, "a refused send takes nothing");
        assert!(reserve.admit(40));
        assert_eq!(reserve.free_bytes(), RESERVE);

        // Written: the charge takes over from the pending bytes.
        drop(USAGE.charge(100));
        reserve.release_pending(100);
        assert_eq!(reserve.free_bytes(), RESERVE);
        assert!(!reserve.admit(1));
    }

    /// Both samples are short once the 200 bytes written since the first are
    /// taken off. The free bytes of the first with the used bytes of the
    /// second are not.
    #[test]
    fn given_a_sample_being_recorded_when_read_from_another_thread_should_never_mix_two_samples() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), USED)));
        drop(USAGE.charge(200));
        let reserve = FreeSpaceReserve::new(RESERVE, &USAGE);
        reserve.observe(RESERVE + 100, USED);

        thread::scope(|scope| {
            let sampler = scope.spawn(|| {
                for _ in 0..2_000_000 {
                    reserve.observe(RESERVE - 100, USED + 200);
                    reserve.observe(RESERVE + 100, USED);
                }
            });
            while !sampler.is_finished() {
                assert_eq!(reserve.free_bytes(), RESERVE - 100);
            }
        });
    }

    #[test]
    fn given_a_reserve_of_zero_when_the_disk_is_full_should_not_refuse() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), USED)));
        let reserve = FreeSpaceReserve::new(0, &USAGE);

        assert_eq!(reserve.observe(0, USED), FreeSpaceChange::Unchanged);
        drop(USAGE.charge(RESERVE));
        assert!(reserve.admit(RESERVE));
    }
}
