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

use governor::{
    Quota, RateLimiter as GovernorRateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;
use std::time::Duration;

/// Cells the quota is built around. Keeping this near a million puts the replenish period
/// near a microsecond, where the whole-nanosecond period is exact for whole-megabyte rates
/// and the rounding error stays around one part in a thousand.
const TARGET_CELLS_PER_SECOND: u64 = 1_000_000;

/// Default burst window. After a stall, an actor catches up at most this much time, so a batch
/// that takes longer lowers the achieved rate. A whole second of credit would end a stall in a
/// spike far above the rate, and the spike would land on the measured latency. Consumers that
/// report message age pass a larger window, because they must drain the backlog of a stall.
const BURST_WINDOW_MILLIS: u64 = 100;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

pub struct BenchmarkRateLimiter {
    rate_limiter: GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    /// Granularity the quota counts in. One byte for rates below the target cell count.
    cell_bytes: u64,
    /// Cells one call may draw. A larger charge is taken in successive calls.
    burst_cells: NonZeroU32,
    /// Unused bytes in the last paid cell, always less than `cell_bytes`.
    unused_cell_bytes: u64,
}

impl BenchmarkRateLimiter {
    pub fn new(bytes_per_second: IggyByteSize) -> Self {
        Self::with_burst_window(bytes_per_second, Duration::from_millis(BURST_WINDOW_MILLIS))
    }

    pub fn with_burst_window(bytes_per_second: IggyByteSize, burst_window: Duration) -> Self {
        let (quota, cell_bytes, burst_cells) =
            Self::quota_for(bytes_per_second.as_bytes_u64(), burst_window);

        let rate_limiter = GovernorRateLimiter::direct(quota);
        // Spend the burst budget up front, so the first batches of a run are paced like every
        // batch after them instead of going out on credit.
        let _ = rate_limiter.check_n(burst_cells);

        Self {
            rate_limiter,
            cell_bytes,
            burst_cells,
            unused_cell_bytes: 0,
        }
    }

    /// The quota for a rate, with the cell size that makes its period exact.
    ///
    /// `Quota::per_second` derives the period as `1e9 / cells` in whole nanoseconds and
    /// truncates. That overshoots the rate by up to 100% once the period is down to two
    /// nanoseconds, and clamps every rate past 1e9 cells/s to one nanosecond per cell. Sizing
    /// the cell first keeps the period near a microsecond, where the same truncation costs a
    /// thousandth of the rate.
    fn quota_for(bytes_per_second: u64, burst_window: Duration) -> (Quota, u64, NonZeroU32) {
        let bytes_per_second = bytes_per_second.max(1);
        let cell_bytes = (bytes_per_second / TARGET_CELLS_PER_SECOND).max(1);
        let cells_per_second = (bytes_per_second / cell_bytes).max(1);
        let period_ns = (NANOS_PER_SECOND / cells_per_second).max(1);
        let burst_cells = u32::try_from(
            u128::from(cells_per_second) * burst_window.as_nanos() / u128::from(NANOS_PER_SECOND),
        )
        .unwrap_or(u32::MAX);
        let burst_cells = NonZeroU32::new(burst_cells).unwrap_or(NonZeroU32::MIN);
        let quota = Quota::with_period(Duration::from_nanos(period_ns))
            .expect("the period is at least one nanosecond")
            .allow_burst(burst_cells);
        (quota, cell_bytes, burst_cells)
    }

    /// Waits until `bytes` of budget are available.
    ///
    /// The limiter refuses a charge larger than the burst, and a batch with more than one burst
    /// window of data asks for one. Such a charge is taken in successive calls, which together
    /// take the same time as one call would.
    pub async fn wait_until_necessary(&mut self, bytes: u64) {
        let credited_bytes = bytes.min(self.unused_cell_bytes);
        let uncredited_bytes = bytes - credited_bytes;
        // Empty polls still pass through the limiter.
        let mut remaining = if bytes == 0 {
            1
        } else {
            uncredited_bytes.div_ceil(self.cell_bytes)
        };
        while remaining > 0 {
            let chunk = u32::try_from(remaining)
                .ok()
                .and_then(NonZeroU32::new)
                .map_or(self.burst_cells, |cells| cells.min(self.burst_cells));
            self.rate_limiter
                .until_n_ready(chunk)
                .await
                .expect("a chunk never exceeds the burst");
            remaining -= u64::from(chunk.get());
        }

        // A cancelled wait must not grant credit for cells that it did not acquire.
        self.unused_cell_bytes -= credited_bytes;
        let remainder = uncredited_bytes % self.cell_bytes;
        if remainder != 0 {
            self.unused_cell_bytes = self.cell_bytes - remainder;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use tokio::time::{Instant, sleep, timeout};

    const TEST_RATE: u64 = 1_000;
    const CATCH_UP_WINDOW: Duration = Duration::from_secs(1);
    const STALL: Duration = Duration::from_millis(500);
    const CATCH_UP_DEADLINE: Duration = Duration::from_millis(200);
    const TEST_CELL_BYTES: u64 = 1_000;
    const TEST_REPLENISH_PERIOD: Duration = Duration::from_hours(1);

    #[test]
    fn given_fractional_batches_when_charged_should_match_combined_batch() {
        const BATCH_COUNT: u64 = 10;
        for batch_bytes in [100, 128, 999, TEST_CELL_BYTES, TEST_CELL_BYTES + 1] {
            let total_bytes = batch_bytes * BATCH_COUNT;
            let cells = u32::try_from(total_bytes.div_ceil(TEST_CELL_BYTES)).unwrap();
            let mut split = with_available_cells(TEST_CELL_BYTES, cells);
            let mut combined = with_available_cells(TEST_CELL_BYTES, cells);

            for batch in 0..BATCH_COUNT {
                assert_eq!(
                    split.wait_until_necessary(batch_bytes).now_or_never(),
                    Some(()),
                    "batch {batch} of {batch_bytes} bytes must use cumulative cell credit"
                );
            }
            assert_eq!(
                combined.wait_until_necessary(total_bytes).now_or_never(),
                Some(())
            );
            assert!(split.rate_limiter.check().is_err());
            assert!(combined.rate_limiter.check().is_err());
        }
    }

    #[test]
    fn given_partial_cell_credit_when_charging_zero_should_preserve_credit_and_pace() {
        let mut limiter = with_available_cells(TEST_CELL_BYTES, 2);
        for bytes in [1, 0, TEST_CELL_BYTES - 1] {
            assert_eq!(limiter.wait_until_necessary(bytes).now_or_never(), Some(()));
        }
        assert_eq!(limiter.wait_until_necessary(0).now_or_never(), None);
        assert_eq!(limiter.wait_until_necessary(1).now_or_never(), None);
    }

    #[test]
    fn given_maximum_byte_charges_when_pacing_should_not_overflow() {
        const BATCH_COUNT: u32 = 2;
        let (_, cell_bytes, _) = BenchmarkRateLimiter::quota_for(u64::MAX, CATCH_UP_WINDOW);
        let total_bytes = u128::from(u64::MAX) * u128::from(BATCH_COUNT);
        let cells = u32::try_from(total_bytes.div_ceil(u128::from(cell_bytes))).unwrap();
        let mut limiter = with_available_cells(cell_bytes, cells);

        for _ in 0..BATCH_COUNT {
            assert_eq!(
                limiter.wait_until_necessary(u64::MAX).now_or_never(),
                Some(())
            );
        }
        assert!(limiter.rate_limiter.check().is_err());
    }

    #[test]
    fn given_a_cancelled_wait_when_reusing_credit_should_only_spend_paid_bytes() {
        let mut limiter = with_available_cells(TEST_CELL_BYTES, 1);
        assert_eq!(limiter.wait_until_necessary(1).now_or_never(), Some(()));
        assert_eq!(
            limiter
                .wait_until_necessary(TEST_CELL_BYTES + 1)
                .now_or_never(),
            None
        );
        assert_eq!(
            limiter
                .wait_until_necessary(TEST_CELL_BYTES - 1)
                .now_or_never(),
            Some(())
        );
        assert_eq!(limiter.wait_until_necessary(1).now_or_never(), None);
        assert_eq!(
            limiter
                .wait_until_necessary(TEST_CELL_BYTES - 1)
                .now_or_never(),
            None
        );
    }

    #[tokio::test]
    async fn given_consumer_stall_when_catching_up_should_use_banked_credit() {
        let mut limiter =
            BenchmarkRateLimiter::with_burst_window(TEST_RATE.into(), CATCH_UP_WINDOW);
        sleep(STALL).await;

        timeout(
            CATCH_UP_DEADLINE,
            limiter.wait_until_necessary(TEST_RATE / 2),
        )
        .await
        .expect("a consumer must drain half a second of backlog from its accumulated credit");
    }

    #[tokio::test]
    async fn given_oversized_batch_when_pacing_should_wait_for_every_burst() {
        const BURSTS: u64 = 3;
        let start = Instant::now();
        let mut limiter = BenchmarkRateLimiter::new(TEST_RATE.into());
        let bytes = BURSTS * u64::from(limiter.burst_cells.get()) * limiter.cell_bytes;

        timeout(CATCH_UP_WINDOW, limiter.wait_until_necessary(bytes))
            .await
            .expect("an oversized charge must complete in successive bursts");

        assert!(
            start.elapsed() >= Duration::from_millis(BURSTS * BURST_WINDOW_MILLIS),
            "every byte of the oversized batch must be charged"
        );
    }

    #[test]
    #[expect(
        clippy::cast_precision_loss,
        reason = "The rates and periods compared here are well inside f64's exact integer range."
    )]
    fn given_a_rate_when_building_the_quota_should_replenish_at_that_rate() {
        for rate in [
            1_048_576u64,
            4_000_000,
            100_000_000,
            350_000_000,
            2_000_000_000,
        ] {
            let (quota, cell_bytes, _) =
                BenchmarkRateLimiter::quota_for(rate, Duration::from_millis(BURST_WINDOW_MILLIS));
            let period_ns = quota.replenish_interval().as_nanos() as f64;
            let replenished_bytes_per_second = 1e9 / period_ns * cell_bytes as f64;
            let error = (replenished_bytes_per_second - rate as f64).abs() / rate as f64;
            assert!(
                error < 0.002,
                "asked for {rate} bytes/s, quota replenishes {replenished_bytes_per_second} bytes/s"
            );
        }
    }

    fn with_available_cells(cell_bytes: u64, cells: u32) -> BenchmarkRateLimiter {
        let burst_cells = NonZeroU32::new(cells).unwrap();
        let quota = Quota::with_period(TEST_REPLENISH_PERIOD)
            .unwrap()
            .allow_burst(burst_cells);
        BenchmarkRateLimiter {
            rate_limiter: GovernorRateLimiter::direct(quota),
            cell_bytes,
            burst_cells,
            unused_cell_bytes: 0,
        }
    }
}
