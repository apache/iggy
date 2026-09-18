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
use tracing::warn;

/// Cells the quota is built around. Keeping this near a million puts the replenish period
/// near a microsecond, where the whole-nanosecond period is exact for whole-megabyte rates
/// and the rounding error stays around one part in a thousand.
const TARGET_CELLS_PER_SECOND: u64 = 1_000_000;

/// Burst budget as a slice of a second. The quota tolerates this much credit, so a stall is
/// repaid in a window this long. A whole second of traffic is the wrong answer to a stall:
/// the rate reads as a one-second average and the spike lands on the measured latency.
const BURST_WINDOW_MILLIS: u64 = 100;

pub struct BenchmarkRateLimiter {
    rate_limiter: GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    /// Granularity the quota counts in. One byte for rates below the target cell count.
    cell_bytes: u64,
    /// Cells one call may draw. A larger charge is taken in successive calls.
    burst_cells: NonZeroU32,
}

impl BenchmarkRateLimiter {
    pub fn new(bytes_per_second: IggyByteSize) -> Self {
        let (quota, cell_bytes, burst_cells) = Self::quota_for(bytes_per_second.as_bytes_u64());

        let rate_limiter = GovernorRateLimiter::direct(quota);
        // Spend the burst budget up front, so the first batches of a run are paced like every
        // batch after them instead of going out on credit.
        let _ = rate_limiter.check_n(burst_cells);

        Self {
            rate_limiter,
            cell_bytes,
            burst_cells,
        }
    }

    /// The quota for a rate, with the cell size that makes its period exact.
    ///
    /// `Quota::per_second` derives the period as `1e9 / cells` in whole nanoseconds and
    /// truncates. That overshoots the rate by up to 100% once the period is down to two
    /// nanoseconds, and clamps every rate past 1e9 cells/s to one nanosecond per cell. Sizing
    /// the cell first keeps the period near a microsecond, where the same truncation costs a
    /// thousandth of the rate.
    fn quota_for(bytes_per_second: u64) -> (Quota, u64, NonZeroU32) {
        let bytes_per_second = bytes_per_second.max(1);
        let cell_bytes = (bytes_per_second / TARGET_CELLS_PER_SECOND).max(1);
        let cells_per_second = (bytes_per_second / cell_bytes).max(1);
        let period_ns = (1_000_000_000 / cells_per_second).max(1);
        let burst_cells =
            u32::try_from(cells_per_second * BURST_WINDOW_MILLIS / 1_000).unwrap_or(u32::MAX);
        let burst_cells = NonZeroU32::new(burst_cells).unwrap_or(NonZeroU32::MIN);
        let quota = Quota::with_period(Duration::from_nanos(period_ns))
            .expect("the period is at least one nanosecond")
            .allow_burst(burst_cells);
        (quota, cell_bytes, burst_cells)
    }

    /// Waits until `bytes` of budget are available.
    ///
    /// A charge larger than the burst is taken in successive calls, which together take the
    /// same time as one call would. Asking for more than the burst at once is refused by the
    /// limiter, and a rate below one batch per second asks for exactly that.
    pub async fn wait_until_necessary(&self, bytes: u64) {
        // At least one cell, so even a zero-byte charge passes through the limiter.
        let mut remaining = bytes.div_ceil(self.cell_bytes).max(1);
        while remaining > 0 {
            let chunk = remaining.min(u64::from(self.burst_cells.get()));
            let chunk = NonZeroU32::new(u32::try_from(chunk).unwrap_or(u32::MAX))
                .unwrap_or(NonZeroU32::MIN);
            match self.rate_limiter.until_n_ready(chunk).await {
                Ok(()) => remaining -= u64::from(chunk.get()),
                Err(error) => {
                    // Unreachable while the chunk stays within the burst. Report it and let
                    // the run continue unthrottled rather than taking the actor down.
                    warn!(
                        "Rate limiter refused {chunk} cells, the rest of the run is not throttled. {error}"
                    );
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let (quota, cell_bytes, _) = BenchmarkRateLimiter::quota_for(rate);
            let period_ns = quota.replenish_interval().as_nanos() as f64;
            let replenished_bytes_per_second = 1e9 / period_ns * cell_bytes as f64;
            let error = (replenished_bytes_per_second - rate as f64).abs() / rate as f64;
            assert!(
                error < 0.002,
                "asked for {rate} bytes/s, quota replenishes {replenished_bytes_per_second} bytes/s"
            );
        }
    }
}
