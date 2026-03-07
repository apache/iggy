/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#![allow(clippy::cast_precision_loss)]

use super::individual::calculate_percentile;
use bench_report::latency_distribution::{
    DistributionPercentiles, HistogramBin, LatencyDistribution, LogNormalParams,
};

const NUM_BINS: usize = 50;

/// Upper range is clamped at P99 * this factor to focus the chart on
/// the interesting region while still showing the tail onset.
const RANGE_FACTOR: f64 = 1.5;

/// Builds a `LatencyDistribution` from pre-sorted positive latency samples (ms).
///
/// The histogram range is clamped to [min, P99 * 1.5] with linear bins.
/// This focuses on the region where the distribution shape is visible,
/// matching how latency distributions are typically visualized.
pub fn compute_latency_distribution(sorted_latencies_ms: &[f64]) -> LatencyDistribution {
    let percentiles = DistributionPercentiles {
        p05_ms: calculate_percentile(sorted_latencies_ms, 5.0),
        p50_ms: calculate_percentile(sorted_latencies_ms, 50.0),
        p95_ms: calculate_percentile(sorted_latencies_ms, 95.0),
        p99_ms: calculate_percentile(sorted_latencies_ms, 99.0),
    };
    let log_normal_params = estimate_log_normal(sorted_latencies_ms);
    let bins = build_histogram(sorted_latencies_ms, percentiles.p99_ms);

    LatencyDistribution {
        bins,
        log_normal_params,
        percentiles,
    }
}

/// Builds a histogram with uniform bin widths over `[min, P99 * RANGE_FACTOR]`.
///
/// Samples beyond the upper limit are counted into the last bin.
/// `Density = count / (total_samples * bin_width)`, so the histogram area
/// sums to the fraction of data within the displayed range (~0.99).
fn build_histogram(sorted: &[f64], p99: f64) -> Vec<HistogramBin> {
    let min_val = sorted[0];
    let max_val = (p99 * RANGE_FACTOR).min(sorted[sorted.len() - 1]);

    let range = max_val - min_val;
    if range <= 0.0 {
        return vec![HistogramBin {
            edge_ms: min_val,
            density: 1.0,
        }];
    }

    let bin_width = range / NUM_BINS as f64;
    let n = sorted.len() as f64;
    let mut bins = Vec::with_capacity(NUM_BINS);
    let mut sorted_idx = 0;

    for i in 0..NUM_BINS {
        let edge = (i as f64).mul_add(bin_width, min_val);
        let upper = if i == NUM_BINS - 1 {
            f64::INFINITY
        } else {
            edge + bin_width
        };

        let mut count = 0u64;
        while sorted_idx < sorted.len() && sorted[sorted_idx] < upper {
            count += 1;
            sorted_idx += 1;
        }

        let density = count as f64 / (n * bin_width);
        bins.push(HistogramBin {
            edge_ms: edge,
            density,
        });
    }

    bins
}

fn estimate_log_normal(sorted: &[f64]) -> LogNormalParams {
    let positive: Vec<f64> = sorted.iter().copied().filter(|&x| x > 0.0).collect();

    if positive.is_empty() {
        return LogNormalParams {
            mu: 0.0,
            sigma: 0.0,
        };
    }

    let n = positive.len() as f64;
    let ln_sum: f64 = positive.iter().map(|x| x.ln()).sum();
    let mu = ln_sum / n;

    let variance = positive
        .iter()
        .map(|x| {
            let diff = x.ln() - mu;
            diff * diff
        })
        .sum::<f64>()
        / n;

    LogNormalParams {
        mu,
        sigma: variance.sqrt(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_density_reasonable() {
        let mut samples: Vec<f64> = (1..=10_000).map(|i| f64::from(i) * 0.1).collect();
        samples.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let dist = compute_latency_distribution(&samples);

        assert_eq!(dist.bins.len(), NUM_BINS);

        let bin_width = dist.bins[1].edge_ms - dist.bins[0].edge_ms;
        let total_area: f64 = dist.bins.iter().map(|b| b.density * bin_width).sum();

        // Area should be close to 1.0 (will be slightly > 1.0 because
        // the last bin catches tail samples beyond the clipped range)
        assert!(
            total_area > 0.9 && total_area < 1.2,
            "Histogram area should be ~1.0, got {total_area}"
        );
    }

    #[test]
    fn log_normal_params_reasonable() {
        let samples: Vec<f64> = (1..=1000).map(f64::from).collect();
        let dist = compute_latency_distribution(&samples);

        assert!(dist.log_normal_params.mu > 0.0);
        assert!(dist.log_normal_params.sigma > 0.0);
    }

    #[test]
    fn percentiles_ordered() {
        let samples: Vec<f64> = (1..=10_000).map(|i| f64::from(i) * 0.01).collect();
        let dist = compute_latency_distribution(&samples);

        assert!(dist.percentiles.p05_ms < dist.percentiles.p50_ms);
        assert!(dist.percentiles.p50_ms < dist.percentiles.p95_ms);
        assert!(dist.percentiles.p95_ms < dist.percentiles.p99_ms);
    }

    #[test]
    fn single_value_distribution() {
        let samples = vec![5.0; 100];
        let dist = compute_latency_distribution(&samples);

        assert_eq!(dist.bins.len(), 1);
        assert!((dist.percentiles.p05_ms - 5.0).abs() < f64::EPSILON);
        assert!((dist.percentiles.p99_ms - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn skewed_distribution_bins_cover_range() {
        let mut samples = Vec::new();
        for i in 1..=9000 {
            samples.push(f64::from(i).mul_add(0.001, 0.5));
        }
        for i in 1..=1000 {
            samples.push(f64::from(i).mul_add(0.1, 10.0));
        }
        samples.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let dist = compute_latency_distribution(&samples);

        // Most bins should have non-zero density since we clip at P99*1.5
        let nonzero_bins = dist.bins.iter().filter(|b| b.density > 0.0).count();
        assert!(
            nonzero_bins > NUM_BINS / 3,
            "Expected >33% non-zero bins, got {nonzero_bins}/{NUM_BINS}"
        );
    }
}
