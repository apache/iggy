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

//! Everything that the OMB run page draws. A detail is tens of KB, so the server sends it whole,
//! with no downsampling.

use serde::{Deserialize, Serialize};

use crate::omb::result::{OmbQuantile, OmbResult};
use crate::omb::summary::OmbRunSummary;
use crate::omb::{OMB_WINDOW_SECONDS, mebibytes_per_second};

/// One OMB run with its series and quantile curves. An end-to-end or publish delay part is `None`
/// when the summary says the run didn't measure it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmbRunDetail {
    pub summary: OmbRunSummary,
    pub series: OmbSeries,
    pub quantiles: OmbQuantileCurves,
}

/// One value per window, with a step of [`OMB_WINDOW_SECONDS`]. The first value is the end of
/// the first window of the test phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmbSeries {
    pub publish_messages_per_second: Vec<f64>,
    pub publish_mebibytes_per_second: Vec<f64>,
    /// Over all subscriptions.
    pub consume_mebibytes_per_second: Vec<f64>,
    /// Messages sent but not yet received, over all subscriptions.
    pub backlog: Vec<f64>,
    /// Failed sends per second. Empty for a file that doesn't record errors.
    pub publish_errors_per_second: Vec<f64>,
    pub publish_latency_ms: OmbLatencySeries,
    pub end_to_end_latency_ms: Option<OmbLatencySeries>,
    pub publish_delay_latency_us: Option<OmbLatencySeries>,
}

/// The latency percentiles of each window that the run page draws. OMB also records the average,
/// the maximum, P75, P95 and P99.99 per window, which no chart uses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmbLatencySeries {
    pub p50: Vec<f64>,
    pub p99: Vec<f64>,
    pub p999: Vec<f64>,
}

/// The quantile curves of the merged histograms, sorted by percentile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmbQuantileCurves {
    pub publish_latency_ms: Vec<OmbQuantile>,
    pub end_to_end_latency_ms: Option<Vec<OmbQuantile>>,
    pub publish_delay_latency_us: Option<Vec<OmbQuantile>>,
}

impl OmbRunDetail {
    /// `summary` must come from `result`.
    pub fn new(summary: OmbRunSummary, result: &OmbResult) -> Self {
        Self {
            summary,
            series: OmbSeries::from(result),
            quantiles: OmbQuantileCurves::from(result),
        }
    }
}

impl From<&OmbResult> for OmbSeries {
    fn from(result: &OmbResult) -> Self {
        let throughput = |rates: &[f64]| {
            rates
                .iter()
                .map(|rate| mebibytes_per_second(*rate, result.message_size))
                .collect()
        };
        Self {
            publish_messages_per_second: result.publish_rate.clone(),
            publish_mebibytes_per_second: throughput(&result.publish_rate),
            consume_mebibytes_per_second: throughput(&result.consume_rate),
            backlog: as_f64(&result.backlog),
            publish_errors_per_second: result.publish_error_rate.clone(),
            publish_latency_ms: OmbLatencySeries {
                p50: result.publish_latency_50pct.clone(),
                p99: result.publish_latency_99pct.clone(),
                p999: result.publish_latency_999pct.clone(),
            },
            end_to_end_latency_ms: result.has_end_to_end_latency().then(|| OmbLatencySeries {
                p50: result.end_to_end_latency_50pct.clone(),
                p99: result.end_to_end_latency_99pct.clone(),
                p999: result.end_to_end_latency_999pct.clone(),
            }),
            publish_delay_latency_us: result.has_publish_delay_latency().then(|| {
                OmbLatencySeries {
                    p50: as_f64(&result.publish_delay_latency_50pct),
                    p99: as_f64(&result.publish_delay_latency_99pct),
                    p999: as_f64(&result.publish_delay_latency_999pct),
                }
            }),
        }
    }
}

impl OmbSeries {
    /// The end of each window in seconds from the start of the test phase: 10, 20, 30 and on.
    pub fn window_ends(&self) -> Vec<u64> {
        (1..=self.publish_messages_per_second.len() as u64)
            .map(|window| window * OMB_WINDOW_SECONDS)
            .collect()
    }
}

impl From<&OmbResult> for OmbQuantileCurves {
    fn from(result: &OmbResult) -> Self {
        Self {
            publish_latency_ms: result.publish_latency_quantiles(),
            end_to_end_latency_ms: result
                .has_end_to_end_latency()
                .then(|| result.end_to_end_latency_quantiles()),
            publish_delay_latency_us: result
                .has_publish_delay_latency()
                .then(|| result.publish_delay_latency_quantiles()),
        }
    }
}

fn as_f64(values: &[i64]) -> Vec<f64> {
    values.iter().map(|value| *value as f64).collect()
}

#[cfg(test)]
mod tests {
    use crate::omb::test_fixtures::{
        ALL, EDGE8_100_PARTITIONS, NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT, assert_close,
        assert_round_trip,
    };

    #[test]
    fn given_real_run_when_detailed_should_keep_every_window_and_point() {
        let result = EDGE8_100_PARTITIONS.result();
        let detail = EDGE8_100_PARTITIONS.detail();
        let series = &detail.series;

        assert_eq!(detail.summary, EDGE8_100_PARTITIONS.summary());
        assert_eq!(series.publish_messages_per_second.len(), 30);
        assert_eq!(series.window_ends().first(), Some(&10));
        assert_eq!(series.window_ends().last(), Some(&300));
        assert_eq!(series.publish_messages_per_second, result.publish_rate);
        assert_eq!(series.backlog.len(), 30);
        assert_eq!(series.backlog.iter().copied().fold(0.0, f64::max), 439.0);
        assert_eq!(series.publish_latency_ms.p99, result.publish_latency_99pct);
        assert_eq!(
            detail.quantiles.publish_latency_ms.len(),
            result.aggregated_publish_latency_quantiles.len()
        );
        assert_eq!(
            detail
                .quantiles
                .end_to_end_latency_ms
                .as_ref()
                .map(Vec::len),
            Some(14)
        );
        assert_eq!(
            detail
                .quantiles
                .publish_delay_latency_us
                .as_ref()
                .map(Vec::len),
            Some(922)
        );
    }

    #[test]
    fn given_real_run_when_detailed_should_convert_each_window_to_mebibytes() {
        let detail = RC1_DEFAULT.detail();
        let series = &detail.series;

        for (rate, mebibytes) in series
            .publish_messages_per_second
            .iter()
            .zip(&series.publish_mebibytes_per_second)
        {
            assert_close(*mebibytes, rate * 1000.0 / 1_048_576.0);
        }
        let mean = series.publish_mebibytes_per_second.iter().sum::<f64>()
            / series.publish_mebibytes_per_second.len() as f64;
        assert_close(mean, detail.summary.publish_mebibytes_per_second);
    }

    #[test]
    fn given_run_without_consumers_when_detailed_should_leave_end_to_end_out() {
        let detail = NO_CONSUMERS.detail();

        assert_eq!(detail.series.end_to_end_latency_ms, None);
        assert_eq!(detail.quantiles.end_to_end_latency_ms, None);
        assert!(detail.series.publish_delay_latency_us.is_some());
    }

    #[test]
    fn given_run_with_publish_errors_when_detailed_should_keep_error_rate_of_each_window() {
        let detail = PUBLISH_ERRORS.detail();
        let errors = &detail.series.publish_errors_per_second;

        assert_eq!(errors.len(), 18);
        assert!(errors.iter().all(|rate| *rate > 0.0));
        assert_eq!(
            detail
                .series
                .publish_delay_latency_us
                .map(|delay| delay.p99.len()),
            Some(18)
        );
    }

    #[test]
    fn given_every_fixture_when_detailed_should_round_trip() {
        for fixture in ALL {
            assert_round_trip(&fixture.detail());
        }
    }
}
