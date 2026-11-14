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

//! The delta rule decides whether a metric changed between two versions by more than run-to-run
//! noise. Every page that shows a delta uses it.

use bench_report::group_metrics_kind::GroupMetricsKind;
use serde::{Deserialize, Serialize};

use crate::config::Thresholds;
use crate::summary::BenchmarkSummary;

const PERCENT: f64 = 100.0;

/// A metric that gets a delta across versions. Min, max, average, standard deviation and P99.99
/// don't compare across versions, so they have no variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Throughput,
    P50,
    P90,
    P95,
    P99,
    P999,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaVerdict {
    Better,
    Worse,
    /// Within run-to-run noise: not above the threshold, or overlapping ranges. A badge doesn't
    /// change this verdict.
    Noise,
    /// Beyond run-to-run noise, but a run of either version has a badge, so the change isn't judged
    /// better or worse.
    Flagged,
}

/// The median and range of one metric over one version's runs.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ValueRange {
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Delta {
    pub verdict: DeltaVerdict,
    pub base: ValueRange,
    pub head: ValueRange,
}

impl Metric {
    const fn is_latency(self) -> bool {
        !matches!(self, Self::Throughput)
    }
}

impl Thresholds {
    /// Compares two versions' runs on one history line. Returns `None` if either side has no runs
    /// or a run lacks the group. A latency metric also returns `None` if a run has no rate limit,
    /// because that run only measures throughput.
    pub fn delta(
        &self,
        metric: Metric,
        group: GroupMetricsKind,
        base: &[&BenchmarkSummary],
        head: &[&BenchmarkSummary],
    ) -> Option<Delta> {
        let runs = || base.iter().chain(head);
        if metric.is_latency() && runs().any(|run| run.workload_key.rate_limit.is_none()) {
            return None;
        }
        let base_range = ValueRange::of(base, metric, group)?;
        let head_range = ValueRange::of(head, metric, group)?;
        let larger = base_range.median.max(head_range.median);
        let smaller = base_range.median.min(head_range.median);
        let beyond_noise = larger > smaller * (1.0 + self.pct(metric) / PERCENT)
            && !base_range.overlaps(&head_range);
        let has_badge = runs().any(|run| !run.badges.is_empty());
        let improved = if metric.is_latency() {
            head_range.median < base_range.median
        } else {
            head_range.median > base_range.median
        };
        let verdict = match (beyond_noise, has_badge, improved) {
            (false, _, _) => DeltaVerdict::Noise,
            (true, true, _) => DeltaVerdict::Flagged,
            (true, false, true) => DeltaVerdict::Better,
            (true, false, false) => DeltaVerdict::Worse,
        };
        Some(Delta {
            verdict,
            base: base_range,
            head: head_range,
        })
    }

    /// The threshold for `metric`, in percent.
    pub const fn pct(&self, metric: Metric) -> f64 {
        match metric {
            Metric::Throughput => self.throughput_pct,
            Metric::P50 => self.p50_pct,
            Metric::P90 => self.p90_pct,
            Metric::P95 => self.p95_pct,
            Metric::P99 => self.p99_pct,
            Metric::P999 => self.p999_pct,
        }
    }
}

impl ValueRange {
    /// `None` when `runs` is empty or a run lacks the group.
    pub fn of(runs: &[&BenchmarkSummary], metric: Metric, group: GroupMetricsKind) -> Option<Self> {
        let mut values = runs
            .iter()
            .map(|run| run.group(group).map(|metrics| metrics.value(metric)))
            .collect::<Option<Vec<_>>>()?;
        values.sort_by(f64::total_cmp);
        let (&min, &max) = (values.first()?, values.last()?);
        let middle = values.len() / 2;
        let median = if values.len() % 2 == 0 {
            (values[middle - 1] + values[middle]) / 2.0
        } else {
            values[middle]
        };
        Some(Self { median, min, max })
    }

    /// A single run is a range of one value. Touching ranges overlap.
    fn overlaps(&self, other: &Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Badge;
    use crate::test_fixtures::{CLUSTER_RUN, THRESHOLDS, report};

    const RATE_LIMITED_THROUGHPUT: f64 = 784.0;
    const LATENCY_MS: f64 = 1.0;

    fn run(throughput: f64, p99: f64) -> BenchmarkSummary {
        let mut summary = BenchmarkSummary::new(&report(CLUSTER_RUN), &THRESHOLDS);
        let producers = &mut summary.groups[0];
        producers.throughput_megabytes_per_second = throughput;
        producers.p99_latency_ms = p99;
        summary
    }

    fn throughputs(values: &[f64]) -> Vec<BenchmarkSummary> {
        values.iter().map(|&value| run(value, LATENCY_MS)).collect()
    }

    fn p99s(values: &[f64]) -> Vec<BenchmarkSummary> {
        values
            .iter()
            .map(|&value| run(RATE_LIMITED_THROUGHPUT, value))
            .collect()
    }

    fn delta(
        metric: Metric,
        base: &[BenchmarkSummary],
        head: &[BenchmarkSummary],
    ) -> Option<Delta> {
        let base: Vec<_> = base.iter().collect();
        let head: Vec<_> = head.iter().collect();
        THRESHOLDS.delta(metric, GroupMetricsKind::Producers, &base, &head)
    }

    fn verdict(
        metric: Metric,
        base: &[BenchmarkSummary],
        head: &[BenchmarkSummary],
    ) -> DeltaVerdict {
        delta(metric, base, head)
            .expect("both sides have rate-limited runs with producers")
            .verdict
    }

    #[test]
    fn given_ranges_apart_by_less_than_threshold_when_compared_should_stay_noise() {
        let delta = delta(
            Metric::Throughput,
            &throughputs(&[100.0, 100.1, 100.2]),
            &throughputs(&[100.3, 100.4, 100.5]),
        )
        .expect("both sides have producers");

        assert_eq!(delta.verdict, DeltaVerdict::Noise);
        assert_eq!(
            delta.base,
            ValueRange {
                median: 100.1,
                min: 100.0,
                max: 100.2
            }
        );
        assert_eq!(
            delta.head,
            ValueRange {
                median: 100.4,
                min: 100.3,
                max: 100.5
            }
        );
    }

    #[test]
    fn given_one_run_against_three_when_compared_should_follow_range_rule() {
        let single = throughputs(&[100.0]);

        assert_eq!(
            verdict(
                Metric::Throughput,
                &single,
                &throughputs(&[104.0, 105.0, 106.0])
            ),
            DeltaVerdict::Better
        );
        assert_eq!(
            verdict(
                Metric::Throughput,
                &single,
                &throughputs(&[100.0, 105.0, 106.0])
            ),
            DeltaVerdict::Noise
        );
        assert_eq!(
            verdict(
                Metric::Throughput,
                &throughputs(&[104.0, 105.0, 106.0]),
                &single
            ),
            DeltaVerdict::Worse
        );
    }

    #[test]
    fn given_latency_change_of_1_7x_when_compared_should_color_both_directions() {
        assert_eq!(
            verdict(Metric::P99, &p99s(&[10.0]), &p99s(&[17.0])),
            DeltaVerdict::Worse
        );
        assert_eq!(
            verdict(Metric::P99, &p99s(&[17.0]), &p99s(&[10.0])),
            DeltaVerdict::Better
        );
        assert_eq!(
            verdict(Metric::P99, &p99s(&[10.0]), &p99s(&[15.0])),
            DeltaVerdict::Noise
        );
    }

    #[test]
    fn given_run_with_badge_when_change_beyond_noise_should_be_flagged() {
        let mut head = p99s(&[17.0, 18.0]);
        head[1].badges.push(Badge::UnevenLoad);
        let mut base = throughputs(&[100.0]);
        base[0].badges.push(Badge::BelowTargetRate);

        assert_eq!(
            verdict(Metric::P99, &p99s(&[10.0]), &head),
            DeltaVerdict::Flagged
        );
        assert_eq!(
            verdict(Metric::Throughput, &base, &throughputs(&[90.0])),
            DeltaVerdict::Flagged
        );
    }

    #[test]
    fn given_run_with_badge_when_change_within_noise_should_stay_noise() {
        let mut below_threshold = p99s(&[15.0, 15.5]);
        below_threshold[0].badges.push(Badge::UnevenLoad);
        let mut overlapping = p99s(&[10.0, 30.0]);
        overlapping[1].badges.push(Badge::UnevenLoad);

        assert_eq!(
            verdict(Metric::P99, &p99s(&[10.0]), &below_threshold),
            DeltaVerdict::Noise
        );
        assert_eq!(
            verdict(Metric::P99, &p99s(&[10.0]), &overlapping),
            DeltaVerdict::Noise
        );
    }

    #[test]
    fn given_run_without_rate_limit_when_compared_should_give_throughput_delta_only() {
        let mut base = p99s(&[10.0]);
        let mut head = p99s(&[17.0]);
        for run in base.iter_mut().chain(&mut head) {
            run.workload_key.rate_limit = None;
        }

        for metric in [
            Metric::P50,
            Metric::P90,
            Metric::P95,
            Metric::P99,
            Metric::P999,
        ] {
            assert_eq!(delta(metric, &base, &head), None, "{metric:?}");
        }
        assert!(delta(Metric::Throughput, &base, &head).is_some());
    }

    #[test]
    fn given_even_number_of_runs_when_compared_should_take_mean_of_middle_pair() {
        let delta = delta(
            Metric::Throughput,
            &throughputs(&[102.0, 100.0]),
            &throughputs(&[140.0, 110.0, 130.0, 120.0]),
        )
        .expect("both sides have producers");

        assert_eq!(delta.base.median, 101.0);
        assert_eq!(delta.head.median, 125.0);
        assert_eq!(delta.verdict, DeltaVerdict::Better);
    }

    #[test]
    fn given_missing_group_or_no_runs_when_compared_should_give_none() {
        let runs = throughputs(&[100.0]);
        let runs: Vec<_> = runs.iter().collect();

        assert_eq!(
            THRESHOLDS.delta(
                Metric::Throughput,
                GroupMetricsKind::Consumers,
                &runs,
                &runs
            ),
            None
        );
        assert_eq!(
            THRESHOLDS.delta(Metric::Throughput, GroupMetricsKind::Producers, &[], &runs),
            None
        );
    }

    #[test]
    fn given_metric_outside_delta_rule_when_parsed_should_not_exist() {
        for name in ["min", "max", "avg", "average", "std_dev", "p9999"] {
            assert!(
                serde_json::from_value::<Metric>(name.into()).is_err(),
                "{name}"
            );
        }
        let group = serde_json::to_value(&run(RATE_LIMITED_THROUGHPUT, LATENCY_MS).groups[0])
            .expect("group metrics serialize");
        let mut fields: Vec<_> = group
            .as_object()
            .expect("group metrics serialize to an object")
            .keys()
            .cloned()
            .collect();
        fields.sort();

        assert_eq!(
            fields,
            [
                "kind",
                "p50_latency_ms",
                "p90_latency_ms",
                "p95_latency_ms",
                "p999_latency_ms",
                "p99_latency_ms",
                "throughput_megabytes_per_second",
                "throughput_messages_per_second",
                "time_spread_pct",
            ]
        );
    }
}
