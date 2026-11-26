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

//! A run's headline numbers and their change against the previous version on its history line. The
//! shared crate's delta rule gives the verdict, so every page colors a change the same way.

use bench_dashboard_shared::config::Thresholds;
use bench_dashboard_shared::delta::{DeltaVerdict, Metric, ValueRange};
use bench_dashboard_shared::format::runs_text;
use bench_dashboard_shared::history::VersionStep;
use bench_dashboard_shared::labels::{metric_name, metric_value_text};
use bench_dashboard_shared::summary::BenchmarkSummary;

const PERCENT: f64 = 100.0;

/// One headline number of a run, from its main group.
#[derive(Debug, Clone, PartialEq)]
pub struct Headline {
    pub metric: Metric,
    /// `None` when the report has no numbers for the main group.
    pub value: Option<f64>,
    pub change: Option<Change>,
}

/// One metric's change against the previous version.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub verdict: DeltaVerdict,
    /// The change of the median. `None` when the previous version's median is 0.
    pub percent: Option<f64>,
    /// The previous version.
    pub versus: String,
    /// Both ranges with their run counts.
    pub ranges: String,
}

impl Change {
    /// For example `+4.6% vs 0.9.0`.
    pub fn text(&self) -> String {
        match self.percent {
            Some(percent) => format!("{} vs {}", percent_text(percent), self.versus),
            None => format!("vs {}", self.versus),
        }
    }
}

/// The [`BenchmarkSummary::headline_metrics`] of the main group.
pub fn headlines(
    run: &BenchmarkSummary,
    step: Option<&VersionStep<'_>>,
    thresholds: &Thresholds,
) -> Vec<Headline> {
    let group = run.group(run.kind_family.primary_group());
    run.headline_metrics()
        .iter()
        .map(|&metric| Headline {
            metric,
            value: group.map(|group| group.value(metric)),
            change: step.and_then(|step| change(metric, run, step, thresholds)),
        })
        .collect()
}

/// The change of `metric` for the main group of `run`. `None` when the delta rule has nothing to
/// compare, for example the latency of a run without a rate limit.
pub fn change(
    metric: Metric,
    run: &BenchmarkSummary,
    step: &VersionStep<'_>,
    thresholds: &Thresholds,
) -> Option<Change> {
    let delta = thresholds.delta(
        metric,
        run.kind_family.primary_group(),
        &step.base,
        &step.head,
    )?;
    let percent = percent_change(delta.base.median, delta.head.median);
    let value = |value| metric_value_text(metric, value);
    Some(Change {
        verdict: delta.verdict,
        percent,
        versus: step.previous.to_owned(),
        ranges: format!(
            "{}. {}.",
            range_text(step.previous, step.base.len(), &delta.base, value),
            range_text(&run.version, step.head.len(), &delta.head, value)
        ),
    })
}

/// The banner text when a headline number got worse by more than run-to-run noise: which
/// metrics, by how much, and against which version.
pub fn regression_text(headlines: &[Headline]) -> Option<String> {
    let worse: Vec<_> = headlines
        .iter()
        .filter_map(|headline| {
            headline
                .change
                .as_ref()
                .filter(|change| change.verdict == DeltaVerdict::Worse)
                .map(|change| (headline.metric, change))
        })
        .collect();
    let (_, first) = worse.first()?;
    let metrics: Vec<_> = worse
        .iter()
        .map(|(metric, change)| match change.percent {
            Some(percent) => format!("{} {}", metric_name(*metric), percent_text(percent)),
            None => metric_name(*metric).to_owned(),
        })
        .collect();
    Some(format!(
        "Worse than {} by more than run-to-run noise: {}.",
        first.versus,
        metrics.join(", ")
    ))
}

/// The change from `base` to `head` in percent of `base`. `None` when `base` is 0.
pub fn percent_change(base: f64, head: f64) -> Option<f64> {
    (base > 0.0).then(|| (head - base) / base * PERCENT)
}

pub fn percent_text(percent: f64) -> String {
    format!("{percent:+.1}%")
}

/// The runs of one version and their range, for example `0.9.0: 3 runs, 1.20 ms to 1.31 ms, median
/// 1.27 ms`. `value` writes a value in its unit.
pub fn range_text(
    version: &str,
    runs: usize,
    range: &ValueRange,
    value: impl Fn(f64) -> String,
) -> String {
    if runs == 1 {
        return format!("{version}: 1 run, {}", value(range.median));
    }
    format!(
        "{version}: {}, {} to {}, median {}",
        runs_text(runs),
        value(range.min),
        value(range.max),
        value(range.median)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, COMMIT, MACHINE, OLD_RELEASE, RELEASE, THRESHOLDS, build, limited, producer,
        rate_limited, run, with_numbers,
    };
    use bench_dashboard_shared::history::HistoryLine;
    use uuid::Uuid;

    const OTHER_MACHINE: &str = "iggy-ci";
    const OTHER_COMMIT: &str = "5c252f08d1aa7e31";
    /// The commit dates put `COMMIT` first, even though its run started later.
    const COMMIT_DATE: &str = "2026-10-01T12:00:00Z";
    const OTHER_COMMIT_DATE: &str = "2026-10-20T12:00:00Z";

    fn step_of(id: u128, summaries: &[BenchmarkSummary]) -> Option<VersionStep<'_>> {
        let run = summaries
            .iter()
            .find(|summary| summary.uuid == Uuid::from_u128(id))
            .expect("the run is in the list");
        HistoryLine::of(run, summaries).step(run)
    }

    fn ids(runs: &[&BenchmarkSummary]) -> Vec<u128> {
        runs.iter().map(|run| run.uuid.as_u128()).collect()
    }

    fn releases() -> Vec<BenchmarkSummary> {
        vec![
            limited(1, OLD_RELEASE),
            limited(2, RELEASE),
            limited(3, RELEASE),
            limited(4, CANDIDATE),
            run(5, rate_limited(producer()), OTHER_MACHINE, RELEASE),
            run(6, producer(), MACHINE, RELEASE),
        ]
    }

    #[test]
    fn given_releases_when_stepping_back_should_compare_all_runs_of_previous_version() {
        let runs = releases();

        let candidate = step_of(4, &runs).expect("0.9.0 comes before the candidate");
        assert_eq!(candidate.previous, "0.9.0");
        assert_eq!(ids(&candidate.base), [2, 3]);
        assert_eq!(ids(&candidate.head), [4]);

        let release = step_of(3, &runs).expect("0.8.0 comes before 0.9.0");
        assert_eq!(release.previous, "0.8.0");
        assert_eq!(ids(&release.base), [1]);
        assert_eq!(ids(&release.head), [2, 3]);
    }

    #[test]
    fn given_first_version_of_its_line_when_stepping_back_should_find_none() {
        let runs = releases();

        assert!(step_of(1, &runs).is_none());
        assert!(
            step_of(5, &runs).is_none(),
            "another machine is another line"
        );
        assert!(
            step_of(6, &runs).is_none(),
            "another rate limit is another line"
        );
    }

    #[test]
    fn given_builds_when_stepping_back_should_order_them_by_commit_date() {
        let runs = vec![
            limited(1, CANDIDATE),
            build(2, OTHER_COMMIT, OTHER_COMMIT_DATE),
            build(3, COMMIT, COMMIT_DATE),
            build(4, COMMIT, COMMIT_DATE),
        ];

        let later = step_of(2, &runs).expect("an older commit comes first");
        assert_eq!(later.previous, "0.10.0 at 91e020636");
        assert_eq!(ids(&later.base), [3, 4]);
        assert_eq!(ids(&later.head), [2]);

        let earlier = step_of(4, &runs).expect("a pre-release comes before its builds");
        assert_eq!(earlier.previous, "0.10.0-rc.1");
        assert_eq!(ids(&earlier.head), [3, 4]);
    }

    #[test]
    fn given_rate_limited_run_when_listing_headlines_should_give_four_with_changes() {
        let runs = vec![
            with_numbers(limited(1, RELEASE), 800.0, 2.0),
            with_numbers(limited(2, RELEASE), 802.0, 2.1),
            with_numbers(limited(3, CANDIDATE), 700.0, 1.0),
        ];
        let step = step_of(3, &runs);

        let headlines = headlines(&runs[2], step.as_ref(), &THRESHOLDS);

        let shown: Vec<_> = headlines
            .iter()
            .map(|headline| {
                let change = headline.change.as_ref().expect("0.9.0 is the base");
                (
                    metric_name(headline.metric),
                    headline
                        .value
                        .map(|value| metric_value_text(headline.metric, value)),
                    change.verdict,
                    change.text(),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                (
                    "Throughput",
                    Some("700 MB/s".to_owned()),
                    DeltaVerdict::Worse,
                    "-12.6% vs 0.9.0".to_owned()
                ),
                (
                    "P50",
                    Some("0.500 ms".to_owned()),
                    DeltaVerdict::Better,
                    "-51.2% vs 0.9.0".to_owned()
                ),
                (
                    "P99",
                    Some("1.00 ms".to_owned()),
                    DeltaVerdict::Better,
                    "-51.2% vs 0.9.0".to_owned()
                ),
                (
                    "P99.9",
                    Some("1.00 ms".to_owned()),
                    DeltaVerdict::Better,
                    "-51.2% vs 0.9.0".to_owned()
                ),
            ]
        );
        assert_eq!(
            headlines[0]
                .change
                .as_ref()
                .map(|change| change.ranges.as_str()),
            Some(
                "0.9.0: 2 runs, 800 MB/s to 802 MB/s, median 801 MB/s. \
                 0.10.0-rc.1: 1 run, 700 MB/s."
            )
        );
    }

    #[test]
    fn given_run_without_rate_limit_when_listing_headlines_should_give_throughput_only() {
        let runs = vec![
            with_numbers(run(1, producer(), MACHINE, RELEASE), 2041.0, 9.0),
            with_numbers(run(2, producer(), MACHINE, CANDIDATE), 2050.0, 30.0),
        ];
        let step = step_of(2, &runs);

        let headlines = headlines(&runs[1], step.as_ref(), &THRESHOLDS);

        assert_eq!(headlines.len(), 1);
        assert_eq!(headlines[0].metric, Metric::Throughput);
        let change = headlines[0].change.as_ref().expect("0.9.0 is the base");
        assert_eq!(change.verdict, DeltaVerdict::Noise);
        assert_eq!(change.text(), "+0.4% vs 0.9.0");
    }

    #[test]
    fn given_no_previous_version_when_listing_headlines_should_give_no_change() {
        let runs = vec![with_numbers(limited(1, RELEASE), 800.0, 2.0)];

        let headlines = headlines(&runs[0], step_of(1, &runs).as_ref(), &THRESHOLDS);

        assert_eq!(headlines.len(), 4);
        assert!(headlines.iter().all(|headline| headline.change.is_none()));
    }

    #[test]
    fn given_worse_and_better_changes_when_writing_banner_should_name_worse_ones_only() {
        let change = |verdict, percent| Change {
            verdict,
            percent,
            versus: "0.9.0".to_owned(),
            ranges: String::new(),
        };
        let headline = |metric, change| Headline {
            metric,
            value: Some(1.0),
            change,
        };
        let worse = [
            headline(
                Metric::Throughput,
                Some(change(DeltaVerdict::Worse, Some(-12.61))),
            ),
            headline(Metric::P50, Some(change(DeltaVerdict::Better, Some(-50.0)))),
            headline(Metric::P99, Some(change(DeltaVerdict::Noise, Some(30.0)))),
            headline(Metric::P999, Some(change(DeltaVerdict::Worse, Some(512.0)))),
        ];
        let calm = [
            headline(Metric::P99, Some(change(DeltaVerdict::Noise, Some(80.0)))),
            headline(
                Metric::P999,
                Some(change(DeltaVerdict::Flagged, Some(-84.0))),
            ),
        ];

        assert_eq!(
            regression_text(&worse).as_deref(),
            Some(
                "Worse than 0.9.0 by more than run-to-run noise: Throughput -12.6%, P99.9 +512.0%."
            )
        );
        assert_eq!(regression_text(&calm), None);
        assert_eq!(regression_text(&[]), None);
    }
}
