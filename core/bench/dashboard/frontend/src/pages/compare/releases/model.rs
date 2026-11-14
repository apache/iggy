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

//! Two versions across one official suite: a row for each of the suite's history lines that ran
//! both versions, with the change between the versions' medians. The Overview's change since the
//! last release uses the same parts, so both pages judge a change the same way.

use std::cmp::Ordering;
use std::fmt;

use bench_dashboard_shared::config::{DashboardConfig, SuiteConfig, Thresholds};
use bench_dashboard_shared::delta::{DeltaVerdict, Metric};
use bench_dashboard_shared::identity::{HistoryKey, KindFamily};
use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;

use crate::pages::run::change::{
    Change, HistoryLine, LineVersion, RATE_LIMITED_HEADLINES, VersionStep, change,
};

const SUITE_PARAM: &str = "suite";
const BASE_PARAM: &str = "base";
const HEAD_PARAM: &str = "head";
const PERCENT: f64 = 100.0;
/// A row counts once, under the first verdict in this order that one of its changes has.
const SUMMARY_ORDER: [DeltaVerdict; 4] = [
    DeltaVerdict::Worse,
    DeltaVerdict::Better,
    DeltaVerdict::Flagged,
    DeltaVerdict::Noise,
];

/// The view that a link names. A missing value takes its default.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReleasesQuery {
    /// The name of a suite in the configuration.
    pub suite: Option<String>,
    pub base: Option<String>,
    pub head: Option<String>,
}

/// A version among a suite's runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteVersion {
    pub version: String,
    pub release: bool,
}

/// A link value that isn't in the data. The page falls back to the default.
#[derive(Debug, PartialEq, Eq)]
pub enum NotFound {
    Suite(String),
    Base(String),
    Head(String),
}

/// What the page shows for one link. Built once per load and per link.
#[derive(Debug)]
pub struct ReleasesCompare {
    /// `None` when the configuration has no suites.
    pub suite: Option<SuiteConfig>,
    pub not_found: Vec<NotFound>,
    /// The versions of the suite's runs, newest first.
    pub versions: Vec<SuiteVersion>,
    /// `None` when the suite has no runs.
    pub base: Option<SuiteVersion>,
    pub head: Option<SuiteVersion>,
    /// Empty unless base and head are two different versions.
    pub rows: Vec<ReleaseRow>,
    pub summary: ReleaseSummary,
}

/// One of the suite's history lines that ran both versions.
#[derive(Debug)]
pub struct ReleaseRow {
    /// For example `Pinned producer, 20 producers`.
    pub title: String,
    /// The `workload` label of the runs.
    pub workload: Option<String>,
    pub family: KindFamily,
    /// One per metric of [`RATE_LIMITED_HEADLINES`].
    pub cells: Vec<MetricCell>,
    pub base: VersionRuns,
    pub head: VersionRuns,
    /// The largest regression among the worse changes, measured in thresholds of their metrics.
    /// `None` when no change is worse.
    pub regression: Option<f64>,
}

#[derive(Debug)]
pub struct MetricCell {
    pub metric: Metric,
    /// A run without a rate limit measures throughput only, so its latency isn't measured.
    pub measured: bool,
    /// The medians of each version's runs. `None` when a run lacks the main group.
    pub base: Option<f64>,
    pub head: Option<f64>,
    pub change: Option<Change>,
}

/// One version's runs on a history line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRuns {
    pub count: usize,
    pub newest: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseSummary {
    pub workloads: usize,
    /// The number of rows per verdict, in [`SUMMARY_ORDER`].
    pub counts: [(DeltaVerdict, usize); SUMMARY_ORDER.len()],
}

impl ReleasesQuery {
    /// The first value of a parameter wins, and an empty value counts as missing.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut query = Self::default();
        for (key, value) in pairs {
            let slot = match key.as_str() {
                SUITE_PARAM => &mut query.suite,
                BASE_PARAM => &mut query.base,
                HEAD_PARAM => &mut query.head,
                _ => continue,
            };
            if slot.is_none() && !value.is_empty() {
                *slot = Some(value);
            }
        }
        query
    }

    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        [
            (SUITE_PARAM, &self.suite),
            (BASE_PARAM, &self.base),
            (HEAD_PARAM, &self.head),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key, value.clone()?)))
        .collect()
    }
}

impl ReleasesCompare {
    pub fn new(
        summaries: &[BenchmarkSummary],
        config: &DashboardConfig,
        query: &ReleasesQuery,
    ) -> Self {
        let mut not_found = Vec::new();
        let (suite, unknown) = resolve(
            &config.suites,
            query.suite.as_deref(),
            |suite| suite.name.as_str(),
            config.suites.first(),
        );
        not_found.extend(unknown.map(NotFound::Suite));
        let runs: Vec<&BenchmarkSummary> = summaries
            .iter()
            .filter(|summary| {
                suite.is_some_and(|suite| summary.suite.as_deref() == Some(suite.name.as_str()))
            })
            .collect();
        // The versions from all of the suite's lines are sorted like a history line, the same way
        // the run page orders them.
        let oldest_first: Vec<SuiteVersion> = HistoryLine::from_runs(runs.iter().copied())
            .versions
            .iter()
            .map(SuiteVersion::of)
            .collect();
        let (default_base, default_head) = default_versions(&oldest_first);
        let (base, unknown) = resolve(
            &oldest_first,
            query.base.as_deref(),
            |version| version.version.as_str(),
            default_base,
        );
        not_found.extend(unknown.map(NotFound::Base));
        let (head, unknown) = resolve(
            &oldest_first,
            query.head.as_deref(),
            |version| version.version.as_str(),
            default_head,
        );
        not_found.extend(unknown.map(NotFound::Head));
        let rows = match (base, head) {
            (Some(base), Some(head)) if base != head => {
                rows(&runs, &base.version, &head.version, &config.thresholds)
            }
            _ => Vec::new(),
        };
        Self {
            suite: suite.cloned(),
            not_found,
            base: base.cloned(),
            head: head.cloned(),
            versions: oldest_first.iter().rev().cloned().collect(),
            summary: ReleaseSummary::new(&rows),
            rows,
        }
    }
}

impl SuiteVersion {
    /// A bare version instead of a release tag gives a build the same text as the release. Runs
    /// with that text count as the release, as on a history line, where they share a place.
    fn of(version: &LineVersion<'_>) -> Self {
        Self {
            version: version.version.to_owned(),
            release: version.runs.iter().any(|run| run.release),
        }
    }
}

impl fmt::Display for NotFound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Suite(name) => write!(formatter, "Suite not found: {name}."),
            Self::Base(version) => write!(formatter, "Base version not found: {version}."),
            Self::Head(version) => write!(formatter, "Head version not found: {version}."),
        }
    }
}

impl ReleaseRow {
    fn new(
        base: &LineVersion<'_>,
        head: &LineVersion<'_>,
        thresholds: &Thresholds,
    ) -> Option<Self> {
        let newest_base = *base.runs.last()?;
        let newest_head = *head.runs.last()?;
        let rate_limited = newest_head.workload_key.rate_limit.is_some();
        let step = VersionStep::between(base, head);
        let cells: Vec<MetricCell> = RATE_LIMITED_HEADLINES
            .iter()
            .map(|&metric| {
                let measured = metric == Metric::Throughput || rate_limited;
                MetricCell {
                    metric,
                    measured,
                    base: base.median(metric).filter(|_| measured),
                    head: head.median(metric).filter(|_| measured),
                    change: change(metric, newest_head, &step, thresholds).filter(|_| measured),
                }
            })
            .collect();
        let regression = cells
            .iter()
            .filter_map(|cell| cell.regression(thresholds))
            .max_by(f64::total_cmp);
        Some(Self {
            title: newest_head.title.clone(),
            workload: newest_head.workload_key.workload.clone(),
            family: newest_head.kind_family,
            cells,
            base: VersionRuns {
                count: base.runs.len(),
                newest: newest_base.uuid,
            },
            head: VersionRuns {
                count: head.runs.len(),
                newest: newest_head.uuid,
            },
            regression,
        })
    }

    /// The first verdict in [`SUMMARY_ORDER`] that one of the changes has. A row without a change
    /// has nothing judged, so it counts as not judged.
    fn verdict(&self) -> DeltaVerdict {
        SUMMARY_ORDER
            .into_iter()
            .find(|verdict| {
                self.cells
                    .iter()
                    .filter_map(|cell| cell.change.as_ref())
                    .any(|change| change.verdict == *verdict)
            })
            .unwrap_or(DeltaVerdict::Flagged)
    }

    /// Worse rows first, the largest regression first. The other rows by kind family, then by
    /// title, then by label, so the order never depends on the order of the runs.
    fn order(&self, other: &Self) -> Ordering {
        let worse_first = match (self.regression, other.regression) {
            (Some(own), Some(other)) => other.total_cmp(&own),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        worse_first
            .then_with(|| self.family.cmp(&other.family))
            .then_with(|| self.title.cmp(&other.title))
            .then_with(|| self.workload.cmp(&other.workload))
    }
}

impl MetricCell {
    /// A worse change, measured the way the delta rule measures it: the larger median over the
    /// smaller one, minus one, divided by the metric's threshold. So a throughput regression and a
    /// P99.9 regression rank by how far each goes past its own noise.
    fn regression(&self, thresholds: &Thresholds) -> Option<f64> {
        self.change
            .as_ref()
            .filter(|change| change.verdict == DeltaVerdict::Worse)?;
        let (base, head) = (self.base?, self.head?);
        let (larger, smaller) = (base.max(head), base.min(head));
        Some((larger / smaller - 1.0) / (thresholds.pct(self.metric) / PERCENT))
    }
}

impl ReleaseSummary {
    fn new(rows: &[ReleaseRow]) -> Self {
        Self {
            workloads: rows.len(),
            counts: SUMMARY_ORDER.map(|verdict| {
                let count = rows.iter().filter(|row| row.verdict() == verdict).count();
                (verdict, count)
            }),
        }
    }
}

/// The item that `named` names, else `fallback`. The second value is a name that no item has.
fn resolve<'a, T>(
    items: &'a [T],
    named: Option<&str>,
    name_of: impl Fn(&T) -> &str,
    fallback: Option<&'a T>,
) -> (Option<&'a T>, Option<String>) {
    let Some(name) = named else {
        return (fallback, None);
    };
    match items.iter().find(|item| name_of(item) == name) {
        Some(item) => (Some(item), None),
        None => (fallback, Some(name.to_owned())),
    }
}

/// `versions` is oldest first. The latest release goes against the release before it, a lone
/// release against the newest version after it. Without a release, or when the lone release is
/// the newest version, the newest version goes against the one before it. A single version
/// stands on both sides.
fn default_versions(versions: &[SuiteVersion]) -> (Option<&SuiteVersion>, Option<&SuiteVersion>) {
    let releases: Vec<&SuiteVersion> = versions.iter().filter(|version| version.release).collect();
    match releases.as_slice() {
        [.., before, latest] => (Some(*before), Some(*latest)),
        [release] if versions.last() != Some(*release) => (Some(*release), versions.last()),
        _ => (
            versions.iter().rev().nth(1).or(versions.first()),
            versions.last(),
        ),
    }
}

/// One row per history line of `runs` with runs of both versions, in [`ReleaseRow::order`].
fn rows(
    runs: &[&BenchmarkSummary],
    base: &str,
    head: &str,
    thresholds: &Thresholds,
) -> Vec<ReleaseRow> {
    let mut lines: Vec<(HistoryKey, Vec<&BenchmarkSummary>)> = Vec::new();
    for &run in runs {
        let key = run.history_key();
        match lines.iter_mut().find(|(known, _)| *known == key) {
            Some((_, line)) => line.push(run),
            None => lines.push((key, vec![run])),
        }
    }
    let mut rows: Vec<ReleaseRow> = lines
        .into_iter()
        .filter_map(|(_, line)| {
            let line = HistoryLine::from_runs(line);
            let version = |text: &str| line.versions.iter().find(|version| version.version == text);
            ReleaseRow::new(version(base)?, version(head)?, thresholds)
        })
        .collect();
    rows.sort_by(ReleaseRow::order);
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, COMMIT, MACHINE, OLD_RELEASE, RELEASE, THRESHOLDS, consumer, params, producer,
        rate_limited, run, with_numbers,
    };
    use bench_dashboard_shared::metrics::Badge;
    use bench_report::benchmark_kind::BenchmarkKind;
    use bench_report::params::BenchmarkParams;

    const SUITE: &str = "i4i-single";
    const OTHER_SUITE: &str = "i4i-cluster3";
    const WRITE: &str = "producer-1p@800MB";
    const COMMIT_DATE: &str = "2026-11-20T12:00:00Z";
    const OLD_PRE_RELEASE: &str = "server-0.9.0-rc.1";
    const COMMIT_VERSION: &str = "0.10.0 at 91e020636";
    const THROUGHPUT: f64 = 1000.0;
    const P99: f64 = 1.0;

    /// `params` labeled as a run of `workload` in [`SUITE`].
    fn in_suite(mut params: BenchmarkParams, workload: &str) -> BenchmarkParams {
        params.suite = Some(SUITE.to_owned());
        params.workload = Some(workload.to_owned());
        params
    }

    /// A rate-limited producer run of `workload` in [`SUITE`].
    fn producer_run(
        id: u128,
        workload: &str,
        gitref: &str,
        throughput: f64,
        p99: f64,
    ) -> BenchmarkSummary {
        let labeled = in_suite(rate_limited(producer()), workload);
        with_numbers(run(id, labeled, MACHINE, gitref), throughput, p99)
    }

    /// A run of [`WRITE`] with the same numbers in every version.
    fn write(id: u128, gitref: &str) -> BenchmarkSummary {
        producer_run(id, WRITE, gitref, THROUGHPUT, P99)
    }

    fn commit_build(id: u128) -> BenchmarkSummary {
        let mut labeled = in_suite(rate_limited(producer()), WRITE);
        labeled.gitref_date = Some(COMMIT_DATE.to_owned());
        with_numbers(run(id, labeled, MACHINE, COMMIT), THROUGHPUT, P99)
    }

    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: [SUITE, OTHER_SUITE]
                .map(|name| SuiteConfig {
                    name: name.to_owned(),
                    title: format!("{name} title"),
                })
                .to_vec(),
            headlines: Vec::new(),
            thresholds: THRESHOLDS,
        }
    }

    fn query(suite: Option<&str>, base: Option<&str>, head: Option<&str>) -> ReleasesQuery {
        ReleasesQuery {
            suite: suite.map(str::to_owned),
            base: base.map(str::to_owned),
            head: head.map(str::to_owned),
        }
    }

    fn compare(runs: &[BenchmarkSummary], query: &ReleasesQuery) -> ReleasesCompare {
        ReleasesCompare::new(runs, &config(), query)
    }

    fn defaults(runs: &[BenchmarkSummary]) -> ReleasesCompare {
        compare(runs, &ReleasesQuery::default())
    }

    fn chosen(compare: &ReleasesCompare) -> (Option<&str>, Option<&str>) {
        (version_text(&compare.base), version_text(&compare.head))
    }

    fn version_text(version: &Option<SuiteVersion>) -> Option<&str> {
        version.as_ref().map(|version| version.version.as_str())
    }

    fn workloads(rows: &[ReleaseRow]) -> Vec<&str> {
        rows.iter()
            .filter_map(|row| row.workload.as_deref())
            .collect()
    }

    #[test]
    fn given_two_releases_when_defaulted_should_compare_latest_release_with_one_before() {
        let runs = [
            write(1, OLD_RELEASE),
            write(2, CANDIDATE),
            write(3, RELEASE),
        ];

        let compare = defaults(&runs);

        assert_eq!(chosen(&compare), (Some("0.8.0"), Some("0.9.0")));
        let versions: Vec<_> = compare
            .versions
            .iter()
            .map(|version| (version.version.as_str(), version.release))
            .collect();
        assert_eq!(
            versions,
            [("0.10.0-rc.1", false), ("0.9.0", true), ("0.8.0", true)],
            "newest first in the shared version order"
        );
        assert!(compare.not_found.is_empty());
    }

    #[test]
    fn given_one_release_when_defaulted_should_compare_it_with_newest_version_after_it() {
        let runs = [
            write(1, OLD_PRE_RELEASE),
            write(2, RELEASE),
            commit_build(3),
            write(4, CANDIDATE),
        ];
        let lone_release_newest = [write(1, OLD_PRE_RELEASE), write(2, RELEASE)];

        assert_eq!(
            chosen(&defaults(&runs)),
            (Some("0.9.0"), Some(COMMIT_VERSION)),
            "the commit build of 0.10.0 comes after its candidate"
        );
        assert_eq!(
            chosen(&defaults(&lone_release_newest)),
            (Some("0.9.0-rc.1"), Some("0.9.0"))
        );
    }

    #[test]
    fn given_no_release_when_defaulted_should_compare_newest_version_with_one_before() {
        let runs = [commit_build(1), write(2, CANDIDATE)];

        assert_eq!(
            chosen(&defaults(&runs)),
            (Some("0.10.0-rc.1"), Some(COMMIT_VERSION))
        );
    }

    #[test]
    fn given_one_version_when_defaulted_should_take_it_for_both_and_give_no_rows() {
        let runs = [write(1, RELEASE), write(2, RELEASE)];

        let compare = defaults(&runs);

        assert_eq!(compare.versions.len(), 1);
        assert_eq!(chosen(&compare), (Some("0.9.0"), Some("0.9.0")));
        assert!(compare.rows.is_empty());
        assert_eq!(compare.summary.workloads, 0);
    }

    #[test]
    fn given_unknown_suite_when_resolved_should_name_it_and_take_first_suite() {
        let mut elsewhere = write(2, CANDIDATE);
        elsewhere.suite = Some(OTHER_SUITE.to_owned());
        let mut unofficial = write(3, OLD_RELEASE);
        unofficial.suite = None;
        let runs = [write(1, RELEASE), elsewhere, unofficial];

        let compare = compare(&runs, &query(Some("atlas-single"), None, None));

        assert_eq!(
            compare.suite.as_ref().map(|suite| suite.name.as_str()),
            Some(SUITE)
        );
        assert_eq!(
            compare.not_found,
            [NotFound::Suite("atlas-single".to_owned())]
        );
        assert_eq!(
            compare.not_found[0].to_string(),
            "Suite not found: atlas-single."
        );
        assert_eq!(compare.versions.len(), 1, "runs of other suites stay out");
    }

    #[test]
    fn given_unknown_versions_when_resolved_should_name_them_and_take_defaults() {
        let runs = [
            write(1, OLD_RELEASE),
            write(2, RELEASE),
            write(3, CANDIDATE),
        ];

        let unknown = compare(&runs, &query(Some(SUITE), Some("0.7.0"), Some("1.0.0")));
        let known_base = compare(&runs, &query(None, Some("0.10.0-rc.1"), None));

        assert_eq!(chosen(&unknown), (Some("0.8.0"), Some("0.9.0")));
        assert_eq!(
            unknown.not_found,
            [
                NotFound::Base("0.7.0".to_owned()),
                NotFound::Head("1.0.0".to_owned())
            ]
        );
        assert_eq!(
            chosen(&known_base),
            (Some("0.10.0-rc.1"), Some("0.9.0")),
            "a missing head takes its default, whatever the base"
        );
        assert!(known_base.not_found.is_empty());
    }

    #[test]
    fn given_lines_when_compared_should_keep_lines_with_runs_of_both_versions() {
        let read = |id, gitref| {
            let labeled = in_suite(rate_limited(consumer()), "consumer-1c@800MB");
            with_numbers(run(id, labeled, MACHINE, gitref), THROUGHPUT, P99)
        };
        let runs = [
            producer_run(1, WRITE, RELEASE, 800.0, 2.0),
            producer_run(2, WRITE, RELEASE, 802.0, 2.2),
            producer_run(3, WRITE, CANDIDATE, 700.0, 1.0),
            read(4, RELEASE),
            producer_run(5, "producer-1p@500MB", CANDIDATE, THROUGHPUT, P99),
        ];

        let compare = defaults(&runs);

        let [row] = compare.rows.as_slice() else {
            panic!("one line ran both versions");
        };
        assert_eq!(row.title, "Pinned producer, 1 producer");
        assert_eq!(row.workload.as_deref(), Some(WRITE));
        assert_eq!(row.family, KindFamily::Producer);
        assert_eq!(
            (row.base, row.head),
            (
                VersionRuns {
                    count: 2,
                    newest: Uuid::from_u128(2)
                },
                VersionRuns {
                    count: 1,
                    newest: Uuid::from_u128(3)
                }
            )
        );
        let cells: Vec<_> = row
            .cells
            .iter()
            .map(|cell| {
                let change = cell.change.as_ref().expect("both versions have numbers");
                (
                    cell.metric,
                    cell.base,
                    cell.head,
                    change.verdict,
                    change.text(),
                )
            })
            .collect();
        assert_eq!(
            cells,
            [
                (
                    Metric::Throughput,
                    Some(801.0),
                    Some(700.0),
                    DeltaVerdict::Worse,
                    "-12.6% vs 0.9.0".to_owned()
                ),
                (
                    Metric::P50,
                    Some(1.05),
                    Some(0.5),
                    DeltaVerdict::Better,
                    "-52.4% vs 0.9.0".to_owned()
                ),
                (
                    Metric::P99,
                    Some(2.1),
                    Some(1.0),
                    DeltaVerdict::Better,
                    "-52.4% vs 0.9.0".to_owned()
                ),
                (
                    Metric::P999,
                    Some(2.1),
                    Some(1.0),
                    DeltaVerdict::Better,
                    "-52.4% vs 0.9.0".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn given_line_without_rate_limit_when_compared_should_measure_throughput_only() {
        let uncapped = |id, gitref, throughput| {
            let labeled = in_suite(producer(), "producer-1p");
            with_numbers(run(id, labeled, MACHINE, gitref), throughput, P99)
        };
        let runs = [uncapped(1, RELEASE, 2041.0), uncapped(2, CANDIDATE, 2050.0)];

        let compare = defaults(&runs);

        let [row] = compare.rows.as_slice() else {
            panic!("the uncapped line ran both versions");
        };
        let throughput = &row.cells[0];
        assert!(throughput.measured);
        assert_eq!(
            (throughput.base, throughput.head),
            (Some(2041.0), Some(2050.0))
        );
        assert_eq!(
            throughput.change.as_ref().map(|change| change.verdict),
            Some(DeltaVerdict::Noise)
        );
        for latency in &row.cells[1..] {
            assert!(!latency.measured, "{:?}", latency.metric);
            assert_eq!((latency.base, latency.head), (None, None));
            assert_eq!(latency.change, None);
        }
    }

    #[test]
    fn given_worse_and_other_rows_when_ordered_should_rank_regressions_by_threshold() {
        // Throughput and P99 of the release, then of the candidate.
        let line =
            |id, actors: BenchmarkParams, workload, release: (f64, f64), candidate: (f64, f64)| {
                let labeled = in_suite(rate_limited(actors), workload);
                [
                    with_numbers(
                        run(id, labeled.clone(), MACHINE, RELEASE),
                        release.0,
                        release.1,
                    ),
                    with_numbers(
                        run(id + 1, labeled, MACHINE, CANDIDATE),
                        candidate.0,
                        candidate.1,
                    ),
                ]
            };
        let four_producers = params(BenchmarkKind::PinnedProducer, 4, 0);
        let runs: Vec<_> = [
            line(1, consumer(), "read-better", (1000.0, 2.0), (1100.0, 1.0)),
            line(
                3,
                four_producers,
                "four-noise",
                (1000.0, 1.0),
                (1000.0, 1.0),
            ),
            line(5, producer(), "p99-twice", (1000.0, 1.0), (1000.0, 2.0)),
            line(7, producer(), "one-noise", (1000.0, 1.0), (1000.0, 1.0)),
            line(
                9,
                producer(),
                "throughput-tenth",
                (1000.0, 1.0),
                (900.0, 1.0),
            ),
        ]
        .into_iter()
        .flatten()
        .collect();

        let compare = defaults(&runs);

        assert_eq!(
            workloads(&compare.rows),
            [
                "throughput-tenth",
                "p99-twice",
                "one-noise",
                "four-noise",
                "read-better"
            ],
            "1000 over 900 MB/s is 3.7 thresholds of 3%, 2 over 1 ms is 1.67 thresholds of 60%; \
             then producers before consumers, 1 producer before 4"
        );
        let regressions: Vec<_> = compare
            .rows
            .iter()
            .map(|row| row.regression.map(|value| (value * 100.0).round() / 100.0))
            .collect();
        assert_eq!(regressions, [Some(3.7), Some(1.67), None, None, None]);
    }

    #[test]
    fn given_rows_when_summarized_should_count_each_by_strongest_verdict() {
        // Throughput and P99 of the release, then of the candidate.
        let line = |id, workload, release: (f64, f64), candidate: (f64, f64)| {
            [
                producer_run(id, workload, RELEASE, release.0, release.1),
                producer_run(id + 1, workload, CANDIDATE, candidate.0, candidate.1),
            ]
        };
        let mut flagged = line(7, "flagged", (1000.0, 1.0), (1000.0, 3.0));
        flagged[1].badges = vec![Badge::UnevenLoad];
        let mut no_numbers = line(9, "no-numbers", (1000.0, 1.0), (1000.0, 1.0));
        for summary in &mut no_numbers {
            summary.groups.clear();
        }
        let runs: Vec<_> = [
            line(1, "worse-and-better", (1000.0, 2.0), (900.0, 1.0)),
            line(3, "better", (1000.0, 2.0), (1000.0, 1.0)),
            line(5, "noise", (1000.0, 1.0), (1001.0, 1.1)),
            flagged,
            no_numbers,
        ]
        .into_iter()
        .flatten()
        .collect();

        let summary = defaults(&runs).summary;

        assert_eq!(
            summary,
            ReleaseSummary {
                workloads: 5,
                counts: [
                    (DeltaVerdict::Worse, 1),
                    (DeltaVerdict::Better, 1),
                    (DeltaVerdict::Flagged, 2),
                    (DeltaVerdict::Noise, 1),
                ],
            }
        );
    }

    #[test]
    fn given_link_pairs_when_parsed_should_take_first_value_and_write_back() {
        let pairs = [
            ("base", ""),
            ("suite", SUITE),
            ("base", "0.9.0"),
            ("head", "0.10.0 at 91e020636"),
            ("head", "0.8.0"),
            ("runs", "a,b"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()));

        let parsed = ReleasesQuery::from_pairs(pairs);

        assert_eq!(
            parsed,
            query(Some(SUITE), Some("0.9.0"), Some("0.10.0 at 91e020636"))
        );
        let written: Vec<_> = parsed
            .to_pairs()
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect();
        assert_eq!(ReleasesQuery::from_pairs(written), parsed);
        assert_eq!(
            query(Some(SUITE), None, None).to_pairs(),
            [("suite", SUITE.to_owned())]
        );
    }
}
