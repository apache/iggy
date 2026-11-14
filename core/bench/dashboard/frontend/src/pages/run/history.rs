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

//! The History section: every run of a run's workload on its machine and topology, by version. The
//! medians are the same ones that the headline number changes compare.

use bench_dashboard_shared::delta::{Metric, ValueRange};
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::group_metrics_kind::GroupMetricsKind;
use uuid::Uuid;
use yew::prelude::*;
use yew_router::prelude::Link;

use super::change::{HistoryLine, metric_name, metric_value_text};
use super::chart::VersionChart;
use crate::components::run_labels::{DayLabel, version_tag, version_tag_view};
use crate::pages::compare::{CompareLink, oldest_first};
use crate::router::AppRoute;

/// A rate-limited line also charts P99, the latency that the headline numbers lead with.
const RATE_LIMITED_CHARTS: [Metric; 2] = [Metric::Throughput, Metric::P99];
/// A line without a rate limit measures throughput only.
const UNCAPPED_CHARTS: [Metric; 1] = [Metric::Throughput];
const LATENCY_COLUMNS: [Metric; 3] = [Metric::P50, Metric::P99, Metric::P999];
const ONLY_RUN_NOTE: &str = "This is the only run of this workload on this machine and topology.";
pub(crate) const NO_VALUE: &str = "-";
pub(super) const CURRENT_LABEL: &str = "This run";
pub(super) const VERSION_HEADER: &str = "Version";
const COMMIT_HEADER: &str = "Commit";
const DATE_HEADER: &str = "Date";
const BADGES_HEADER: &str = "Badges";
const OPEN_LABEL: &str = "Open";
const COMPARE_LABEL: &str = "Compare";
const COMPARE_TITLE: &str = "Compare this run with the page's run";

#[derive(Debug, PartialEq)]
pub struct History {
    pub charts: Vec<HistoryChart>,
    /// Newest version first, and the newest run first within a version.
    pub rows: Vec<HistoryRow>,
    /// Only a rate-limited line has latency columns.
    pub latency: bool,
    /// A release run records its tag and no commit, so a line of releases has no commit column.
    pub commits: bool,
}

/// One metric of the main group of each run on the line, by version.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryChart {
    pub metric: Metric,
    /// Oldest first, the x axis of the chart.
    pub versions: Vec<String>,
    /// The median of each version whose runs all have the main group.
    pub medians: Vec<VersionPoint>,
    /// The runs other than the page's own run.
    pub runs: Vec<VersionPoint>,
    /// `None` when the page's run lacks the main group.
    pub current: Option<VersionPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VersionPoint {
    /// The index of the version in [`HistoryChart::versions`].
    pub version: usize,
    pub value: f64,
}

#[derive(Debug, PartialEq)]
pub struct HistoryRow {
    pub summary: BenchmarkSummary,
    /// The page's own run.
    pub current: bool,
}

impl History {
    pub fn new(run: &BenchmarkSummary, line: &HistoryLine<'_>) -> Self {
        let latency = run.workload_key.rate_limit.is_some();
        let metrics: &[Metric] = if latency {
            &RATE_LIMITED_CHARTS
        } else {
            &UNCAPPED_CHARTS
        };
        let rows: Vec<HistoryRow> = line
            .versions
            .iter()
            .rev()
            .flat_map(|version| version.runs.iter().rev())
            .map(|&summary| HistoryRow {
                current: summary.uuid == run.uuid,
                summary: summary.clone(),
            })
            .collect();
        Self {
            charts: metrics
                .iter()
                .map(|&metric| {
                    HistoryChart::new(
                        metric,
                        run.kind_family.primary_group(),
                        line,
                        Some(run.uuid),
                    )
                })
                .collect(),
            commits: rows.iter().any(|row| row.summary.commit.is_some()),
            rows,
            latency,
        }
    }

    /// A line with one run has nothing to compare.
    pub fn is_single_run(&self) -> bool {
        self.rows.len() == 1
    }
}

impl HistoryChart {
    /// `group` is the main group of the line's runs. The chart marks the run `current`, if any.
    pub fn new(
        metric: Metric,
        group: GroupMetricsKind,
        line: &HistoryLine<'_>,
        current: Option<Uuid>,
    ) -> Self {
        let mut chart = Self {
            metric,
            versions: Vec::with_capacity(line.versions.len()),
            medians: Vec::with_capacity(line.versions.len()),
            runs: Vec::new(),
            current: None,
        };
        for (index, version) in line.versions.iter().enumerate() {
            chart.versions.push(version.version.to_owned());
            if let Some(range) = ValueRange::of(&version.runs, metric, group) {
                chart.medians.push(VersionPoint {
                    version: index,
                    value: range.median,
                });
            }
            for summary in &version.runs {
                let Some(numbers) = summary.group(group) else {
                    continue;
                };
                let point = VersionPoint {
                    version: index,
                    value: numbers.value(metric),
                };
                if current == Some(summary.uuid) {
                    chart.current = Some(point);
                } else {
                    chart.runs.push(point);
                }
            }
        }
        chart
    }
}

/// The charts and the table for the history line of `run`, or a note for a line with one run.
pub fn history_view(history: &History, run: &BenchmarkSummary) -> Html {
    if history.is_single_run() {
        return html! { <p class="section-text">{ ONLY_RUN_NOTE }</p> };
    }
    let latency: &[Metric] = if history.latency {
        &LATENCY_COLUMNS
    } else {
        &[]
    };
    html! {
        <>
            <div class="run-charts">
                { for history.charts.iter().map(|chart| html! {
                    <article class="card run-chart-card">
                        <h3>{ format!("{} by version", metric_name(chart.metric)) }</h3>
                        <VersionChart uuid={run.uuid} chart={chart.clone()} />
                    </article>
                }) }
            </div>
            <div class="card run-table-card">
                <table class="run-table">
                    <thead>
                        <tr>
                            <th scope="col">{ VERSION_HEADER }</th>
                            if history.commits {
                                <th scope="col">{ COMMIT_HEADER }</th>
                            }
                            <th scope="col">{ DATE_HEADER }</th>
                            <th scope="col" class="run-table-number">
                                { metric_name(Metric::Throughput) }
                            </th>
                            { for latency.iter().map(|&metric| html! {
                                <th scope="col" class="run-table-number">{ metric_name(metric) }</th>
                            }) }
                            <th scope="col">{ BADGES_HEADER }</th>
                            { link_header() }
                        </tr>
                    </thead>
                    <tbody>
                        { for history.rows.iter().map(|row| {
                            row_view(row, latency, history.commits, run)
                        }) }
                    </tbody>
                </table>
            </div>
        </>
    }
}

/// The text of `metric` for the main group of `summary`, or a dash for a run without the group.
pub(super) fn main_value_text(summary: &BenchmarkSummary, metric: Metric) -> String {
    summary
        .group(summary.kind_family.primary_group())
        .map_or_else(
            || NO_VALUE.to_owned(),
            |numbers| metric_value_text(metric, numbers.value(metric)),
        )
}

/// The header of the column that [`link_cell`] fills.
pub(super) fn link_header() -> Html {
    html! { <th scope="col"><span class="visually-hidden">{"Link"}</span></th> }
}

/// Links to `summary` and to its comparison with `run`, the page's own run, with the older run
/// first. The row of `run` gets a label instead.
pub(super) fn link_cell(summary: &BenchmarkSummary, run: &BenchmarkSummary) -> Html {
    if summary.uuid == run.uuid {
        return html! { <td class="run-table-link">{ CURRENT_LABEL }</td> };
    }
    let compared: Vec<Uuid> = oldest_first([run, summary])
        .into_iter()
        .map(|compared| compared.uuid)
        .collect();
    html! {
        <td class="run-table-link">
            <Link<AppRoute> to={AppRoute::Benchmark { uuid: summary.uuid.to_string() }}>
                { OPEN_LABEL }
            </Link<AppRoute>>
            <CompareLink runs={compared} title={COMPARE_TITLE}>{ COMPARE_LABEL }</CompareLink>
        </td>
    }
}

/// On a phone, the first cell labels the row, and every other cell names its column.
fn row_view(row: &HistoryRow, latency: &[Metric], commits: bool, run: &BenchmarkSummary) -> Html {
    let summary = &row.summary;
    let start = summary
        .version_order
        .timestamp_us
        .and_then(DayLabel::from_unix_micros);
    html! {
        <tr
            class={classes!(row.current.then_some("run-table-current"))}
            aria-current={row.current.then_some("page")}
        >
            <td>{ version_tag_view(&version_tag(summary), summary.release) }</td>
            if commits {
                // The commit's font stays off the cell, so its label on a phone keeps the same font
                // as every other label.
                <td data-label={COMMIT_HEADER}>
                    <span class="run-table-mono">
                        { summary.commit.as_deref().unwrap_or(NO_VALUE) }
                    </span>
                </td>
            }
            <td data-label={DATE_HEADER}>
                { start.map_or_else(|| html! { NO_VALUE }, |start| start.view()) }
            </td>
            <td class="run-table-number" data-label={metric_name(Metric::Throughput)}>
                { main_value_text(summary, Metric::Throughput) }
            </td>
            { for latency.iter().map(|&metric| html! {
                <td class="run-table-number" data-label={metric_name(metric)}>
                    { main_value_text(summary, metric) }
                </td>
            }) }
            <td data-label={BADGES_HEADER}>
                { for summary.badges.iter().map(|badge| html! {
                    <span class="badge" title={badge.explanation()}>{ badge.to_string() }</span>
                }) }
            </td>
            { link_cell(summary, run) }
        </tr>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, COMMIT, MACHINE, OLD_RELEASE, RELEASE, THRESHOLDS, build, limited, producer,
        rate_limited, run, with_numbers,
    };

    const OTHER_MACHINE: &str = "iggy-ci";
    const COMMIT_DATE: &str = "2026-10-01T12:00:00Z";

    fn history_of(id: u128, runs: &[BenchmarkSummary]) -> History {
        let run = runs
            .iter()
            .find(|summary| summary.uuid == Uuid::from_u128(id))
            .expect("the run is in the list");
        History::new(run, &HistoryLine::of(run, runs))
    }

    fn point(version: usize, value: f64) -> VersionPoint {
        VersionPoint { version, value }
    }

    fn row_ids(history: &History) -> Vec<u128> {
        history
            .rows
            .iter()
            .map(|row| row.summary.uuid.as_u128())
            .collect()
    }

    #[test]
    fn given_several_runs_per_version_when_charted_should_draw_medians_of_the_change() {
        let runs = vec![
            with_numbers(limited(1, OLD_RELEASE), 100.0, 4.0),
            with_numbers(limited(2, RELEASE), 120.0, 3.0),
            with_numbers(limited(3, RELEASE), 140.0, 2.0),
            with_numbers(limited(4, RELEASE), 110.0, 5.0),
            with_numbers(limited(5, CANDIDATE), 150.0, 1.0),
        ];

        let history = history_of(5, &runs);

        let [throughput, p99] = history.charts.as_slice() else {
            panic!("a rate-limited line charts throughput and P99");
        };
        assert_eq!(throughput.metric, Metric::Throughput);
        assert_eq!(throughput.versions, ["0.8.0", "0.9.0", "0.10.0-rc.1"]);
        assert_eq!(
            throughput.medians,
            [point(0, 100.0), point(1, 120.0), point(2, 150.0)]
        );
        assert_eq!(
            throughput.runs,
            [
                point(0, 100.0),
                point(1, 120.0),
                point(1, 140.0),
                point(1, 110.0)
            ]
        );
        assert_eq!(p99.metric, Metric::P99);
        assert_eq!(p99.medians, [point(0, 4.0), point(1, 3.0), point(2, 1.0)]);

        let run = &runs[4];
        let step = HistoryLine::of(run, &runs)
            .step(run)
            .expect("0.9.0 comes before the candidate");
        let delta = THRESHOLDS
            .delta(
                Metric::Throughput,
                run.kind_family.primary_group(),
                &step.base,
                &step.head,
            )
            .expect("both versions hold the main group");
        assert_eq!(
            (delta.base.median, delta.head.median),
            (throughput.medians[1].value, throughput.medians[2].value),
            "the line goes through the medians that the headline change compares"
        );
    }

    #[test]
    fn given_releases_and_builds_when_listed_should_order_versions_and_show_newest_first() {
        let runs = vec![
            build(4, COMMIT, COMMIT_DATE),
            limited(1, OLD_RELEASE),
            limited(3, CANDIDATE),
            limited(2, RELEASE),
        ];

        let history = history_of(2, &runs);

        assert_eq!(
            history.charts[0].versions,
            ["0.8.0", "0.9.0", "0.10.0-rc.1", "0.10.0 at 91e020636"]
        );
        assert_eq!(row_ids(&history), [4, 3, 2, 1]);
        let releases: Vec<_> = history.rows.iter().map(|row| row.summary.release).collect();
        assert_eq!(releases, [false, false, true, true]);
    }

    #[test]
    fn given_line_without_recorded_commit_when_listed_should_drop_commit_column() {
        let releases = vec![limited(1, OLD_RELEASE), limited(2, RELEASE)];
        let mut with_build = releases.clone();
        with_build.push(build(3, COMMIT, COMMIT_DATE));

        assert!(!history_of(2, &releases).commits);
        assert!(history_of(2, &with_build).commits);
    }

    #[test]
    fn given_run_among_runs_of_its_version_when_listed_should_mark_only_that_run() {
        let runs = vec![
            with_numbers(limited(1, RELEASE), 120.0, 3.0),
            with_numbers(limited(2, RELEASE), 140.0, 2.0),
            with_numbers(limited(3, RELEASE), 110.0, 5.0),
        ];

        let history = history_of(2, &runs);

        let current: Vec<_> = history
            .rows
            .iter()
            .filter(|row| row.current)
            .map(|row| row.summary.uuid.as_u128())
            .collect();
        assert_eq!(current, [2]);
        assert_eq!(row_ids(&history), [3, 2, 1]);
        assert_eq!(history.charts[0].current, Some(point(0, 140.0)));
        assert_eq!(history.charts[0].runs, [point(0, 120.0), point(0, 110.0)]);
        assert!(!history.is_single_run());
    }

    #[test]
    fn given_line_of_one_run_when_listed_should_hold_that_run_only() {
        let runs = vec![
            limited(1, RELEASE),
            run(2, producer(), MACHINE, RELEASE),
            run(3, rate_limited(producer()), OTHER_MACHINE, RELEASE),
        ];

        let history = history_of(1, &runs);

        assert!(history.is_single_run());
        assert_eq!(row_ids(&history), [1]);
        assert!(history.rows[0].current);
    }

    #[test]
    fn given_line_without_rate_limit_when_charted_should_show_throughput_only() {
        let runs = vec![
            with_numbers(run(1, producer(), MACHINE, RELEASE), 2000.0, 9.0),
            with_numbers(run(2, producer(), MACHINE, CANDIDATE), 2100.0, 8.0),
        ];

        let history = history_of(2, &runs);

        let metrics: Vec<_> = history.charts.iter().map(|chart| chart.metric).collect();
        assert_eq!(metrics, [Metric::Throughput]);
        assert!(!history.latency);
    }
}
