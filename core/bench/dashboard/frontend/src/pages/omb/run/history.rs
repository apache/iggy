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

//! The History section: every run of a run's workload and driver on its machines. When every run
//! has an Iggy version, the line follows the versions, as on the iggy-bench pages, and the charts
//! put the runs of a version at one place with their median. Otherwise the line follows the run
//! times, one place per run.

use bench_dashboard_shared::delta::ValueRange;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::labels::{NO_VALUE, run_name, version_text};
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use yew::prelude::*;
use yew_router::prelude::Link;

use super::change::Kpi;
use super::chart::RunsChart;
use crate::pages::omb::common::{
    LineOrder, OmbCompareLink, day_view, line_order, run_route, version_view,
};
use crate::router::AppRoute;

/// The numbers that the History charts draw, one chart each.
const HISTORY_CHARTS: [Kpi; 2] = [Kpi::PublishThroughput, Kpi::PublishP99];
/// The numbers in the table, after the date.
pub const TABLE_KPIS: [Kpi; 3] = [Kpi::PublishThroughput, Kpi::PublishP99, Kpi::EndToEndP99];
const ONLY_RUN_NOTE: &str = "This is the only run of this workload and driver on these machines.";
const ONLY_RUN_UNKNOWN_MACHINES_NOTE: &str = "This is the only run of this workload and driver \
                                              whose machines are unknown.";
pub const CURRENT_LABEL: &str = "This run";
pub const VERSION_HEADER: &str = "Version";
pub const DATE_HEADER: &str = "Date";
const BADGES_HEADER: &str = "Badges";
const OPEN_LABEL: &str = "Open";
const COMPARE_LABEL: &str = "Compare";
const COMPARE_TITLE: &str = "Compare this run with the page's run, the older run first";
/// For example `Nov 6`, short enough for a chart axis.
const AXIS_DAY_FORMAT: &str = "%b %-d";

/// Whether the setup of the page's run names its machines. Runs that don't name them share a line,
/// so the page can't say that the runs it puts next to each other ran on the same machines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Machines {
    Known,
    Unknown,
}

#[derive(Debug, PartialEq)]
pub struct History {
    pub order: LineOrder,
    pub machines: Machines,
    /// Oldest first.
    pub runs: Vec<OmbRunSummary>,
    /// The page's own run.
    pub current: OmbRunId,
}

/// What the changes of the page's run compare with.
#[derive(Debug, PartialEq)]
pub enum Baseline<'a> {
    /// On a line in version order, as on the iggy-bench pages: the median of the previous version
    /// against the median of the run's own version, so a rerun of a version doesn't count as a
    /// change.
    Version(VersionStep<'a>),
    /// On a line in time order, which has no versions to compare: the run right before.
    Run(&'a OmbRunSummary),
}

/// Two neighboring versions on a line, each with its runs in line order.
#[derive(Debug, PartialEq)]
pub struct VersionStep<'a> {
    pub previous: String,
    pub base: Vec<&'a OmbRunSummary>,
    /// The version of the page's run.
    pub version: String,
    /// The page's run included.
    pub head: Vec<&'a OmbRunSummary>,
}

/// One number of each run on the line, for a chart.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryChart {
    pub kpi: Kpi,
    pub order: LineOrder,
    /// The x axis, oldest first: one label per version on a line in version order, else one per
    /// run.
    pub labels: Vec<String>,
    /// One per run that has the number.
    pub points: Vec<HistoryPoint>,
    /// The median of each version whose runs all have the number. Empty on a line in time order,
    /// which has one run per label.
    pub medians: Vec<HistoryPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryPoint {
    /// The index of the run's label in [`HistoryChart::labels`].
    pub index: usize,
    pub value: f64,
    /// The run or the version, and the value, for the tooltip.
    pub name: String,
    /// The page's run. Never a median.
    pub current: bool,
}

impl Machines {
    pub fn of(run: &OmbRunSummary) -> Self {
        if run.history_key().environment.is_some() {
            Self::Known
        } else {
            Self::Unknown
        }
    }
}

impl History {
    /// The line of `run` within `runs`: the runs with its workload, driver and machines.
    pub fn new(run: &OmbRunSummary, runs: &[OmbRunSummary]) -> Self {
        let key = run.history_key();
        let (order, line) = line_order(runs.iter().filter(|summary| summary.history_key() == key));
        Self {
            order,
            machines: Machines::of(run),
            runs: line.into_iter().cloned().collect(),
            current: run.id,
        }
    }

    /// What the changes of the page's run compare with. `None` when the run's version, or the run
    /// on a line in time order, comes first on the line.
    pub fn baseline(&self) -> Option<Baseline<'_>> {
        match self.order {
            LineOrder::Version => {
                let places = self.places();
                let index = places
                    .iter()
                    .position(|(_, runs)| runs.iter().any(|run| run.id == self.current))?;
                let mut step = places.into_iter().skip(index.checked_sub(1)?);
                let (previous, base) = step.next()?;
                let (version, head) = step.next()?;
                Some(Baseline::Version(VersionStep {
                    previous,
                    base,
                    version,
                    head,
                }))
            }
            LineOrder::Time => {
                let index = self.runs.iter().position(|run| run.id == self.current)?;
                self.runs.get(index.checked_sub(1)?).map(Baseline::Run)
            }
        }
    }

    /// The page's run and `other` in line order, for a compare link whose baseline is the run that
    /// comes first on the line, as it does in the changes of the page.
    pub fn compared(&self, other: OmbRunId) -> Vec<OmbRunId> {
        self.runs
            .iter()
            .map(|run| run.id)
            .filter(|id| *id == self.current || *id == other)
            .collect()
    }

    /// A line with one run has nothing to compare.
    pub fn is_single_run(&self) -> bool {
        self.runs.len() <= 1
    }

    /// Whether any run has a version, which gives the table a version column.
    pub fn has_versions(&self) -> bool {
        self.runs.iter().any(|run| run.version.is_some())
    }

    pub fn chart(&self, kpi: Kpi) -> HistoryChart {
        let places = self.places();
        let mut chart = HistoryChart {
            kpi,
            order: self.order,
            labels: Vec::with_capacity(places.len()),
            points: Vec::with_capacity(self.runs.len()),
            medians: Vec::new(),
        };
        for (index, (label, runs)) in places.into_iter().enumerate() {
            let values: Option<Vec<f64>> = runs.iter().map(|run| kpi.value(run)).collect();
            if self.order == LineOrder::Version
                && let Some(range) = values.and_then(ValueRange::of_values)
            {
                chart.medians.push(HistoryPoint {
                    index,
                    value: range.median,
                    name: format!("{label} median: {}", kpi.text(Some(range.median))),
                    current: false,
                });
            }
            chart.points.extend(runs.iter().filter_map(|run| {
                let value = kpi.value(run)?;
                Some(HistoryPoint {
                    index,
                    value,
                    name: format!("{}: {}", run_name(run), kpi.text(Some(value))),
                    current: run.id == self.current,
                })
            }));
            chart.labels.push(label);
        }
        chart
    }

    /// The places on the x axis, oldest first, each with its label and its runs. On a line in
    /// version order, a version takes the place of its first run, as on the iggy-bench pages.
    fn places(&self) -> Vec<(String, Vec<&OmbRunSummary>)> {
        let mut places: Vec<(String, Vec<&OmbRunSummary>)> = Vec::new();
        for run in &self.runs {
            match (self.order, version_text(run)) {
                (LineOrder::Version, Some(version)) => {
                    match places.iter_mut().find(|(label, _)| *label == version) {
                        Some((_, runs)) => runs.push(run),
                        None => places.push((version, vec![run])),
                    }
                }
                _ => places.push((
                    run.time.instant.format(AXIS_DAY_FORMAT).to_string(),
                    vec![run],
                )),
            }
        }
        places
    }
}

/// The charts and the table of the line, or a note for a line with one run.
pub fn history_view(history: &History, run: &OmbRunSummary) -> Html {
    if history.is_single_run() {
        return html! { <p class="section-text">{ only_run_note(history.machines) }</p> };
    }
    let versions = history.has_versions();
    html! {
        <>
            <div class="run-charts">
                { for HISTORY_CHARTS.iter().map(|&kpi| html! {
                    <article class="card run-chart-card">
                        <h3>{ format!("{} by {}", kpi.label(), axis_unit(history.order)) }</h3>
                        <RunsChart run={run.id} chart={history.chart(kpi)} />
                    </article>
                }) }
            </div>
            <div class="card run-table-card">
                <table class="run-table">
                    <thead>
                        <tr>
                            if versions {
                                <th scope="col">{ VERSION_HEADER }</th>
                            }
                            <th scope="col">{ DATE_HEADER }</th>
                            { kpi_headers() }
                            <th scope="col">{ BADGES_HEADER }</th>
                            { link_header() }
                        </tr>
                    </thead>
                    <tbody>
                        { for history.runs.iter().rev().map(|summary| {
                            row_view(summary, versions, run, history.compared(summary.id))
                        }) }
                    </tbody>
                </table>
            </div>
        </>
    }
}

/// The headers of the cells that [`kpi_cells`] fills.
pub fn kpi_headers() -> Html {
    html! {
        { for TABLE_KPIS.iter().map(|kpi| html! {
            <th scope="col" class="run-table-number" title={kpi.hint()}>{ kpi.label() }</th>
        }) }
    }
}

/// The numbers of `summary` under [`kpi_headers`].
pub fn kpi_cells(summary: &OmbRunSummary) -> Html {
    html! {
        { for TABLE_KPIS.iter().map(|kpi| html! {
            <td class="run-table-number" data-label={kpi.label()}>
                { kpi.text(kpi.value(summary)) }
            </td>
        }) }
    }
}

/// The header of the column that [`link_cell`] fills.
pub fn link_header() -> Html {
    html! { <th scope="col"><span class="visually-hidden">{"Link"}</span></th> }
}

/// Links to `summary` and to `compared`, its comparison with `run`, the page's own run, with the
/// older run first. The row of `run` gets a label instead.
pub fn link_cell(summary: &OmbRunSummary, run: &OmbRunSummary, compared: Vec<OmbRunId>) -> Html {
    if summary.id == run.id {
        return html! { <td class="run-table-link">{ CURRENT_LABEL }</td> };
    }
    html! {
        <td class="run-table-link">
            <Link<AppRoute> to={run_route(summary.id)}>{ OPEN_LABEL }</Link<AppRoute>>
            <OmbCompareLink runs={compared} title={COMPARE_TITLE}>{ COMPARE_LABEL }</OmbCompareLink>
        </td>
    }
}

/// What a place on the x axis of the charts stands for, for the chart titles.
const fn axis_unit(order: LineOrder) -> &'static str {
    match order {
        LineOrder::Version => "version",
        LineOrder::Time => "run",
    }
}

const fn only_run_note(machines: Machines) -> &'static str {
    match machines {
        Machines::Known => ONLY_RUN_NOTE,
        Machines::Unknown => ONLY_RUN_UNKNOWN_MACHINES_NOTE,
    }
}

/// On a phone, the first cell labels the row, and every other cell names its column. `compared`
/// is the row's run and the page's run in line order.
fn row_view(
    summary: &OmbRunSummary,
    versions: bool,
    run: &OmbRunSummary,
    compared: Vec<OmbRunId>,
) -> Html {
    let current = summary.id == run.id;
    html! {
        <tr
            class={classes!(current.then_some("run-table-current"))}
            aria-current={current.then_some("page")}
        >
            if versions {
                <td>
                    if summary.version.is_some() {
                        { version_view(summary) }
                    } else {
                        { NO_VALUE }
                    }
                </td>
            }
            <td data-label={versions.then_some(DATE_HEADER)}>{ day_view(&summary.time) }</td>
            { kpi_cells(summary) }
            <td data-label={BADGES_HEADER}>
                { for summary.badges.iter().map(|badge| html! {
                    <span class="badge" title={badge.explanation()}>{ badge.to_string() }</span>
                }) }
            </td>
            { link_cell(summary, run, compared) }
        </tr>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS, OMB_RC1_DEFAULT,
        OMB_RC1_PERSISTED, omb_rerun, omb_summaries,
    };

    fn ids(runs: &[OmbRunSummary]) -> Vec<OmbRunId> {
        runs.iter().map(|run| run.id).collect()
    }

    #[test]
    fn given_fixtures_of_two_versions_when_lined_up_should_follow_versions() {
        let runs = omb_summaries();
        let run = OMB_RC1_DEFAULT.summary();

        let history = History::new(&run, &runs);

        assert_eq!(history.order, LineOrder::Version);
        assert_eq!(history.machines, Machines::Known);
        assert_eq!(
            ids(&history.runs),
            [OMB_EDGE8_DEFAULT.id(), OMB_RC1_DEFAULT.id()],
            "the persisted driver, the other workloads and the runs without machines are left out"
        );
        assert_eq!(
            history.baseline(),
            Some(Baseline::Version(VersionStep {
                previous: "0.9.1-edge.8".to_owned(),
                base: vec![&history.runs[0]],
                version: "0.10.0-rc.1".to_owned(),
                head: vec![&history.runs[1]],
            }))
        );
        assert!(!history.is_single_run());
        assert!(history.has_versions());
    }

    #[test]
    fn given_oldest_version_of_line_when_lined_up_should_have_no_baseline() {
        let runs = omb_summaries();

        let history = History::new(&OMB_EDGE8_DEFAULT.summary(), &runs);

        assert_eq!(history.baseline(), None);
    }

    #[test]
    fn given_older_version_and_newer_run_without_version_when_compared_should_follow_run_times() {
        let older = OMB_RC1_DEFAULT.summary();
        let mut newer = omb_rerun(&older, "00000000000000ab", 24 * 4);
        newer.version = None;
        let runs = [newer.clone(), older.clone()];

        let on_newer_page = History::new(&newer, &runs);
        let on_older_page = History::new(&older, &runs);

        assert_eq!(on_newer_page.order, LineOrder::Time);
        assert_eq!(
            on_newer_page.baseline(),
            Some(Baseline::Run(&older)),
            "the changes compare with the older run"
        );
        assert_eq!(on_newer_page.compared(older.id), [older.id, newer.id]);
        assert_eq!(on_older_page.compared(newer.id), [older.id, newer.id]);
    }

    #[test]
    fn given_runs_without_sidecar_when_lined_up_should_share_line_in_time_order() {
        let first = OMB_NO_CONSUMERS.summary();
        let second = omb_rerun(&first, "00000000000000cc", 2);
        let runs = vec![second.clone(), first.clone(), OMB_PUBLISH_ERRORS.summary()];

        let history = History::new(&second, &runs);

        assert_eq!(history.order, LineOrder::Time);
        assert_eq!(history.machines, Machines::Unknown);
        assert_eq!(ids(&history.runs), [first.id, second.id]);
        assert_eq!(history.baseline(), Some(Baseline::Run(&first)));
        assert!(!history.has_versions());
    }

    #[test]
    fn given_run_alone_on_its_line_when_lined_up_should_hold_that_run_only() {
        let runs = omb_summaries();

        let history = History::new(&OMB_RC1_PERSISTED.summary(), &runs);

        assert!(history.is_single_run());
        assert_eq!(ids(&history.runs), [OMB_RC1_PERSISTED.id()]);
        assert_eq!(history.baseline(), None);
    }

    #[test]
    fn given_reruns_of_one_version_when_lined_up_should_compare_versions_not_reruns() {
        let edge8 = OMB_EDGE8_DEFAULT.summary();
        let edge8_rerun = omb_rerun(&edge8, "00000000000000f1", 2);
        let rc1 = OMB_RC1_DEFAULT.summary();
        let rc1_rerun = omb_rerun(&rc1, "00000000000000f2", 2);
        let runs = [
            rc1_rerun.clone(),
            edge8_rerun.clone(),
            rc1.clone(),
            edge8.clone(),
        ];

        let on_rerun_page = History::new(&rc1_rerun, &runs);
        let on_oldest_version_page = History::new(&edge8_rerun, &runs);

        let Some(Baseline::Version(step)) = on_rerun_page.baseline() else {
            panic!("a line of versions compares versions");
        };
        assert_eq!(
            (step.previous.as_str(), step.version.as_str()),
            ("0.9.1-edge.8", "0.10.0-rc.1")
        );
        let ids_of = |runs: &[&OmbRunSummary]| {
            let mut ids: Vec<_> = runs.iter().map(|run| run.id).collect();
            ids.sort();
            ids
        };
        let mut expected_base = vec![edge8.id, edge8_rerun.id];
        let mut expected_head = vec![rc1.id, rc1_rerun.id];
        expected_base.sort();
        expected_head.sort();
        assert_eq!(ids_of(&step.base), expected_base);
        assert_eq!(
            ids_of(&step.head),
            expected_head,
            "the version's other run counts, not only the page's run"
        );
        assert_eq!(
            on_oldest_version_page.baseline(),
            None,
            "an earlier run of the same version is no baseline"
        );
    }

    #[test]
    fn given_lone_run_when_noted_should_name_machines_only_when_known() {
        let known = History::new(&OMB_RC1_PERSISTED.summary(), &omb_summaries());
        let unknown = History::new(
            &OMB_PUBLISH_ERRORS.summary(),
            &[OMB_PUBLISH_ERRORS.summary()],
        );

        assert_eq!(only_run_note(known.machines), ONLY_RUN_NOTE);
        assert_eq!(
            only_run_note(unknown.machines),
            ONLY_RUN_UNKNOWN_MACHINES_NOTE
        );
        assert!(!ONLY_RUN_UNKNOWN_MACHINES_NOTE.contains("these machines"));
    }

    #[test]
    fn given_line_of_versions_when_charted_should_label_versions_and_mark_page_run() {
        let history = History::new(&OMB_RC1_DEFAULT.summary(), &omb_summaries());

        let chart = history.chart(Kpi::PublishP99);

        assert_eq!(chart.labels, ["0.9.1-edge.8", "0.10.0-rc.1"]);
        let values: Vec<_> = chart
            .points
            .iter()
            .map(|point| (point.index, point.value, point.current))
            .collect();
        assert_eq!(values, [(0, 1.519, false), (1, 1.523, true)]);
        assert!(chart.points[1].name.ends_with(": 1.52 ms"));
        assert_eq!(chart.order, LineOrder::Version);
        assert_eq!(
            chart
                .medians
                .iter()
                .map(|median| (median.index, median.value))
                .collect::<Vec<_>>(),
            [(0, 1.519), (1, 1.523)]
        );
        assert_eq!(chart.medians[1].name, "0.10.0-rc.1 median: 1.52 ms");
    }

    #[test]
    fn given_reruns_of_one_version_when_charted_should_share_its_place_with_their_median() {
        let edge8 = OMB_EDGE8_DEFAULT.summary();
        let rc1 = OMB_RC1_DEFAULT.summary();
        let mut rerun = omb_rerun(&rc1, "00000000000000e1", 24);
        rerun.publish_latency_ms.p99 = 1.6;
        let mut slow_rerun = omb_rerun(&rc1, "00000000000000e2", 48);
        slow_rerun.publish_latency_ms.p99 = 9.9;
        let runs = [slow_rerun.clone(), rc1, rerun.clone(), edge8];

        let chart = History::new(&rerun, &runs).chart(Kpi::PublishP99);

        assert_eq!(chart.labels, ["0.9.1-edge.8", "0.10.0-rc.1"]);
        let mut values: Vec<_> = chart
            .points
            .iter()
            .map(|point| (point.index, point.value, point.current))
            .collect();
        // The reruns keep the sidecar time of the run they copy, so their order in the version
        // comes from their ids, which says nothing about the chart.
        values.sort_by(|left, right| (left.0, left.1).partial_cmp(&(right.0, right.1)).unwrap());
        assert_eq!(
            values,
            [
                (0, 1.519, false),
                (1, 1.523, false),
                (1, 1.6, true),
                (1, 9.9, false)
            ]
        );
        assert_eq!(
            chart
                .medians
                .iter()
                .map(|median| (median.index, median.value))
                .collect::<Vec<_>>(),
            [(0, 1.519), (1, 1.6)],
            "the median of three runs is the middle one, not their mean"
        );
    }

    #[test]
    fn given_line_in_time_order_when_charted_should_label_days() {
        let first = OMB_NO_CONSUMERS.summary();
        let second = omb_rerun(&first, "00000000000000dd", 48);
        let history = History::new(&first, &[first.clone(), second]);

        let chart = history.chart(Kpi::PublishThroughput);

        assert_eq!(chart.labels, ["Oct 22", "Oct 24"]);
        assert_eq!(chart.points.len(), 2);
        assert_eq!(chart.order, LineOrder::Time);
        assert!(chart.medians.is_empty());
        assert_eq!(axis_unit(chart.order), "run");
    }
}
