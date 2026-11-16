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

//! The Related runs section: the runs of a run's workload on its machines with another driver
//! configuration, for example `Iggy-persisted` next to `Iggy`. Each driver shows its newest run.

use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use yew::prelude::*;

use super::history::{
    DATE_HEADER, Machines, VERSION_HEADER, kpi_cells, kpi_headers, link_cell, link_header,
};
use crate::pages::compare::runs::with_baseline;
use crate::pages::omb::common::{
    NO_VALUE, OmbCompareLink, day_view, driver_tag, line_order, oldest_first, version_view,
};
use crate::state::selection::MAX_COMPARED_RUNS;

const NO_RELATED_NOTE: &str = "No run of this workload on these machines used another driver \
                               configuration.";
const NO_RELATED_UNKNOWN_MACHINES_NOTE: &str = "No run of this workload whose machines are unknown \
                                                used another driver configuration.";
const DRIVER_HEADER: &str = "Driver";
const COMPARE_ALL_TITLE: &str = "Open these runs on the compare page, with this run as the \
                                 baseline";

/// The newest run of each driver other than the driver of `run`, with the workload and machines of
/// `run`, in driver name order. A line's order tells which run is the newest.
pub fn related_runs(run: &OmbRunSummary, runs: &[OmbRunSummary]) -> Vec<OmbRunSummary> {
    let machines = run.history_key().environment;
    let candidates: Vec<&OmbRunSummary> = runs
        .iter()
        .filter(|candidate| {
            candidate.driver != run.driver
                && candidate.workload == run.workload
                && candidate.history_key().environment == machines
        })
        .collect();
    let mut drivers: Vec<&str> = candidates
        .iter()
        .map(|candidate| candidate.driver.as_str())
        .collect();
    drivers.sort_unstable();
    drivers.dedup();
    drivers
        .into_iter()
        .filter_map(|driver| {
            let (_, line) = line_order(
                candidates
                    .iter()
                    .copied()
                    .filter(|candidate| candidate.driver == driver),
            );
            line.last().map(|&newest| newest.clone())
        })
        .collect()
}

/// The page's run as the baseline, then the related runs that fit on the compare page.
pub fn compare_all(run: &OmbRunSummary, related: &[OmbRunSummary]) -> Vec<OmbRunId> {
    let others: Vec<OmbRunId> = related.iter().map(|summary| summary.id).collect();
    let mut compared = with_baseline(&others, run.id);
    compared.truncate(MAX_COMPARED_RUNS);
    compared
}

/// A table with the page's run and the newest run of each other driver, or a note when there's
/// none. `machines` tells whether the page's run names its machines.
pub fn related_view(related: &[OmbRunSummary], run: &OmbRunSummary, machines: Machines) -> Html {
    if related.is_empty() {
        return html! { <p class="section-text">{ no_related_note(machines) }</p> };
    }
    let compared = compare_all(run, related);
    let label = if compared.len() == related.len() + 1 {
        "Compare all".to_owned()
    } else {
        format!("Compare the first {}", compared.len())
    };
    html! {
        <>
            <div class="card run-table-card">
                <table class="run-table">
                    <thead>
                        <tr>
                            <th scope="col">{ DRIVER_HEADER }</th>
                            <th scope="col">{ VERSION_HEADER }</th>
                            <th scope="col">{ DATE_HEADER }</th>
                            { kpi_headers() }
                            { link_header() }
                        </tr>
                    </thead>
                    <tbody>
                        { row_view(run, run) }
                        { for related.iter().map(|summary| row_view(summary, run)) }
                    </tbody>
                </table>
            </div>
            <p class="omb-run-related-actions">
                <OmbCompareLink
                    runs={compared}
                    classes={classes!("run-action")}
                    title={COMPARE_ALL_TITLE}
                >
                    { label }
                </OmbCompareLink>
            </p>
        </>
    }
}

const fn no_related_note(machines: Machines) -> &'static str {
    match machines {
        Machines::Known => NO_RELATED_NOTE,
        Machines::Unknown => NO_RELATED_UNKNOWN_MACHINES_NOTE,
    }
}

/// On a phone, the first cell labels the row, and every other cell names its column. The runs of
/// two drivers are on two lines, so the compare link puts the older of the two first.
fn row_view(summary: &OmbRunSummary, run: &OmbRunSummary) -> Html {
    let current = summary.id == run.id;
    let compared: Vec<OmbRunId> = oldest_first([run, summary])
        .into_iter()
        .map(|compared| compared.id)
        .collect();
    html! {
        <tr
            class={classes!(current.then_some("run-table-current"))}
            aria-current={current.then_some("page")}
        >
            <td>{ driver_tag(&summary.driver) }</td>
            <td data-label={VERSION_HEADER}>
                if summary.version.is_some() {
                    { version_view(summary) }
                } else {
                    { NO_VALUE }
                }
            </td>
            <td data-label={DATE_HEADER}>{ day_view(&summary.time) }</td>
            { kpi_cells(summary) }
            { link_cell(summary, run, compared) }
        </tr>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_EDGE8_100_PARTITIONS, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS,
        OMB_RC1_DEFAULT, OMB_RC1_PERSISTED, omb_summaries,
    };

    fn ids(runs: &[OmbRunSummary]) -> Vec<OmbRunId> {
        runs.iter().map(|run| run.id).collect()
    }

    /// `run` with another driver name and id, as a run of a third driver configuration.
    fn with_driver(run: &OmbRunSummary, driver: &str, id: &str) -> OmbRunSummary {
        let mut other = run.clone();
        other.driver = driver.to_owned();
        other.id = id.parse().expect("a test id parses");
        other
    }

    #[test]
    fn given_fixtures_when_related_should_pick_other_driver_on_same_workload_and_machines() {
        let runs = omb_summaries();

        assert_eq!(
            ids(&related_runs(&OMB_RC1_DEFAULT.summary(), &runs)),
            [OMB_RC1_PERSISTED.id()]
        );
        assert_eq!(
            ids(&related_runs(&OMB_RC1_PERSISTED.summary(), &runs)),
            [OMB_RC1_DEFAULT.id()],
            "the 0.10.0-rc.1 run is newer than the 0.9.1-edge.8 run of the same driver"
        );
        assert_eq!(
            ids(&related_runs(&OMB_EDGE8_DEFAULT.summary(), &runs)),
            [OMB_RC1_PERSISTED.id()]
        );
    }

    #[test]
    fn given_fixtures_of_other_workload_or_machines_when_related_should_leave_them_out() {
        let runs = omb_summaries();

        for fixture in [
            &OMB_EDGE8_100_PARTITIONS,
            &OMB_NO_CONSUMERS,
            &OMB_PUBLISH_ERRORS,
        ] {
            assert_eq!(
                related_runs(&fixture.summary(), &runs),
                [],
                "{}",
                fixture.file_name
            );
        }
    }

    #[test]
    fn given_run_without_sidecar_when_nothing_related_should_not_name_these_machines() {
        let run = OMB_NO_CONSUMERS.summary();

        assert_eq!(related_runs(&run, &omb_summaries()), []);
        assert_eq!(
            no_related_note(Machines::of(&run)),
            NO_RELATED_UNKNOWN_MACHINES_NOTE
        );
        assert_eq!(
            no_related_note(Machines::of(&OMB_RC1_DEFAULT.summary())),
            NO_RELATED_NOTE
        );
        assert!(!NO_RELATED_UNKNOWN_MACHINES_NOTE.contains("these machines"));
    }

    #[test]
    fn given_other_driver_on_other_machines_when_related_should_leave_it_out() {
        let run = OMB_RC1_DEFAULT.summary();
        let mut elsewhere = OMB_RC1_PERSISTED.summary();
        elsewhere
            .setup
            .as_mut()
            .and_then(|setup| setup.environment.as_mut())
            .expect("the fixture names its machines")
            .server_nodes = Some(5);

        assert_eq!(related_runs(&run, &[run.clone(), elsewhere]), []);
    }

    #[test]
    fn given_more_drivers_than_compare_page_holds_when_compared_should_keep_page_run_first() {
        let run = OMB_RC1_DEFAULT.summary();
        let persisted = OMB_RC1_PERSISTED.summary();
        let fsync = with_driver(&persisted, "Iggy-fsync", "00000000000000a1");
        let batched = with_driver(&persisted, "Iggy-batched", "00000000000000a2");
        let runs = [
            run.clone(),
            persisted.clone(),
            fsync.clone(),
            batched.clone(),
        ];

        let related = related_runs(&run, &runs);
        let compared = compare_all(&run, &related);

        assert_eq!(ids(&related), [batched.id, fsync.id, persisted.id]);
        assert_eq!(compared, [run.id, batched.id, fsync.id]);
        assert_eq!(compared.len(), MAX_COMPARED_RUNS);
        assert_eq!(compare_all(&run, &related[..1]), [run.id, batched.id]);
    }
}
