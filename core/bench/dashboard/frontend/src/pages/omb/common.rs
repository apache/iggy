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

//! The parts that every OMB page shares: the page head with the banner, the number and label texts,
//! the links, the compare tray, and the states of the run list. They're defined once, so an OMB run
//! reads the same on every page.

use bench_dashboard_shared::labels::DAY_FORMAT;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::labels::{run_name, version_text};
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary, OmbRunTime, OmbTimeSource};
use chrono::SecondsFormat;
use yew::prelude::*;

use crate::components::compare_tray::{CompareToggleOf, CompareTrayOf, TrayRun};
use crate::components::framework::omb_banner;
use crate::components::page::{Crumb, loadable, page_head_with_banner};
use crate::components::run_labels::version_tag_view;
use crate::pages::compare::runs::CompareLinkOf;
use crate::pages::run::change::percent_text;
use crate::router::AppRoute;
use crate::state::data::Loadable;
use crate::state::omb::{OmbData, use_omb_data};
use crate::state::selection::use_omb_selection;

/// What the pages show for a number that the run didn't measure, such as the end-to-end latency
/// of a run without consumers.
pub const NOT_MEASURED: &str = "not measured";
/// What the pages show for a number that the file doesn't hold, such as the publish delay of a file
/// from an older OMB.
pub const NOT_RECORDED: &str = "not recorded";
/// The breadcrumb and eyebrow of the OMB pages.
pub const OMB_CRUMB: &str = "OMB";
/// Where [`bench_dashboard_shared::omb::chart::PERCENTILE_LABELS`] has P99, the percentile that the
/// headlines use.
pub const P99_INDEX: usize = 3;
pub const PUBLISH_THROUGHPUT_HINT: &str = "The mean publish rate over the 10 second windows, times \
                                           the message size. 1 MiB is 1,048,576 bytes.";
pub const MESSAGES_HINT: &str = "The mean publish rate over the 10 second windows, in messages.";
pub const CONSUME_HINT: &str = "The mean rate at which the consumers received messages, over all \
                                subscriptions. A run without consumers didn't measure it.";
pub const PUBLISH_LATENCY_HINT: &str = "The time from sending a message until the server confirmed \
                                        it, from one histogram of every send of every producer.";
pub const END_TO_END_LATENCY_HINT: &str = "The time from sending a message until a consumer \
                                           received it, in whole milliseconds from the clocks of \
                                           the worker hosts.";
pub const PUBLISH_DELAY_HINT: &str = "How much later than planned the producers sent their \
                                      messages, against the schedule of the target rate.";
pub const DRIVER_HINT: &str = "The driver configuration, named in its OMB driver file";
const NO_CONSUMERS_NOTE: &str = "This run has no consumers, so it didn't measure end-to-end \
                                 latency.";
const NOT_IN_FILE_NOTE: &str = "The result file of this run holds no end-to-end latency.";
/// A change that rounds to zero, without a sign.
const ZERO_PERCENT: &str = "0.0%";
const RUNS_WHAT: &str = "the OMB runs";
/// A time from a file name has no zone, so its machine-readable form has none either.
const LOCAL_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

/// A link to the OMB compare page, the baseline first.
pub type OmbCompareLink = CompareLinkOf<OmbRunId>;
/// A row's toggle that puts an OMB run into the OMB tray.
pub type OmbCompareToggle = CompareToggleOf<OmbRunId>;

/// How the runs of a line are ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineOrder {
    /// Every run has a version from its sidecar.
    Version,
    /// A run lacks a version, so the line can't follow the versions.
    Time,
}

/// The end-to-end latency of a run, or why it has none.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EndToEnd {
    Measured(OmbPercentiles),
    /// The workload has no consumers, so no message arrived anywhere.
    NoConsumers,
    /// The run had consumers, but its file holds no end-to-end numbers. OMB only records a sample
    /// above 0 ms, so clocks that disagree or messages that all arrive within a millisecond leave
    /// the histogram empty.
    NotInFile,
}

impl EndToEnd {
    pub fn of(run: &OmbRunSummary) -> Self {
        match run.end_to_end_latency_ms {
            Some(latency) => Self::Measured(latency),
            None if run.workload.consumers_per_topic == 0 => Self::NoConsumers,
            None => Self::NotInFile,
        }
    }

    /// Why the run has no end-to-end numbers. `None` for a run that has them.
    pub const fn note(self) -> Option<&'static str> {
        match self {
            Self::Measured(_) => None,
            Self::NoConsumers => Some(NO_CONSUMERS_NOTE),
            Self::NotInFile => Some(NOT_IN_FILE_NOTE),
        }
    }
}

/// The head of an OMB page, with the OMB banner below the lead.
pub fn omb_page_head(
    above: Html,
    eyebrow: Option<&str>,
    title: &str,
    lead: &str,
    aside: Html,
) -> Html {
    page_head_with_banner(above, eyebrow, title, lead, omb_banner(), aside)
}

/// The breadcrumb that leads back to the OMB run list.
pub fn omb_crumb() -> Crumb {
    Crumb::new(OMB_CRUMB, AppRoute::OmbRuns)
}

pub fn run_route(id: OmbRunId) -> AppRoute {
    AppRoute::OmbRun { id: id.to_string() }
}

/// The loading or failed state of the run list, the empty state of a server without OMB results,
/// else `ready` with the list.
pub fn loaded_runs(data: &OmbData, ready: impl FnOnce(&OmbRunList) -> Html) -> Html {
    loadable(&data.runs, RUNS_WHAT, &data.reload, |list| {
        empty_state(list).unwrap_or_else(|| ready(list))
    })
}

/// The loading or failed state of the run list. Empty once it has loaded.
pub fn runs_status(data: &OmbData) -> Html {
    loadable(&data.runs, RUNS_WHAT, &data.reload, |_| Html::default())
}

/// What a list without runs says: the server has no OMB results directory, or the directory has
/// no runs yet. `None` for a list with runs.
pub fn empty_state(list: &OmbRunList) -> Option<Html> {
    let (title, note) = match (list.enabled, list.runs.is_empty()) {
        (_, false) => return None,
        (false, true) => (
            "No OMB results directory",
            "This dashboard server doesn't read OMB results. Start it with --omb-results-dir set \
             to a directory that holds one OMB run per subdirectory. The Docker image and the dev \
             script take that directory from OMB_RESULTS_DIR.",
        ),
        (true, true) => (
            "No OMB results yet",
            "Put each OMB result file in its own directory inside the OMB results directory, with \
             an optional setup.toml next to it. The server reads new files a few seconds after \
             they change. Reload the page to see them.",
        ),
    };
    Some(html! {
        <div class="explore-empty" role="status">
            <p class="explore-empty-title">{ title }</p>
            <p class="explore-empty-note">{ note }</p>
        </div>
    })
}

/// The picked OMB runs. Floats over the page while it holds a run.
#[function_component(OmbCompareTray)]
pub fn omb_compare_tray() -> Html {
    let data = use_omb_data();
    let selection = use_omb_selection();
    let (Some(selection), Loadable::Ready(list)) = (selection, &data.runs) else {
        return Html::default();
    };
    let runs: Vec<TrayRun<OmbRunId>> = tray_runs(selection.runs(), &list.runs)
        .into_iter()
        .map(|run| TrayRun {
            id: run.id,
            name: run_name(run),
            title: format!("{} ({})", run.workload.name, run.file_name),
        })
        .collect();
    html! { <CompareTrayOf<OmbRunId> {runs} /> }
}

/// The picked runs that are in the loaded list, oldest first, which is the compare page's order.
pub fn tray_runs<'a>(picked: &[OmbRunId], runs: &'a [OmbRunSummary]) -> Vec<&'a OmbRunSummary> {
    oldest_first(
        picked
            .iter()
            .filter_map(|id| runs.iter().find(|run| run.id == *id)),
    )
}

/// `runs` in the order of [`line_order`].
pub fn oldest_first<'a>(
    runs: impl IntoIterator<Item = &'a OmbRunSummary>,
) -> Vec<&'a OmbRunSummary> {
    line_order(runs).1
}

/// `runs` in line order: the shared crate's version order when every run has a version, as on the
/// iggy-bench pages, else time order. The version order puts a run without a version before every
/// version, whatever its time, so one such run makes the whole line follow the times. The id
/// orders runs of the same moment.
pub fn line_order<'a>(
    runs: impl IntoIterator<Item = &'a OmbRunSummary>,
) -> (LineOrder, Vec<&'a OmbRunSummary>) {
    let mut runs: Vec<_> = runs.into_iter().collect();
    if runs.iter().all(|run| run.version.is_some()) {
        runs.sort_by(|left, right| {
            (&left.version_order, left.id).cmp(&(&right.version_order, right.id))
        });
        return (LineOrder::Version, runs);
    }
    runs.sort_by_key(|run| (run.time.instant, run.id));
    (LineOrder::Time, runs)
}

/// A change in percent with its sign, for example `+4.6%`. A change that rounds to zero reads
/// `0.0%`, because a sign there would point in a direction that the number doesn't show.
pub fn change_text(percent: f64) -> String {
    let text = percent_text(percent);
    if text.trim_start_matches(['+', '-']) == ZERO_PERCENT {
        ZERO_PERCENT.to_owned()
    } else {
        text
    }
}

/// The driver configuration, the only label OMB gives it, for example `Iggy-persisted`.
pub fn driver_tag(driver: &str) -> Html {
    html! { <span class="omb-driver" title={DRIVER_HINT}>{ driver.to_owned() }</span> }
}

/// The version tag, styled as a release or as a build. Empty without a version.
pub fn version_view(summary: &OmbRunSummary) -> Html {
    match (version_text(summary), &summary.version) {
        (Some(text), Some(version)) => version_tag_view(&text, version.release),
        _ => Html::default(),
    }
}

/// The run time as the summary writes it, with its source on hover, for example
/// `2026-11-06 13:22:01 UTC`.
pub fn time_view(time: &OmbRunTime) -> Html {
    html! {
        <time datetime={machine_time(time)} title={time.source.to_string()}>
            { time.text.clone() }
        </time>
    }
}

/// The day of the run, for a table, with the full time and its source on hover.
pub fn day_view(time: &OmbRunTime) -> Html {
    html! {
        <time datetime={machine_time(time)} title={format!("{} ({})", time.text, time.source)}>
            { time.instant.format(DAY_FORMAT).to_string() }
        </time>
    }
}

fn machine_time(time: &OmbRunTime) -> String {
    match time.source {
        OmbTimeSource::FileName => time.instant.format(LOCAL_TIME_FORMAT).to_string(),
        OmbTimeSource::Sidecar | OmbTimeSource::FileModified => {
            time.instant.to_rfc3339_opts(SecondsFormat::Secs, true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_ALL, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS, OMB_RC1_DEFAULT,
        OMB_RC1_PERSISTED, omb_rerun, omb_run_list,
    };
    use bench_dashboard_shared::omb::chart::{PERCENTILE_LABELS, percentile_values};

    fn ids(runs: &[&OmbRunSummary]) -> Vec<OmbRunId> {
        runs.iter().map(|run| run.id).collect()
    }

    #[test]
    fn given_time_sources_when_shown_should_keep_zone_only_where_known() {
        let sidecar = OMB_RC1_DEFAULT.summary().time;
        let file_name = OMB_NO_CONSUMERS.summary().time;

        assert_eq!(sidecar.text, "2026-11-06 13:22:01 UTC");
        assert_eq!(machine_time(&sidecar), "2026-11-06T13:22:01Z");
        assert_eq!(file_name.source, OmbTimeSource::FileName);
        assert_eq!(machine_time(&file_name), "2026-10-22T10:21:15");
    }

    #[test]
    fn given_fixture_percentiles_when_listed_should_follow_labels_with_p99_at_its_index() {
        let latency = OMB_RC1_DEFAULT.summary().publish_latency_ms;

        let values = percentile_values(&latency);

        assert_eq!(PERCENTILE_LABELS[P99_INDEX], "P99");
        assert_eq!(values[P99_INDEX], 1.523);
        assert_eq!(values[0], latency.p50);
        assert_eq!(values[6], latency.max);
        assert!(!values.contains(&latency.avg));
    }

    #[test]
    fn given_changes_that_round_to_zero_when_written_should_have_no_sign() {
        assert_eq!(change_text(0.04), "0.0%");
        assert_eq!(change_text(-0.04), "0.0%");
        assert_eq!(change_text(0.0), "0.0%");
        assert_eq!(change_text(-0.0), "0.0%");
        assert_eq!(change_text(0.05), "+0.1%");
        assert_eq!(change_text(-4.56), "-4.6%");
        assert_eq!(change_text(127.5), "+127.5%");
    }

    #[test]
    fn given_fixture_end_to_end_latency_when_read_should_tell_measured_from_not_measured() {
        let measured = EndToEnd::of(&OMB_PUBLISH_ERRORS.summary());
        let no_consumers = EndToEnd::of(&OMB_NO_CONSUMERS.summary());
        let mut consumers_without_numbers = OMB_RC1_DEFAULT.summary();
        consumers_without_numbers.end_to_end_latency_ms = None;

        let Some(latency) = OMB_PUBLISH_ERRORS.summary().end_to_end_latency_ms else {
            panic!("the run with errors has consumers and measured end-to-end latency");
        };
        assert_eq!(measured, EndToEnd::Measured(latency));
        assert_eq!(latency.max, 10_003.007);
        assert_eq!(measured.note(), None);
        assert_eq!(no_consumers, EndToEnd::NoConsumers);
        assert_eq!(no_consumers.note(), Some(NO_CONSUMERS_NOTE));
        assert_eq!(
            EndToEnd::of(&consumers_without_numbers),
            EndToEnd::NotInFile
        );
        assert_eq!(EndToEnd::NotInFile.note(), Some(NOT_IN_FILE_NOTE));
    }

    #[test]
    fn given_newer_version_that_ran_earlier_when_lined_up_should_still_follow_versions() {
        let edge8 = OMB_EDGE8_DEFAULT.summary();
        let rc1 = omb_rerun(&OMB_RC1_DEFAULT.summary(), "00000000000000aa", -24 * 30);

        let (order, line) = line_order([&rc1, &edge8]);

        assert_eq!(order, LineOrder::Version);
        assert_eq!(ids(&line), [edge8.id, rc1.id]);
    }

    #[test]
    fn given_run_without_version_on_line_when_lined_up_should_follow_run_times() {
        let rc1 = OMB_RC1_DEFAULT.summary();
        let mut edge8 = OMB_EDGE8_DEFAULT.summary();
        edge8.version = None;
        let later = omb_rerun(&edge8, "00000000000000bb", 24 * 30);

        let (order, line) = line_order([&later, &rc1, &edge8]);

        assert_eq!(order, LineOrder::Time);
        assert_eq!(ids(&line), [edge8.id, rc1.id, later.id]);
    }

    #[test]
    fn given_older_version_and_newer_run_without_version_when_trayed_should_follow_run_times() {
        let older = OMB_RC1_DEFAULT.summary();
        let mut newer = omb_rerun(&older, "00000000000000bc", 24 * 4);
        newer.version = None;
        let runs = [newer.clone(), older.clone()];

        let tray = tray_runs(&[newer.id, older.id], &runs);

        assert_eq!(ids(&tray), [older.id, newer.id]);
        assert_eq!(ids(&oldest_first([&newer, &older])), [older.id, newer.id]);
    }

    #[test]
    fn given_picks_of_two_versions_when_trayed_should_list_oldest_version_first() {
        let list = omb_run_list();
        let picked = [
            OMB_RC1_PERSISTED.id(),
            OMB_EDGE8_DEFAULT.id(),
            OMB_RC1_DEFAULT.id(),
        ];

        let tray = tray_runs(&picked, &list.runs);

        assert_eq!(
            ids(&tray),
            [
                OMB_EDGE8_DEFAULT.id(),
                OMB_RC1_DEFAULT.id(),
                OMB_RC1_PERSISTED.id()
            ]
        );
    }

    #[test]
    fn given_pick_missing_from_list_when_trayed_should_leave_it_out() {
        let runs = [OMB_RC1_DEFAULT.summary()];
        let picked = [OMB_NO_CONSUMERS.id(), OMB_RC1_DEFAULT.id()];

        assert_eq!(ids(&tray_runs(&picked, &runs)), [OMB_RC1_DEFAULT.id()]);
    }

    #[test]
    fn given_lists_when_checked_for_empty_state_should_tell_no_directory_from_no_runs() {
        let no_directory = OmbRunList::default();
        let no_runs = OmbRunList {
            enabled: true,
            runs: Vec::new(),
        };

        assert!(empty_state(&no_directory).is_some());
        assert!(empty_state(&no_runs).is_some());
        assert_ne!(empty_state(&no_directory), empty_state(&no_runs));
        assert_eq!(empty_state(&omb_run_list()), None);
        assert_eq!(omb_run_list().runs.len(), OMB_ALL.len());
    }
}
