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

//! The OMB compare page: two or three OMB runs side by side, each against the baseline, the first
//! run in the link. It takes OMB runs only, so an iggy-bench run id in its link is a run that isn't
//! found. The link holds the whole state, so a shared link shows the same page.

mod chart;
mod matrix;
mod settings;
mod summary;

use bench_dashboard_shared::labels::messages_text;
use bench_dashboard_shared::omb::compare::OmbComparability;
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::labels::{conditions, environment_text, run_name, short_id};
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary};
use yew::prelude::*;
use yew_router::Routable;
use yew_router::prelude::{Link, use_location, use_navigator};

use self::chart::{ChartKind, ChartRun, CompareChart};
use self::matrix::{
    Cell, CellChange, Latency, MatrixRow, consume_row, latency_rows, summary_changes,
    throughput_row,
};
use self::settings::{SettingRow, setting_rows};
use self::summary::{drivers_text, level_fields, level_look, level_texts, properties_text};
use crate::api::fetch_omb_run;
use crate::components::chart::series_color;
use crate::components::headline::neutral_chip;
use crate::components::page::{breadcrumbs, page_shell, section_head};
use crate::components::section_nav::{NavSection, SectionNav};
use crate::components::share_button::ShareButton;
use crate::pages::compare::RUN_COLORS;
use crate::pages::compare::runs::{
    RUNS_PARAM, RunEntry, RunList, opens_elsewhere, with_baseline, without,
};
use crate::pages::omb::common::{
    CONSUME_HINT, END_TO_END_LATENCY_HINT, EndToEnd, MESSAGES_HINT, OmbCompareLink,
    PUBLISH_LATENCY_HINT, PUBLISH_THROUGHPUT_HINT, driver_tag, omb_crumb, omb_page_head, run_route,
    runs_status, time_view, version_view,
};
use crate::router::AppRoute;
use crate::state::data::{Loadable, fetch_into};
use crate::state::omb::{OmbData, use_omb_data};
use crate::state::selection::{MAX_COMPARED_RUNS, SelectionAction, use_omb_selection};

const PAGE_TITLE: &str = "Compare OMB runs";
const CRUMB: &str = "Compare";
const SUMMARY_TITLE: &str = "What changed";
const BASELINE: &str = "baseline";
const CHANGE_NOTE: &str = "A change is the percent from the baseline. OMB runs have no noise rule \
                           yet, so the page doesn't judge a change, and every change is gray. Runs \
                           of different workloads show their values side by side, with no change. \
                           Point at a change to see the two values that it compares.";
const P99_CHART_NOTE: &str = "The P99 of the publish latency in each 10 second window of the test \
                              phase. OMB doesn't record the warm-up.";
const NO_END_TO_END_NOTE: &str = "No run measured the end-to-end latency, because no run had \
                                  consumers.";
const NO_END_TO_END_NUMBERS_NOTE: &str = "No run has end-to-end latency numbers.";
const NO_CONSUMERS_REASON: &str = "a run without consumers doesn't measure the end-to-end latency.";
const NOT_IN_FILE_REASON: &str = "the result file holds no end-to-end latency.";
const NO_SETUP_NOTE: &str = "No setup recorded";
const LATENCY: NavSection = NavSection {
    id: "latency",
    label: "Latency",
};
const THROUGHPUT: NavSection = NavSection {
    id: "throughput",
    label: "Throughput",
};
const DISTRIBUTION: NavSection = NavSection {
    id: "distribution",
    label: "Distribution",
};
const CONFIGURATION: NavSection = NavSection {
    id: "configuration",
    label: "Configuration",
};
static SECTIONS: [NavSection; 4] = [LATENCY, THROUGHPUT, DISTRIBUTION, CONFIGURATION];

#[function_component(OmbComparePage)]
pub fn omb_compare_page() -> Html {
    let data = use_omb_data();
    let scroll_ref = use_node_ref();
    let list: RunList<OmbRunId> = use_location()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .and_then(|pairs| pairs.into_iter().find(|(key, _)| key == RUNS_PARAM))
        .map(|(_, runs)| RunList::from(runs.as_str()))
        .unwrap_or_default();
    let lookup = use_memo((data.runs.clone(), list), |(runs, list)| {
        Lookup::new(runs, list)
    });
    let fetched: Vec<OmbRunId> = match &*lookup {
        Lookup::Ready(view) => view.ids(),
        Lookup::Pending => Vec::new(),
    };
    let details = use_state_eq(|| Loadable::Loading);
    let load_details = {
        let (details, fetched) = (details.clone(), fetched.clone());
        Callback::from(move |()| {
            let ids = fetched.clone();
            fetch_into(details.clone(), async move {
                let mut fetched = Vec::with_capacity(ids.len());
                for id in ids {
                    fetched.push(fetch_omb_run(id).await?);
                }
                Ok(fetched)
            });
        })
    };
    // Only the charts need the details, and the page has charts from two runs on. It fetches when
    // the details it holds miss one of the link's runs. After a removal or a new baseline, it keeps
    // them, so the charts redraw without loading. A late answer for an older link that misses a run
    // makes it fetch again.
    let held = holds_every(&details, &fetched);
    {
        let load_details = load_details.clone();
        use_effect_with((fetched, held), move |(fetched, held)| {
            if fetched.len() > 1 && !held {
                load_details.emit(());
            }
        });
    }

    let content = match &*lookup {
        Lookup::Pending => pending_view(&data),
        Lookup::Ready(view) => compare_view(view, &details, &load_details, &scroll_ref),
    };
    page_shell(&scroll_ref, content)
}

/// The link's runs in the loaded list.
enum Lookup {
    /// The list is loading or failed.
    Pending,
    Ready(Box<CompareView>),
}

/// What the page shows from the run list. Built once per load and per link.
struct CompareView {
    /// The link's runs that are in the loaded list, baseline first.
    runs: Vec<OmbRunSummary>,
    /// The link's entries that don't name a loaded OMB run, as written in the link.
    missing: Vec<String>,
    /// The entries past the page's limit.
    dropped: usize,
}

/// What the page says about the runs without end-to-end numbers.
#[derive(Debug, PartialEq, Eq)]
enum MissingEndToEnd {
    /// One note per reason, each naming the runs without numbers for that reason. Empty when every
    /// run has numbers.
    Runs(Vec<String>),
    /// No run has numbers. Only when no run had consumers does the note blame the consumers.
    Every(&'static str),
}

impl Lookup {
    fn new(runs: &Loadable<OmbRunList>, list: &RunList<OmbRunId>) -> Self {
        let Loadable::Ready(loaded) = runs else {
            return Self::Pending;
        };
        let mut found = Vec::with_capacity(list.entries.len());
        let mut missing = Vec::new();
        for entry in &list.entries {
            match entry {
                RunEntry::Id(id) => match loaded.runs.iter().find(|run| run.id == *id) {
                    Some(run) => found.push(run.clone()),
                    None => missing.push(id.to_string()),
                },
                RunEntry::Invalid(text) => missing.push(text.clone()),
            }
        }
        Self::Ready(Box::new(CompareView {
            runs: found,
            missing,
            dropped: list.dropped,
        }))
    }
}

impl CompareView {
    fn ids(&self) -> Vec<OmbRunId> {
        self.runs.iter().map(|run| run.id).collect()
    }

    fn refs(&self) -> Vec<&OmbRunSummary> {
        self.runs.iter().collect()
    }

    /// How comparable each run except the baseline is with the baseline.
    fn levels(&self) -> Vec<OmbComparability> {
        self.runs
            .split_first()
            .map_or_else(Vec::new, |(baseline, others)| {
                others
                    .iter()
                    .map(|run| OmbComparability::between(baseline, run))
                    .collect()
            })
    }
}

impl MissingEndToEnd {
    fn of(runs: &[OmbRunSummary]) -> Self {
        let names = |reason: EndToEnd| -> Vec<String> {
            runs.iter()
                .filter(|run| EndToEnd::of(run) == reason)
                .map(run_name)
                .collect()
        };
        let (no_consumers, not_in_file) =
            (names(EndToEnd::NoConsumers), names(EndToEnd::NotInFile));
        if no_consumers.len() + not_in_file.len() == runs.len() {
            return Self::Every(if not_in_file.is_empty() {
                NO_END_TO_END_NOTE
            } else {
                NO_END_TO_END_NUMBERS_NOTE
            });
        }
        Self::Runs(
            [
                (no_consumers, NO_CONSUMERS_REASON),
                (not_in_file, NOT_IN_FILE_REASON),
            ]
            .into_iter()
            .filter(|(names, _)| !names.is_empty())
            .map(|(names, reason)| format!("No line for {}: {reason}", names.join(", ")))
            .collect(),
        )
    }
}

/// Whether `details` has the detail of every one of `runs`, as it does after a removal or a new
/// baseline.
fn holds_every(details: &Loadable<Vec<OmbRunDetail>>, runs: &[OmbRunId]) -> bool {
    match details {
        Loadable::Ready(details) => runs
            .iter()
            .all(|id| details.iter().any(|detail| detail.summary.id == *id)),
        Loadable::Loading | Loadable::Failed(_) => false,
    }
}

fn pending_view(data: &OmbData) -> Html {
    html! {
        <>
            { head("") }
            <section class="page-container">{ runs_status(data) }</section>
        </>
    }
}

fn head(lead: &str) -> Html {
    omb_page_head(
        breadcrumbs(&[omb_crumb()], CRUMB),
        None,
        PAGE_TITLE,
        lead,
        html! { <ShareButton class={classes!("run-action")} labeled={true} /> },
    )
}

fn compare_view(
    view: &CompareView,
    details: &Loadable<Vec<OmbRunDetail>>,
    reload: &Callback<()>,
    scroll_ref: &NodeRef,
) -> Html {
    let lead = match view.runs.first() {
        Some(baseline) if view.runs.len() > 1 => format!(
            "{} OMB runs, each against the baseline {}.",
            view.runs.len(),
            run_name(baseline)
        ),
        _ => "Two or three OMB runs side by side, each against the first run, the baseline."
            .to_owned(),
    };
    if view.runs.len() < 2 {
        return html! {
            <>
                { head(&lead) }
                <section class="page-container compare-head">
                    { notes(view) }
                    { run_cards(view) }
                    { ask_view(view) }
                </section>
            </>
        };
    }
    let chart_runs = chart_runs(view);
    let chart = |kind: ChartKind| {
        html! {
            <CompareChart
                {kind}
                runs={chart_runs.clone()}
                details={details.clone()}
                reload={reload.clone()}
            />
        }
    };
    let sections: &'static [NavSection] = &SECTIONS;
    html! {
        <>
            { head(&lead) }
            <section class="page-container compare-head">
                { notes(view) }
                { summary_card(view) }
                { run_cards(view) }
            </section>
            <SectionNav {sections} scroll_ref={scroll_ref.clone()} />

            <section id={LATENCY.id} class="page-section page-container">
                { section_head(
                    LATENCY.label,
                    "Latency against the baseline",
                    "One line per run with the publish P99 of each 10 second window. The table gives the \
                     percentiles over the whole run, from the merged histograms of every producer \
                     and consumer, with each run's change from the baseline.",
                ) }
                <article class="card run-chart-card">
                    <h3>{"Publish P99"}</h3>
                    { chart(ChartKind::PublishP99) }
                    <p class="card-note">{ P99_CHART_NOTE }</p>
                </article>
                { latency_table(view) }
            </section>

            <section id={THROUGHPUT.id} class="page-section page-container">
                { section_head(
                    THROUGHPUT.label,
                    "Throughput against the baseline",
                    "One line per run with the publish throughput of each 10 second window. The \
                     card next to the chart gives each run's publish and consume rates and their \
                     change.",
                ) }
                <div class="chart-with-card">
                    <article class="card run-chart-card">
                        <h3>{"Publish throughput"}</h3>
                        { chart(ChartKind::PublishThroughput) }
                    </article>
                    { throughput_card(view) }
                </div>
            </section>

            <section id={DISTRIBUTION.id} class="page-section page-container">
                { section_head(
                    DISTRIBUTION.label,
                    "Latency distribution",
                    "The quantile curves of the merged histograms, one line per run. The steps \
                     from P90 to P99 and on to P99.9 are equal, so the tail gets room.",
                ) }
                <div class="omb-compare-curves">
                    <article class="card run-chart-card">
                        <h3 title={PUBLISH_LATENCY_HINT}>{"Publish latency"}</h3>
                        { chart(ChartKind::PublishQuantiles) }
                    </article>
                    <article class="card run-chart-card">
                        <h3 title={END_TO_END_LATENCY_HINT}>{"End-to-end latency"}</h3>
                        { end_to_end_chart(view, chart(ChartKind::EndToEndQuantiles)) }
                    </article>
                </div>
            </section>

            <section id={CONFIGURATION.id} class="page-section page-container">
                { section_head(
                    CONFIGURATION.label,
                    "What the runs share",
                    "Each run's workload, the setup from its sidecar, and one row per sidecar \
                     property. The rows that differ from the baseline come first.",
                ) }
                { settings_view(view) }
            </section>
        </>
    }
}

fn notes(view: &CompareView) -> Html {
    html! {
        <>
            { for view.missing.iter().map(|entry| html! {
                <p class="data-status data-failed" role="alert">
                    { format!("Run not found: {entry}.") }
                </p>
            }) }
            if view.dropped > 0 {
                <p class="data-status">
                    { format!(
                        "The link names {} more {}. The page compares at most {MAX_COMPARED_RUNS} \
                         runs, so anything past that is left out.",
                        view.dropped,
                        if view.dropped == 1 { "run" } else { "runs" }
                    ) }
                </p>
            }
        </>
    }
}

/// The furthest level of the runs, which limits what the page can say, with each run's own level,
/// what differs at that level, and its changes. A run of another workload has no change.
fn summary_card(view: &CompareView) -> Html {
    let levels = view.levels();
    let Some(level) = levels.iter().max() else {
        return Html::default();
    };
    let runs = view.refs();
    let changes = summary_changes(&runs);
    let others = view
        .runs
        .iter()
        .zip(RUN_COLORS)
        .skip(1)
        .zip(&levels)
        .zip(&changes);
    html! {
        <div class={classes!("card", "compare-summary", level_look(level))} role="note">
            <span class="eyebrow">{ SUMMARY_TITLE }</span>
            <strong class="compare-summary-level">{ level.to_string() }</strong>
            { for level_texts(&levels).into_iter().map(|text| html! { <p>{ text }</p> }) }
            <ul class="compare-summary-runs">
                { for others.map(|(((run, color), run_level), run_changes)| {
                    summary_line(run, color, run_level, run_changes)
                }) }
            </ul>
            <div class="omb-compare-notes">
                if let Some(text) = drivers_text(&runs) {
                    <p>{ text }</p>
                }
                if let Some(text) = properties_text(&runs) {
                    <p>{ text }</p>
                }
            </div>
        </div>
    }
}

fn summary_line(
    run: &OmbRunSummary,
    color: &str,
    level: &OmbComparability,
    changes: &[(&'static str, CellChange)],
) -> Html {
    html! {
        <li class="compare-summary-run" style={series_color(color)}>
            <span class="compare-summary-name">
                <span class="series-swatch" aria-hidden="true"></span>
                { run_name(run) }
            </span>
            <span class={classes!("compare-level", level_look(level))}>{ level.to_string() }</span>
            if let Some(fields) = level_fields(level) {
                <span class="compare-summary-change">{ fields }</span>
            }
            { for changes.iter().map(|(label, change)| html! {
                <span class="compare-summary-change">
                    { *label }
                    { change_chip(change) }
                </span>
            }) }
        </li>
    }
}

fn run_cards(view: &CompareView) -> Html {
    let ids = view.ids();
    let levels = view.levels();
    let cards = view
        .runs
        .iter()
        .zip(RUN_COLORS)
        .enumerate()
        .map(|(position, (run, color))| {
            let level = position.checked_sub(1).and_then(|other| levels.get(other));
            run_card(run, color, level, &ids)
        });
    html! {
        <div class="compare-runs">
            { for cards }
            if (2..MAX_COMPARED_RUNS).contains(&view.runs.len()) {
                <div class="card compare-run compare-run-add">
                    <p>{"Compare one more OMB run against the baseline."}</p>
                    <AddRunLink runs={ids.clone()}>{"Add a run"}</AddRunLink>
                </div>
            }
        </div>
    }
}

/// `level` is `None` for the baseline. The run time says where it comes from, because a sidecar
/// gives the start of the run and a file name or a file time gives its end.
fn run_card(
    run: &OmbRunSummary,
    color: &'static str,
    level: Option<&OmbComparability>,
    ids: &[OmbRunId],
) -> Html {
    let environment = run
        .environment()
        .and_then(environment_text)
        .unwrap_or_else(|| NO_SETUP_NOTE.to_owned());
    html! {
        <article class="card compare-run" style={series_color(color)}>
            <div class="compare-run-top">
                <span class="series-swatch" aria-hidden="true"></span>
                { version_view(run) }
                { driver_tag(&run.driver) }
                if let Some(level) = level {
                    <span class={classes!("compare-level", level_look(level))}>
                        { level.to_string() }
                    </span>
                } else {
                    <span class="compare-level compare-baseline">{"Baseline"}</span>
                }
            </div>
            <h2 class="compare-run-title">{ run.workload.name.clone() }</h2>
            <p class="run-meta compare-run-meta">
                <span>{ short_id(run.id) }</span>
                <span>
                    { time_view(&run.time) }
                    <small class="omb-compare-time-source">
                        { format!(" ({})", run.time.source) }
                    </small>
                </span>
            </p>
            <p class="compare-run-conditions">{ conditions(run) }</p>
            <p class="compare-run-conditions omb-compare-environment">{ environment }</p>
            <div class="compare-run-actions">
                <Link<AppRoute> to={run_route(run.id)} classes={classes!("run-action")}>
                    {"Open run"}
                </Link<AppRoute>>
                if level.is_some() {
                    <OmbCompareLink runs={with_baseline(ids, run.id)} classes={classes!("run-action")}>
                        {"Use as baseline"}
                    </OmbCompareLink>
                }
                if ids.len() > 1 {
                    <OmbCompareLink runs={without(ids, run.id)} classes={classes!("run-action")}>
                        {"Remove"}
                    </OmbCompareLink>
                }
            </div>
        </article>
    }
}

fn ask_view(view: &CompareView) -> Html {
    let ask = if view.runs.is_empty() {
        "Pick two or three runs in the OMB run list to compare them."
    } else {
        "Add a run from the OMB run list to compare it with this one."
    };
    html! {
        <p class="compare-ask">
            <span>{ ask }</span>
            <AddRunLink runs={view.ids()}>{"Open the OMB runs"}</AddRunLink>
        </p>
    }
}

#[derive(Properties, PartialEq)]
struct AddRunLinkProps {
    /// The page's runs, which the OMB tray takes over.
    runs: Vec<OmbRunId>,
    children: Html,
}

/// Opens the OMB run list with the page's runs in the OMB tray, so one more pick completes the
/// comparison. A page without runs leaves the tray as it is.
#[function_component(AddRunLink)]
fn add_run_link(props: &AddRunLinkProps) -> Html {
    let navigator = use_navigator();
    let selection = use_omb_selection();
    let runs = props.runs.clone();
    let onclick = Callback::from(move |event: MouseEvent| {
        let Some(navigator) = navigator.as_ref().filter(|_| !opens_elsewhere(&event)) else {
            return;
        };
        event.prevent_default();
        if let Some(selection) = selection.as_ref().filter(|_| !runs.is_empty()) {
            selection.dispatch(SelectionAction::Replace(runs.clone()));
        }
        navigator.push(&AppRoute::OmbRuns);
    });
    html! {
        <a href={AppRoute::OmbRuns.to_path()} class="run-action" {onclick}>
            { props.children.clone() }
        </a>
    }
}

fn chart_runs(view: &CompareView) -> Vec<ChartRun> {
    view.runs
        .iter()
        .zip(RUN_COLORS)
        .map(|(run, color)| ChartRun {
            id: run.id,
            name: run_name(run),
            color,
        })
        .collect()
}

/// A run without end-to-end numbers has no line, and a note names it with the reason.
fn end_to_end_chart(view: &CompareView, chart: Html) -> Html {
    match MissingEndToEnd::of(&view.runs) {
        MissingEndToEnd::Every(note) => html! { <p class="data-status">{ note }</p> },
        MissingEndToEnd::Runs(notes) => html! {
            <>
                { chart }
                { for notes.into_iter().map(|note| html! { <p class="card-note">{ note }</p> }) }
            </>
        },
    }
}

fn run_heads(view: &CompareView, first: &'static str) -> Html {
    html! {
        <tr>
            <th scope="col">{ first }</th>
            { for view.runs.iter().zip(RUN_COLORS).enumerate().map(|(position, (run, color))| html! {
                <th scope="col" class="run-table-number compare-run-head" style={series_color(color)}>
                    <span class="series-swatch" aria-hidden="true"></span>
                    { run_name(run) }
                    if position == 0 {
                        <small>{ BASELINE }</small>
                    }
                </th>
            }) }
        </tr>
    }
}

/// What a cell repeats from its column head. The phone layout hides the head, so the cell carries
/// it.
struct Column {
    /// The run's name, with the baseline marked.
    run: AttrValue,
    /// The run's color.
    style: AttrValue,
}

fn columns(view: &CompareView) -> Vec<Column> {
    view.runs
        .iter()
        .zip(RUN_COLORS)
        .enumerate()
        .map(|(position, (run, color))| {
            let name = run_name(run);
            let run = if position == 0 {
                format!("{name} ({BASELINE})")
            } else {
                name
            };
            Column {
                run: run.into(),
                style: series_color(color).into(),
            }
        })
        .collect()
}

/// The publish percentiles, then the end-to-end percentiles of the runs that measured them. A run
/// without end-to-end numbers shows "not measured" in the end-to-end rows.
fn latency_table(view: &CompareView) -> Html {
    let runs = view.refs();
    let columns = columns(view);
    let colspan = (view.runs.len() + 1).to_string();
    let no_end_to_end = match MissingEndToEnd::of(&view.runs) {
        MissingEndToEnd::Every(note) => Some(note),
        MissingEndToEnd::Runs(_) => None,
    };
    let part = |title: &'static str, hint: &'static str, latency: Latency| {
        html! {
            <tbody>
                <tr class="compare-part">
                    <th scope="colgroup" colspan={colspan.clone()} title={hint}>{ title }</th>
                </tr>
                { for latency_rows(&runs, latency).iter().map(|row| matrix_row(row, &columns)) }
            </tbody>
        }
    };
    html! {
        <div class="card run-table-card">
            <table class="run-table compare-table compare-latency-table">
                <thead>{ run_heads(view, "Percentile") }</thead>
                { part("Publish latency", PUBLISH_LATENCY_HINT, Latency::Publish) }
                if no_end_to_end.is_none() {
                    { part("End-to-end latency", END_TO_END_LATENCY_HINT, Latency::EndToEnd) }
                }
            </table>
            if let Some(note) = no_end_to_end {
                <p class="card-note compare-table-note">{ note }</p>
            }
            <p class="card-note compare-table-note">{ CHANGE_NOTE }</p>
        </div>
    }
}

fn matrix_row(row: &MatrixRow, columns: &[Column]) -> Html {
    html! {
        <tr>
            <th scope="row">{ row.label }</th>
            { for row.cells.iter().zip(columns).map(|(cell, column)| matrix_cell(cell, column)) }
        </tr>
    }
}

/// Every cell has a slot for its change, even the baseline's, so the values of every column sit
/// at the same place under their heads.
fn matrix_cell(cell: &Cell, column: &Column) -> Html {
    html! {
        <td
            class="run-table-number"
            data-label={column.run.clone()}
            style={column.style.clone()}
        >
            <span class="compare-cell">
                <span class={classes!("compare-value", (!cell.measured).then_some("omb-missing"))}>
                    { cell.text.clone() }
                </span>
                <span class="compare-cell-change">
                    if let Some(change) = &cell.change {
                        { change_chip(change) }
                    }
                </span>
            </span>
        </td>
    }
}

/// Each run's publish throughput with its change, its message rate and its consume rate with its
/// change, and a bar against the fastest run, so the runs read at a glance. The message rate of one
/// workload moves with its throughput, so its change would only repeat the throughput's.
fn throughput_card(view: &CompareView) -> Html {
    let runs = view.refs();
    let (throughput, consume) = (throughput_row(&runs), consume_row(&runs));
    let fastest = view
        .runs
        .iter()
        .map(|run| run.publish_mebibytes_per_second)
        .fold(0.0, f64::max);
    let items = view
        .runs
        .iter()
        .zip(RUN_COLORS)
        .zip(throughput.cells.iter().zip(&consume.cells))
        .enumerate()
        .map(|(position, ((run, color), (rate, consumed)))| {
            let share = if fastest > 0.0 {
                run.publish_mebibytes_per_second / fastest * 100.0
            } else {
                0.0
            };
            html! {
                <li class="rates-row" style={series_color(color)}>
                    <span class="rates-name">
                        <span class="series-swatch" aria-hidden="true"></span>
                        <span class="rates-label">{ run_name(run) }</span>
                    </span>
                    <span class="rates-change">
                        if position == 0 {
                            <span class="rates-baseline">{ BASELINE }</span>
                        } else if let Some(change) = &rate.change {
                            { change_chip(change) }
                        }
                    </span>
                    <strong class="rates-value" title={PUBLISH_THROUGHPUT_HINT}>
                        { rate.text.clone() }
                    </strong>
                    <span class="rates-messages" title={MESSAGES_HINT}>
                        { messages_text(run.publish_messages_per_second) }
                    </span>
                    <span class="omb-compare-consume" title={CONSUME_HINT}>
                        <span>
                            { format!("{}: ", consume.label) }
                            <span class={classes!((!consumed.measured).then_some("omb-missing"))}>
                                { consumed.text.clone() }
                            </span>
                        </span>
                        if let Some(change) = &consumed.change {
                            { change_chip(change) }
                        }
                    </span>
                    <span class="rates-bar" aria-hidden="true">
                        <span style={format!("width: {share:.1}%")}></span>
                    </span>
                </li>
            }
        });
    html! {
        <article class="card rates-card">
            <h3>{"Against the baseline"}</h3>
            <ul class="rates-list">{ for items }</ul>
        </article>
    }
}

fn change_chip(change: &CellChange) -> Html {
    neutral_chip(change.text.clone(), change.values.clone())
}

fn settings_view(view: &CompareView) -> Html {
    let rows = setting_rows(&view.refs());
    let (differing, equal): (Vec<&SettingRow>, Vec<&SettingRow>) =
        rows.iter().partition(|row| row.differs);
    let columns = columns(view);
    let colspan = (view.runs.len() + 1).to_string();
    let part = |title: &'static str, rows: &[&SettingRow]| {
        if rows.is_empty() {
            return Html::default();
        }
        html! {
            <tbody>
                <tr class="compare-part">
                    <th scope="colgroup" colspan={colspan.clone()}>{ title }</th>
                </tr>
                { for rows.iter().map(|row| setting_row(row, &columns)) }
            </tbody>
        }
    };
    html! {
        <div class="card run-table-card">
            <table class="run-table compare-table">
                <thead>{ run_heads(view, "Setting") }</thead>
                { part("Differs from the baseline", &differing) }
                { part("Same in every run", &equal) }
            </table>
        </div>
    }
}

fn setting_row(row: &SettingRow, columns: &[Column]) -> Html {
    let baseline = row.values.first();
    html! {
        <tr class={classes!(row.differs.then_some("compare-differs"))}>
            <th scope="row">{ row.label.clone() }</th>
            { for row.values.iter().zip(columns).map(|(value, column)| html! {
                <td
                    class={classes!((Some(value) != baseline).then_some("compare-cell-differs"))}
                    data-label={column.run.clone()}
                    style={column.style.clone()}
                >
                    { value.clone() }
                </td>
            }) }
        </tr>
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use uuid::Uuid;

    use super::*;
    use crate::test_support::{
        OMB_ALL, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED,
        omb_rerun, omb_run_list,
    };

    /// The padding that turns an OMB run id into a 32 digit UUID.
    const UUID_PADDING: &str = "0000000000000000";

    fn loaded() -> Loadable<OmbRunList> {
        Loadable::Ready(Rc::new(omb_run_list()))
    }

    fn view(link: &str) -> CompareView {
        match Lookup::new(&loaded(), &RunList::from(link)) {
            Lookup::Ready(view) => *view,
            Lookup::Pending => panic!("the list is loaded"),
        }
    }

    #[test]
    fn given_link_of_fixture_runs_when_looked_up_should_keep_link_order_with_baseline_first() {
        let runs = [
            OMB_RC1_PERSISTED.id(),
            OMB_EDGE8_DEFAULT.id(),
            OMB_RC1_DEFAULT.id(),
        ];

        let view = view(&format!("{},{},{}", runs[0], runs[1], runs[2]));

        assert_eq!(view.ids(), runs);
        assert!(view.missing.is_empty());
        assert_eq!(view.dropped, 0);
    }

    #[test]
    fn given_iggy_bench_id_in_link_when_looked_up_should_report_run_not_found() {
        let uuid = Uuid::from_u128(7).to_string();

        let view = view(&format!("{},{uuid}", OMB_RC1_DEFAULT.id()));

        assert_eq!(view.ids(), [OMB_RC1_DEFAULT.id()]);
        assert_eq!(view.missing, [uuid]);
    }

    #[test]
    fn given_uuid_that_starts_with_omb_run_id_when_parsed_should_name_no_omb_run() {
        let id = OMB_RC1_DEFAULT.id();
        let uuid: Uuid = format!("{id}{UUID_PADDING}")
            .parse()
            .expect("32 hex digits are a UUID");
        let (hyphenated, simple) = (uuid.to_string(), uuid.simple().to_string());
        assert!(simple.starts_with(&id.to_string()));

        let list = RunList::<OmbRunId>::from(format!("{hyphenated},{simple}").as_str());
        let view = view(&format!("{hyphenated},{simple}"));

        assert_eq!(
            list.entries,
            [
                RunEntry::Invalid(hyphenated.clone()),
                RunEntry::Invalid(simple.clone())
            ]
        );
        assert!(view.runs.is_empty());
        assert_eq!(view.missing, [hyphenated, simple]);
    }

    #[test]
    fn given_every_fixture_in_link_when_looked_up_should_count_runs_past_limit() {
        let link = OMB_ALL
            .iter()
            .map(|fixture| fixture.id().to_string())
            .collect::<Vec<_>>()
            .join(",");

        let view = view(&link);

        assert_eq!(view.runs.len(), MAX_COMPARED_RUNS);
        assert_eq!(view.dropped, OMB_ALL.len() - MAX_COMPARED_RUNS);
    }

    #[test]
    fn given_list_still_loading_when_looked_up_should_wait() {
        let list = RunList::from(OMB_RC1_DEFAULT.id().to_string().as_str());

        assert!(matches!(
            Lookup::new(&Loadable::Loading, &list),
            Lookup::Pending
        ));
    }

    #[test]
    fn given_fixture_runs_when_leveled_should_compare_each_with_baseline() {
        let view = view(&format!(
            "{},{},{}",
            OMB_RC1_DEFAULT.id(),
            OMB_RC1_PERSISTED.id(),
            OMB_NO_CONSUMERS.id()
        ));

        let levels = view.levels();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], OmbComparability::LikeForLike);
        assert!(matches!(levels[1], OmbComparability::WorkloadDiffers(_)));
    }

    #[test]
    fn given_run_with_consumers_but_no_end_to_end_numbers_when_noted_should_not_blame_consumers() {
        let rc1 = OMB_RC1_DEFAULT.summary();
        let mut unmeasured = OMB_RC1_PERSISTED.summary();
        unmeasured.end_to_end_latency_ms = None;
        assert_eq!(unmeasured.workload.consumers_per_topic, 1);

        let some = MissingEndToEnd::of(&[rc1.clone(), unmeasured.clone()]);
        let every = MissingEndToEnd::of(&[unmeasured.clone(), OMB_NO_CONSUMERS.summary()]);

        assert_eq!(
            some,
            MissingEndToEnd::Runs(vec![format!(
                "No line for {}: {NOT_IN_FILE_REASON}",
                run_name(&unmeasured)
            )])
        );
        assert_eq!(every, MissingEndToEnd::Every(NO_END_TO_END_NUMBERS_NOTE));
    }

    #[test]
    fn given_runs_without_consumers_when_noted_should_blame_consumers() {
        let no_consumers = OMB_NO_CONSUMERS.summary();
        let rerun = omb_rerun(&no_consumers, "00000000000000ee", 2);

        let some = MissingEndToEnd::of(&[OMB_RC1_DEFAULT.summary(), no_consumers.clone()]);
        let every = MissingEndToEnd::of(&[no_consumers.clone(), rerun]);
        let none = MissingEndToEnd::of(&[OMB_RC1_DEFAULT.summary(), OMB_RC1_PERSISTED.summary()]);

        assert_eq!(
            some,
            MissingEndToEnd::Runs(vec![format!(
                "No line for {}: {NO_CONSUMERS_REASON}",
                run_name(&no_consumers)
            )])
        );
        assert_eq!(every, MissingEndToEnd::Every(NO_END_TO_END_NOTE));
        assert_eq!(none, MissingEndToEnd::Runs(Vec::new()));
    }

    #[test]
    fn given_details_of_some_runs_when_checked_should_hold_only_fetched_runs() {
        let details = Loadable::Ready(Rc::new(vec![
            OMB_RC1_DEFAULT.detail(),
            OMB_RC1_PERSISTED.detail(),
        ]));

        assert!(holds_every(&details, &[OMB_RC1_PERSISTED.id()]));
        assert!(!holds_every(
            &details,
            &[OMB_RC1_DEFAULT.id(), OMB_EDGE8_DEFAULT.id()]
        ));
        assert!(!holds_every(&Loadable::Loading, &[OMB_RC1_DEFAULT.id()]));
    }
}
