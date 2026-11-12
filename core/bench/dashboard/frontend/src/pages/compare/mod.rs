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

//! The compare page: two or three runs side by side, each against the baseline, the first run in
//! the link. The link holds the whole state, so a shared link shows the same page.

mod chart;
mod matrix;
pub mod releases;
mod runs;
mod settings;

pub use self::runs::{CompareLink, RunList, oldest_first};

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::series::SeriesMetric;
use bench_dashboard_shared::summary::{BenchmarkSummary, Comparability};
use uuid::Uuid;
use yew::prelude::*;
use yew_router::Routable;
use yew_router::prelude::{Link, use_location, use_navigator};

use self::chart::{ChartRun, OverlayChart};
use self::matrix::{
    Cell, CellChange, MatrixRow, RowKind, change_rows, report_rows, summary_changes,
};
use self::runs::{RUNS_PARAM, RunEntry, opens_elsewhere, with_baseline, without};
use self::settings::{SettingRow, setting_rows};
use crate::api::fetch_benchmark_by_uuid;
use crate::components::headline::{compact_judged_chip, judged_chip};
use crate::components::page::{
    Crumb, breadcrumbs, data_status, loadable, page_head, page_shell, section_head,
};
use crate::components::run_labels::{DayLabel, run_name, short_id, version_tag, version_tag_view};
use crate::components::section_nav::{NavSection, SectionNav};
use crate::components::share_button::ShareButton;
use crate::pages::run::chart::{CURRENT_COLOR, LATENCY_CHART_NOTE};
use crate::pages::run::facts::conditions;
use crate::pages::run::history::NO_VALUE;
use crate::router::AppRoute;
use crate::state::data::{DashboardData, Loadable, fetch_into, use_dashboard_data};
use crate::state::selection::{MAX_COMPARED_RUNS, SelectionAction, use_selection};

const PAGE_TITLE: &str = "Compare runs";
const CRUMB: &str = "Compare";
const REPORTS: &str = "the run reports";
const SUMMARY_TITLE: &str = "What changed";
const BASELINE: &str = "baseline";
const CHANGE_NOTE: &str = "A green or red change passes the delta rule. A gray change does not: it is \
                           within run-to-run noise, a run has a badge, or the parameters differ. \
                           Point at a change to see why, and the two values that it compares.";
/// The baseline takes the accent, like the page's own run on the run page. Blue and violet stay
/// distinct from it and from the colors of a better or worse change.
const RUN_COLORS: [&str; MAX_COMPARED_RUNS] = [CURRENT_COLOR, "#147ce5", "#8b5cf6"];
const LATENCY: NavSection = NavSection {
    id: "latency",
    label: "Latency",
};
const THROUGHPUT: NavSection = NavSection {
    id: "throughput",
    label: "Throughput",
};
const CONFIGURATION: NavSection = NavSection {
    id: "configuration",
    label: "Configuration",
};
static SECTIONS: [NavSection; 3] = [LATENCY, THROUGHPUT, CONFIGURATION];
/// Runs without a rate limit measure throughput only, so the page has no latency to show.
static THROUGHPUT_SECTIONS: [NavSection; 2] = [THROUGHPUT, CONFIGURATION];

#[derive(Properties, PartialEq)]
pub struct ComparePageProps {
    /// The runs of an old link, `/compare/:left/:right`, instead of the query.
    #[prop_or_default]
    pub alias: Option<RunList>,
}

#[function_component(ComparePage)]
pub fn compare_page(props: &ComparePageProps) -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let location = use_location();
    let list = props.alias.clone().unwrap_or_else(|| {
        location
            .and_then(|location| location.query::<Vec<(String, String)>>().ok())
            .and_then(|pairs| pairs.into_iter().find(|(key, _)| key == RUNS_PARAM))
            .map(|(_, runs)| RunList::from(runs.as_str()))
            .unwrap_or_default()
    });
    let lookup = use_memo(
        (data.summaries.clone(), data.config.clone(), list),
        |(summaries, config, list)| Lookup::new(summaries, config, list),
    );
    let fetched: Vec<Uuid> = match &*lookup {
        Lookup::Ready(view) => view.ids(),
        Lookup::Pending => Vec::new(),
    };
    let reports = use_state_eq(|| Loadable::Loading);
    let load_reports = {
        let (reports, fetched) = (reports.clone(), fetched.clone());
        Callback::from(move |()| {
            let uuids = fetched.clone();
            fetch_into(reports.clone(), async move {
                let mut fetched = Vec::with_capacity(uuids.len());
                for uuid in uuids {
                    fetched.push(fetch_benchmark_by_uuid(&uuid.to_string()).await?);
                }
                Ok(fetched)
            });
        })
    };
    // The page only fetches when the reports it holds are missing one of the link's runs. After a
    // removal or a new baseline, it keeps them, so their rows stay in place. A late answer for an
    // older link that's missing a run makes it fetch again.
    let held = holds_every(&reports, &fetched, |report, run| report.uuid == *run);
    {
        let load_reports = load_reports.clone();
        use_effect_with((fetched, held), move |(fetched, held)| {
            if !fetched.is_empty() && !held {
                load_reports.emit(());
            }
        });
    }

    let content = match &*lookup {
        Lookup::Pending => pending_view(&data),
        Lookup::Ready(view) => compare_view(view, &reports, &load_reports, &scroll_ref),
    };
    page_shell(&scroll_ref, content)
}

/// The link's runs in the loaded list.
enum Lookup {
    /// The list or the configuration is loading or failed.
    Pending,
    Ready(Box<CompareView>),
}

/// What the page shows before the reports arrive. Built once per load and per link.
struct CompareView {
    /// The link's runs that are in the loaded list, baseline first.
    runs: Vec<BenchmarkSummary>,
    /// The link's entries that don't name a loaded run, as written in the link.
    missing: Vec<String>,
    /// The entries past the page's limit.
    dropped: usize,
    change_rows: Vec<MatrixRow>,
}

impl Lookup {
    fn new(
        summaries: &Loadable<Vec<BenchmarkSummary>>,
        config: &Loadable<DashboardConfig>,
        list: &RunList,
    ) -> Self {
        let (Loadable::Ready(summaries), Loadable::Ready(config)) = (summaries, config) else {
            return Self::Pending;
        };
        let mut runs = Vec::with_capacity(list.entries.len());
        let mut missing = Vec::new();
        for entry in &list.entries {
            match entry {
                RunEntry::Id(id) => match summaries.iter().find(|summary| summary.uuid == *id) {
                    Some(run) => runs.push(run.clone()),
                    None => missing.push(id.to_string()),
                },
                RunEntry::Invalid(text) => missing.push(text.clone()),
            }
        }
        let refs: Vec<&BenchmarkSummary> = runs.iter().collect();
        let change_rows = change_rows(&refs, &config.thresholds);
        Self::Ready(Box::new(CompareView {
            runs,
            missing,
            dropped: list.dropped,
            change_rows,
        }))
    }
}

impl CompareView {
    fn ids(&self) -> Vec<Uuid> {
        self.runs.iter().map(|run| run.uuid).collect()
    }

    fn refs(&self) -> Vec<&BenchmarkSummary> {
        self.runs.iter().collect()
    }

    /// How comparable each run except the baseline is with the baseline.
    fn levels(&self) -> Vec<Comparability> {
        self.runs
            .split_first()
            .map_or_else(Vec::new, |(baseline, others)| {
                others
                    .iter()
                    .map(|run| baseline.comparability(run))
                    .collect()
            })
    }
}

/// The report of `run`, from reports that arrive in no particular order.
fn report_of<'a>(
    run: &BenchmarkSummary,
    reports: &'a [BenchmarkReportLight],
) -> Option<&'a BenchmarkReportLight> {
    reports.iter().find(|report| report.uuid == run.uuid)
}

/// Whether `loaded` has an item for every one of `runs`, as it does after a removal or a new
/// baseline.
fn holds_every<T>(
    loaded: &Loadable<Vec<T>>,
    runs: &[Uuid],
    is_of: impl Fn(&T, &Uuid) -> bool,
) -> bool {
    match loaded {
        Loadable::Ready(items) => runs
            .iter()
            .all(|run| items.iter().any(|item| is_of(item, run))),
        Loadable::Loading | Loadable::Failed(_) => false,
    }
}

fn pending_view(data: &DashboardData) -> Html {
    html! {
        <>
            { head("") }
            <section class="page-container">{ data_status(data) }</section>
        </>
    }
}

fn head(lead: &str) -> Html {
    page_head(
        breadcrumbs(&[Crumb::new("Explore", AppRoute::Explore)], CRUMB),
        None,
        PAGE_TITLE,
        lead,
        html! { <ShareButton class={classes!("run-action")} labeled={true} /> },
    )
}

fn compare_view(
    view: &CompareView,
    reports: &Loadable<Vec<BenchmarkReportLight>>,
    reload: &Callback<()>,
    scroll_ref: &NodeRef,
) -> Html {
    let lead = match view.runs.first() {
        Some(baseline) if view.runs.len() > 1 => format!(
            "{} runs, each against the baseline {}.",
            view.runs.len(),
            run_name(baseline)
        ),
        _ => "Two or three runs side by side, each against the first run, the baseline.".to_owned(),
    };
    let notes = html! {
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
    };
    if view.runs.len() < 2 {
        return html! {
            <>
                { head(&lead) }
                <section class="page-container compare-head">
                    { notes }
                    { run_cards(view) }
                    { ask_view(view) }
                </section>
            </>
        };
    }
    let ready = match reports {
        Loadable::Ready(reports) => Some(reports.as_slice()),
        Loadable::Loading | Loadable::Failed(_) => None,
    };
    let status = loadable(reports, REPORTS, reload, |_| Html::default());
    let chart_runs = chart_runs(view);
    let latency_runs: Vec<ChartRun> = chart_runs
        .iter()
        .filter(|(_, rate_limited)| *rate_limited)
        .map(|(line, _)| line.clone())
        .collect();
    let throughput_runs: Vec<ChartRun> = chart_runs.into_iter().map(|(line, _)| line).collect();
    let sections: &'static [NavSection] = if latency_runs.is_empty() {
        &THROUGHPUT_SECTIONS
    } else {
        &SECTIONS
    };
    html! {
        <>
            { head(&lead) }
            <section class="page-container compare-head">
                { notes }
                { summary_card(view) }
                { run_cards(view) }
            </section>
            <SectionNav {sections} scroll_ref={scroll_ref.clone()} />

            if !latency_runs.is_empty() {
                <section id={LATENCY.id} class="page-section page-container">
                    { section_head(
                        LATENCY.label,
                        "Latency against the baseline",
                        "One line per run with a rate limit, for its main group. In the table, a \
                         change follows the delta rule from the Methodology page, with one run on \
                         each side. The rows below the line come from the reports and only show \
                         values.",
                    ) }
                    <article class="card run-chart-card">
                        <h3>{"Average latency"}</h3>
                        <OverlayChart runs={latency_runs} metric={SeriesMetric::Latency} />
                        <p class="card-note">{ LATENCY_CHART_NOTE }</p>
                    </article>
                    { latency_table(view, ready) }
                    { status.clone() }
                </section>
            }

            <section id={THROUGHPUT.id} class="page-section page-container">
                { section_head(
                    THROUGHPUT.label,
                    "Throughput against the baseline",
                    "One line per run for its main group, plotted against the time since the run \
                     started. The card next to the chart gives each run's rates and their change.",
                ) }
                <div class="compare-throughput">
                    <article class="card run-chart-card">
                        <h3>{"Throughput"}</h3>
                        <OverlayChart runs={throughput_runs} metric={SeriesMetric::Throughput} />
                    </article>
                    { throughput_card(view) }
                </div>
            </section>

            <section id={CONFIGURATION.id} class="page-section page-container">
                { section_head(
                    CONFIGURATION.label,
                    "What the runs share",
                    "Each run's workload, environment and bench host. The rows that differ from \
                     the baseline come first.",
                ) }
                { settings_view(view, ready) }
                { status }
            </section>
        </>
    }
}

/// What changed against the baseline, under the runs' furthest level, which limits what the page
/// can say. Each run keeps the judgment of its own level, as in the matrix.
fn summary_card(view: &CompareView) -> Html {
    let levels = view.levels();
    let Some(level) = levels.iter().copied().max() else {
        return Html::default();
    };
    let lines = summary_changes(&view.change_rows);
    let others = view.runs.iter().zip(RUN_COLORS).skip(1).zip(levels);
    html! {
        <div class={classes!("card", "compare-summary", level_look(level))} role="note">
            <span class="eyebrow">{ SUMMARY_TITLE }</span>
            <strong class="compare-summary-level">{ level.to_string() }</strong>
            <p>{ level.explanation() }</p>
            if !lines.is_empty() {
                <ul class="compare-summary-runs">
                    { for others.zip(&lines).map(|(((run, color), run_level), changes)| {
                        summary_line(run, color, run_level, changes)
                    }) }
                </ul>
            }
        </div>
    }
}

/// A run of a different workload from the baseline has no change, so its line names its level. Any
/// other run whose cells have no change, such as one without the main group, shows a dash, like the
/// matrix.
fn summary_line(
    run: &BenchmarkSummary,
    color: &str,
    level: Comparability,
    changes: &[(&'static str, &CellChange)],
) -> Html {
    let no_change = match level {
        Comparability::DifferentWorkloads => level.to_string(),
        _ => NO_VALUE.to_owned(),
    };
    html! {
        <li class="compare-summary-run" style={run_color(color)}>
            <span class="compare-summary-name">
                <span class="compare-swatch" aria-hidden="true"></span>
                { run_name(run) }
            </span>
            { for changes.iter().map(|(label, change)| html! {
                <span class="compare-summary-change">
                    { *label }
                    { judged_chip(change.text.clone(), change.values.clone(), change.judgment) }
                </span>
            }) }
            if changes.is_empty() {
                <span class="compare-summary-change">{ no_change }</span>
            }
        </li>
    }
}

const fn level_look(level: Comparability) -> &'static str {
    match level {
        Comparability::LikeForLike => "compare-level-like",
        Comparability::EnvironmentDiffers => "compare-level-environment",
        Comparability::ParametersDiffer => "compare-level-parameters",
        Comparability::DifferentWorkloads => "compare-level-different",
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
            let level = position
                .checked_sub(1)
                .and_then(|other| levels.get(other))
                .copied();
            run_card(run, color, level, &ids)
        });
    html! {
        <div class="compare-runs">
            { for cards }
            if (2..MAX_COMPARED_RUNS).contains(&view.runs.len()) {
                <div class="card compare-run compare-run-add">
                    <p>{"Compare one more run against the baseline."}</p>
                    <AddRunLink runs={ids.clone()}>{"Add a run"}</AddRunLink>
                </div>
            }
        </div>
    }
}

/// `level` is `None` for the baseline.
fn run_card(
    run: &BenchmarkSummary,
    color: &'static str,
    level: Option<Comparability>,
    ids: &[Uuid],
) -> Html {
    let start = run
        .version_order
        .timestamp_us
        .and_then(DayLabel::from_unix_micros);
    html! {
        <article class="card compare-run" style={run_color(color)}>
            <div class="compare-run-top">
                <span class="compare-swatch" aria-hidden="true"></span>
                { version_tag_view(&version_tag(run), run.release) }
                if let Some(level) = level {
                    <span class={classes!("compare-level", level_look(level))}>
                        { level.to_string() }
                    </span>
                } else {
                    <span class="compare-level compare-baseline">{"Baseline"}</span>
                }
            </div>
            <h2 class="compare-run-title">{ run.title.clone() }</h2>
            <p class="run-meta compare-run-meta">
                <span>{ short_id(&run.uuid) }</span>
                if let Some(start) = start {
                    <span>{ start.view() }</span>
                }
            </p>
            <p class="compare-run-conditions">{ conditions(run) }</p>
            <div class="compare-run-actions">
                <Link<AppRoute>
                    to={AppRoute::Benchmark { uuid: run.uuid.to_string() }}
                    classes={classes!("run-action")}
                >
                    {"Open run"}
                </Link<AppRoute>>
                if level.is_some() {
                    <CompareLink runs={with_baseline(ids, run.uuid)} classes={classes!("run-action")}>
                        {"Use as baseline"}
                    </CompareLink>
                }
                if ids.len() > 1 {
                    <CompareLink runs={without(ids, run.uuid)} classes={classes!("run-action")}>
                        {"Remove"}
                    </CompareLink>
                }
            </div>
        </article>
    }
}

fn run_color(color: &str) -> String {
    format!("--run-color: {color}")
}

fn ask_view(view: &CompareView) -> Html {
    let ask = if view.runs.is_empty() {
        "Pick two or three runs in Explore to compare them."
    } else {
        "Add a run from Explore to compare it with this one."
    };
    html! {
        <p class="compare-ask">
            <span>{ ask }</span>
            <AddRunLink runs={view.ids()}>{"Open Explore"}</AddRunLink>
        </p>
    }
}

#[derive(Properties, PartialEq)]
struct AddRunLinkProps {
    /// The page's runs, which Explore's tray takes over.
    runs: Vec<Uuid>,
    children: Html,
}

/// Opens Explore with the page's runs in the tray, so one more pick completes the comparison. A
/// page without runs leaves the tray as it is.
#[function_component(AddRunLink)]
fn add_run_link(props: &AddRunLinkProps) -> Html {
    let navigator = use_navigator();
    let selection = use_selection();
    let runs = props.runs.clone();
    let onclick = Callback::from(move |event: MouseEvent| {
        let Some(navigator) = navigator.as_ref().filter(|_| !opens_elsewhere(&event)) else {
            return;
        };
        event.prevent_default();
        if let Some(selection) = selection.as_ref().filter(|_| !runs.is_empty()) {
            selection.dispatch(SelectionAction::Replace(runs.clone()));
        }
        navigator.push(&AppRoute::Explore);
    });
    html! {
        <a href={AppRoute::Explore.to_path()} class="run-action" {onclick}>
            { props.children.clone() }
        </a>
    }
}

fn run_heads(view: &CompareView, first: &'static str) -> Html {
    html! {
        <tr>
            <th scope="col">{ first }</th>
            { for view.runs.iter().zip(RUN_COLORS).enumerate().map(|(position, (run, color))| html! {
                <th scope="col" class="run-table-number compare-run-head" style={run_color(color)}>
                    <span class="compare-swatch" aria-hidden="true"></span>
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
                style: run_color(color).into(),
            }
        })
        .collect()
}

fn latency_table(view: &CompareView, reports: Option<&[BenchmarkReportLight]>) -> Html {
    let report_rows = reports.map(|reports| report_rows(&view.refs(), reports));
    let columns = columns(view);
    let change_rows = view
        .change_rows
        .iter()
        .filter(|row| row.kind == RowKind::Latency);
    html! {
        <div class="card run-table-card">
            <table class="run-table compare-table compare-latency-table">
                <thead>{ run_heads(view, "Metric") }</thead>
                <tbody>{ for change_rows.map(|row| matrix_row(row, &columns)) }</tbody>
                if let Some(rows) = report_rows.filter(|rows| !rows.is_empty()) {
                    <tbody class="compare-report-rows">
                        { for rows.iter().map(|row| matrix_row(row, &columns)) }
                    </tbody>
                }
            </table>
            <p class="card-note compare-table-note">{ CHANGE_NOTE }</p>
        </div>
    }
}

fn matrix_row(row: &MatrixRow, columns: &[Column]) -> Html {
    html! {
        <tr>
            <th scope="row" title={row.hint}>{ row.label }</th>
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
            title={cell.note}
            data-label={column.run.clone()}
            style={column.style.clone()}
        >
            <span class="compare-cell">
                <span class="compare-value">{ cell_text(cell) }</span>
                <span class="compare-cell-change">
                    if let Some(change) = &cell.change {
                        { compact_judged_chip(change.text.clone(), change.values.clone(), change.judgment) }
                    }
                </span>
            </span>
        </td>
    }
}

/// Each run's line, and whether the run has a rate limit. A run without one has no latency to draw,
/// so the latency chart leaves it out.
fn chart_runs(view: &CompareView) -> Vec<(ChartRun, bool)> {
    view.runs
        .iter()
        .zip(RUN_COLORS)
        .map(|(run, color)| {
            let line = ChartRun {
                uuid: run.uuid,
                group: run.kind_family.primary_group(),
                name: run_name(run),
                color,
            };
            (line, run.workload_key.rate_limit.is_some())
        })
        .collect()
}

/// Each run's throughput with its change, its message rate, and a bar against the fastest run, so
/// the runs read at a glance. The numbers come from the matrix rows that the summary at the top also
/// uses, so the two never disagree. The message rate moves with the throughput, so its change would
/// only repeat the throughput's.
fn throughput_card(view: &CompareView) -> Html {
    let row_of = |kind: RowKind| view.change_rows.iter().find(|row| row.kind == kind);
    let (Some(throughput), Some(messages)) =
        (row_of(RowKind::Throughput), row_of(RowKind::MessageRate))
    else {
        return Html::default();
    };
    let rates: Vec<Option<f64>> = view
        .runs
        .iter()
        .map(|run| {
            run.group(run.kind_family.primary_group())
                .map(|group| group.value(Metric::Throughput))
        })
        .collect();
    let fastest = rates.iter().flatten().copied().fold(0.0, f64::max);
    let runs = view
        .runs
        .iter()
        .zip(RUN_COLORS)
        .zip(rates)
        .enumerate()
        .map(|(position, ((run, color), rate))| {
            let (Some(rate_cell), Some(messages_cell)) =
                (throughput.cells.get(position), messages.cells.get(position))
            else {
                return Html::default();
            };
            let share = rate
                .filter(|_| fastest > 0.0)
                .map_or(0.0, |rate| rate / fastest * 100.0);
            html! {
                <li class="compare-throughput-run" style={run_color(color)}>
                    <span class="compare-throughput-name">
                        <span class="compare-swatch" aria-hidden="true"></span>
                        <span class="compare-throughput-run-name">{ run_name(run) }</span>
                    </span>
                    <span class="compare-throughput-change">
                        if position == 0 {
                            <span class="compare-throughput-baseline">{ BASELINE }</span>
                        } else if let Some(change) = &rate_cell.change {
                            { compact_judged_chip(change.text.clone(), change.values.clone(), change.judgment) }
                        }
                    </span>
                    <strong class="compare-throughput-rate" title={throughput.hint}>
                        { cell_text(rate_cell) }
                    </strong>
                    <span class="compare-throughput-messages" title={messages.hint}>
                        { cell_text(messages_cell) }
                    </span>
                    <span class="compare-throughput-bar" aria-hidden="true">
                        <span style={format!("width: {share:.1}%")}></span>
                    </span>
                </li>
            }
        });
    html! {
        <article class="card compare-throughput-card">
            <h3>{"Against the baseline"}</h3>
            <ul class="compare-throughput-runs">{ for runs }</ul>
        </article>
    }
}

fn cell_text(cell: &Cell) -> String {
    cell.value.clone().unwrap_or_else(|| NO_VALUE.to_owned())
}

fn settings_view(view: &CompareView, reports: Option<&[BenchmarkReportLight]>) -> Html {
    let rows = setting_rows(&view.refs(), reports);
    let (differing, equal): (Vec<&SettingRow>, Vec<&SettingRow>) =
        rows.iter().partition(|row| row.differs());
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
        <tr class={classes!(row.differs().then_some("compare-differs"))}>
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
