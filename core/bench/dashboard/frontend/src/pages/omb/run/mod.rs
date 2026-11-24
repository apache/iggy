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

//! One OMB run's page at `/omb/runs/{id}`. It shows exactly the run in the URL: the summary comes
//! from the loaded OMB list by id and the detail from the server by id, so no other run can take
//! its place. An iggy-bench run id isn't an OMB id, so it gives "Run not found".

mod advanced;
mod change;
mod chart;
mod facts;
mod history;
mod related;

use std::rc::Rc;

use bench_dashboard_shared::embed::OmbChartType;
use bench_dashboard_shared::labels::{messages_text, milliseconds, version_detail};
use bench_dashboard_shared::omb::chart::WindowMetric;
use bench_dashboard_shared::omb::compare::OmbComparability;
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::labels::{
    environment_text, mebibytes_text, run_name, short_id, size_and_length, workload_heading,
};
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::setup::{OmbSetup, omb_setup_download_name};
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary};
use yew::prelude::*;
use yew_router::prelude::Link;

use self::advanced::Advanced;
use self::change::{Change, KPIS, Kpi};
use self::chart::{DetailRef, WindowChart};
use self::facts::{
    END_TO_END_TITLE, Fact, FactGroup, NO_SIDECAR_NOTE, UNRECORDED, end_to_end_side,
    environment_groups, estimated_start, ladder, property_groups, publish_side, workload_facts,
};
use self::history::{Baseline, History, Machines, history_view};
use self::related::{related_runs, related_view};
use crate::api::{fetch_omb_run, omb_result_url, omb_setup_url};
use crate::components::chart::series_color;
use crate::components::embed::{EmbedButton, EmbedCard, EmbedChart, EmbedProvider, EmbedRun};
use crate::components::headline::kpi_card;
use crate::components::ladder::{Rung, percentile_ladder};
use crate::components::page::{breadcrumbs, loadable, page_shell, section_head};
use crate::components::section_nav::{NavSection, SectionNav};
use crate::components::share_button::ShareButton;
use crate::pages::omb::common::{
    CONSUME_HINT, EndToEnd, LineOrder, MESSAGES_HINT, NOT_MEASURED, PUBLISH_THROUGHPUT_HINT,
    driver_tag, omb_crumb, omb_page_head, runs_status, time_view, version_view,
};
use crate::pages::run::chart::SERIES_COLOR_VARS;
use crate::router::AppRoute;
use crate::state::data::{Loadable, fetch_into};
use crate::state::omb::{OmbData, use_omb_data};

const RUN_CRUMB: &str = "Run";
const DETAIL_WHAT: &str = "the OMB result of the run";
const ESTIMATE_HINT: &str = "The end of the run from the file name, minus the measured length. The \
                             warm-up ran before it.";
const LATENCY_NOTE: &str = "One point at the end of each 10 second window of the measured phase. \
                            OMB leaves the warm-up out of its results.";
const THROUGHPUT_SECTION_NOTE: &str = "One point at the end of each 10 second window of the measured \
                                       phase. The card next to the chart gives the mean rates over \
                                       the windows.";
const RATES_TITLE: &str = "Over the whole run";
const PUBLISH_LABEL: &str = "Publish";
const CONSUME_LABEL: &str = "Consume";
const THROUGHPUT_NOTE: &str = "The publish and consume rates of each window, times the message \
                               size. The consume rate counts every subscription.";
/// Repeats the clock caveat of the End-to-end latency card on the Methodology page.
const CLOCK_NOTE: &str = "End-to-end latency compares the send time on one worker host with the \
                          receive time on another, so it depends on how well their clocks agree. \
                          Read a small change as possible clock drift.";
const PUBLISH_LADDER_NOTE: &str = "From the histogram that OMB merges over every send of every \
                                   producer in the measured phase.";
const VERSION_ORDER_NOTE: &str = "Every run of this workload and driver on these machines, in Iggy \
                                  version order.";
const TIME_ORDER_NOTE: &str = "Every run of this workload and driver on these machines, in time \
                               order, because a run on the line has no Iggy version.";
const VERSION_ORDER_UNKNOWN_MACHINES_NOTE: &str = "Every run of this workload and driver whose \
                                                   machines are unknown, in Iggy version order.";
const TIME_ORDER_UNKNOWN_MACHINES_NOTE: &str = "Every run of this workload and driver whose \
                                                machines are unknown, in time order, because a \
                                                run on the line has no Iggy version.";
const RELATED_NOTE: &str = "Runs of this workload on these machines with another driver, such as a \
                            persisted one, with the newest run of each driver.";
const RELATED_UNKNOWN_MACHINES_NOTE: &str = "Runs of this workload with another driver, such as a \
                                             persisted one, with the newest run of each driver. \
                                             Their machines are unknown, as they are for this \
                                             run.";
const NO_NOISE_RULE: &str = "OMB runs have no noise rule yet, so a change has no color.";
const UNRECORDED_LEAD: &str = "OMB doesn't record these, so no OMB page can show them:";
const LATENCY: NavSection = NavSection {
    id: "latency",
    label: "Latency",
};
const PERCENTILES: NavSection = NavSection {
    id: "percentiles",
    label: "Percentiles",
};
const THROUGHPUT: NavSection = NavSection {
    id: "throughput",
    label: "Throughput",
};
const HISTORY: NavSection = NavSection {
    id: "history",
    label: "History",
};
const RELATED: NavSection = NavSection {
    id: "related",
    label: "Related runs",
};
const CONFIGURATION: NavSection = NavSection {
    id: "configuration",
    label: "Configuration",
};
const ENVIRONMENT: NavSection = NavSection {
    id: "environment",
    label: "Environment",
};
const ARTIFACTS: NavSection = NavSection {
    id: "artifacts",
    label: "Artifacts",
};
const ADVANCED: NavSection = NavSection {
    id: "advanced",
    label: "Advanced",
};
static SIDECAR_SECTIONS: [NavSection; 9] = [
    LATENCY,
    PERCENTILES,
    THROUGHPUT,
    HISTORY,
    RELATED,
    CONFIGURATION,
    ENVIRONMENT,
    ARTIFACTS,
    ADVANCED,
];
/// Only a setup sidecar names the machines and builds, so a run without one has no Environment
/// section.
static PLAIN_SECTIONS: [NavSection; 8] = [
    LATENCY,
    PERCENTILES,
    THROUGHPUT,
    HISTORY,
    RELATED,
    CONFIGURATION,
    ARTIFACTS,
    ADVANCED,
];

#[derive(Properties, PartialEq)]
pub struct OmbRunPageProps {
    /// As written in the URL.
    pub id: AttrValue,
}

#[function_component(OmbRunPage)]
pub fn omb_run_page(props: &OmbRunPageProps) -> Html {
    let data = use_omb_data();
    let scroll_ref = use_node_ref();
    let id = props.id.parse::<OmbRunId>().ok();
    let detail = use_state_eq(|| Loadable::Loading);
    let load_detail = {
        let detail = detail.clone();
        Callback::from(move |()| {
            if let Some(id) = id {
                fetch_into(detail.clone(), fetch_omb_run(id));
            }
        })
    };
    {
        let load_detail = load_detail.clone();
        use_effect_with(id, move |_| load_detail.emit(()));
    }
    let lookup = use_memo((data.runs.clone(), id), |(runs, id)| Lookup::new(runs, *id));

    let content = match &*lookup {
        Lookup::Pending => pending_view(&data),
        Lookup::Missing => missing_view(&props.id),
        Lookup::Found(view) => found_view(view, &detail, &load_detail, &scroll_ref),
    };
    page_shell(&scroll_ref, content)
}

/// The URL's run in the loaded list.
enum Lookup {
    /// The list is loading or failed.
    Pending,
    Missing,
    Found(Box<RunView>),
}

/// What the page shows about the run before its detail arrives. Built once per load.
struct RunView {
    run: OmbRunSummary,
    history: History,
    /// The newest run of each other driver configuration.
    related: Vec<OmbRunSummary>,
}

/// One line of the throughput chart, with its rates over the whole run.
#[derive(Debug, PartialEq)]
struct RateRow {
    label: &'static str,
    rate_hint: &'static str,
    messages_hint: &'static str,
    /// In MiB/s and messages per second. `None` for a consume rate without consumers.
    rates: Option<(f64, f64)>,
    /// The headline's change, on the publish line only.
    change: Option<Change>,
    /// The rate against the faster line, for the length of its bar.
    share_pct: f64,
}

impl Lookup {
    fn new(runs: &Loadable<OmbRunList>, id: Option<OmbRunId>) -> Self {
        let Loadable::Ready(list) = runs else {
            return Self::Pending;
        };
        match id.and_then(|id| list.runs.iter().find(|run| run.id == id)) {
            Some(run) => Self::Found(Box::new(RunView::new(run, &list.runs))),
            None => Self::Missing,
        }
    }
}

impl RunView {
    fn new(run: &OmbRunSummary, runs: &[OmbRunSummary]) -> Self {
        Self {
            history: History::new(run, runs),
            related: related_runs(run, runs),
            run: run.clone(),
        }
    }

    /// What the changes on the page compare with.
    fn baseline(&self) -> Option<Baseline<'_>> {
        self.history.baseline()
    }

    /// The lead of the History section.
    const fn history_note(&self) -> &'static str {
        match (self.history.order, self.history.machines) {
            (LineOrder::Version, Machines::Known) => VERSION_ORDER_NOTE,
            (LineOrder::Time, Machines::Known) => TIME_ORDER_NOTE,
            (LineOrder::Version, Machines::Unknown) => VERSION_ORDER_UNKNOWN_MACHINES_NOTE,
            (LineOrder::Time, Machines::Unknown) => TIME_ORDER_UNKNOWN_MACHINES_NOTE,
        }
    }

    /// The lead of the Related runs section.
    const fn related_note(&self) -> &'static str {
        match self.history.machines {
            Machines::Known => RELATED_NOTE,
            Machines::Unknown => RELATED_UNKNOWN_MACHINES_NOTE,
        }
    }

    /// The notes under the headline numbers about what their changes compare. A run whose machines
    /// are unknown may have run on other machines than the runs it compares with, so the page says
    /// so, as the compare page does for such runs. Empty without a baseline.
    fn comparison_notes(&self) -> Vec<String> {
        let Some(baseline) = self.baseline() else {
            return Vec::new();
        };
        let line = match self.history.machines {
            Machines::Known => "this workload and driver on these machines",
            Machines::Unknown => "this workload and driver",
        };
        let compared = match baseline {
            Baseline::Version(step) => format!(
                "Each change compares the median of this version, {}, with the median of the \
                 previous version, {}, over the runs of {line}.",
                step.version, step.previous
            ),
            Baseline::Run(previous) => format!(
                "Each change compares with the previous run of {line}, {}.",
                run_name(previous)
            ),
        };
        let mut notes = vec![format!("{compared} {NO_NOISE_RULE}")];
        if self.history.machines == Machines::Unknown {
            notes.push(OmbComparability::SetupUnknown.explanation().to_owned());
        }
        notes
    }
}

/// The loading or failed state of the detail, else `ready` with it.
fn with_detail(
    detail: &Loadable<OmbRunDetail>,
    reload: &Callback<()>,
    ready: impl FnOnce(&DetailRef) -> Html,
) -> Html {
    match detail {
        Loadable::Ready(detail) => ready(&DetailRef(Rc::clone(detail))),
        loading_or_failed => loadable(loading_or_failed, DETAIL_WHAT, reload, |_| Html::default()),
    }
}

fn pending_view(data: &OmbData) -> Html {
    html! {
        <>
            { plain_head("OMB run", "") }
            <section class="page-container">{ runs_status(data) }</section>
        </>
    }
}

fn missing_view(id: &str) -> Html {
    html! {
        <>
            { plain_head("Run not found", &format!("No loaded OMB run has the id {id}.")) }
            <section class="page-container">
                <Link<AppRoute> to={AppRoute::OmbRuns} classes={classes!("run-action")}>
                    {"All OMB runs"}
                </Link<AppRoute>>
            </section>
        </>
    }
}

fn plain_head(title: &str, lead: &str) -> Html {
    omb_page_head(
        breadcrumbs(&[omb_crumb()], RUN_CRUMB),
        None,
        title,
        lead,
        Html::default(),
    )
}

fn found_view(
    view: &RunView,
    detail: &Loadable<OmbRunDetail>,
    reload: &Callback<()>,
    scroll_ref: &NodeRef,
) -> Html {
    let run = &view.run;
    let sections: &'static [NavSection] = if run.setup.is_some() {
        &SIDECAR_SECTIONS
    } else {
        &PLAIN_SECTIONS
    };
    html! {
        <EmbedProvider run={EmbedRun::Omb(run.id)} charts={embed_charts(run)}>
            { head(run) }
            { headline_strip(view) }
            <SectionNav {sections} scroll_ref={scroll_ref.clone()} />

            <section id={LATENCY.id} class="page-section page-container">
                { section_head(LATENCY.label, "Every window", LATENCY_NOTE) }
                { with_detail(detail, reload, |detail| latency_charts(detail, run)) }
            </section>

            <section id={PERCENTILES.id} class="page-section page-container">
                { section_head(
                    PERCENTILES.label,
                    "Every percentile",
                    "On a log scale, so the median and the tail fit in one view. Each ladder comes \
                     from one histogram that OMB merges over the measured phase.",
                ) }
                { latency_cards(view) }
            </section>

            <section id={THROUGHPUT.id} class="page-section page-container">
                { section_head(THROUGHPUT.label, "Start to finish", THROUGHPUT_SECTION_NOTE) }
                <div class="chart-with-card">
                    { with_detail(detail, reload, |detail| {
                        window_card(detail, WindowMetric::Throughput, Some(THROUGHPUT_NOTE))
                    }) }
                    { rates_card(view) }
                </div>
            </section>

            <section id={HISTORY.id} class="page-section page-container">
                { section_head(HISTORY.label, "Same workload and driver", view.history_note()) }
                { history_view(&view.history, run) }
            </section>

            <section id={RELATED.id} class="page-section page-container">
                { section_head(
                    RELATED.label,
                    "Other driver configurations",
                    view.related_note(),
                ) }
                { related_view(&view.related, run, view.history.machines) }
            </section>

            <section id={CONFIGURATION.id} class="page-section page-container">
                { section_head(
                    CONFIGURATION.label,
                    "What the result records",
                    "The workload as the result file records it, the properties from the setup \
                     sidecar, and what OMB leaves out.",
                ) }
                { configuration(run) }
            </section>

            if let Some(setup) = &run.setup {
                <section id={ENVIRONMENT.id} class="page-section page-container">
                    { section_head(
                        ENVIRONMENT.label,
                        "Machines and builds",
                        "As the setup.toml next to the result names them.",
                    ) }
                    { environment(run, setup) }
                </section>
            }

            <section id={ARTIFACTS.id} class="page-section page-container">
                { section_head(
                    ARTIFACTS.label,
                    "Files of this run",
                    "The result file as OMB wrote it, the setup sidecar if the run has one, and \
                     the code to show this run's charts on another page.",
                ) }
                { artifacts(run) }
            </section>

            <section id={ADVANCED.id} class="page-section page-container">
                { section_head(
                    ADVANCED.label,
                    "Additional insights",
                    "The latency at every percentile, the publish delay, the backlog and the \
                     publish errors over the run. The charts load when you open them.",
                ) }
                <Advanced detail={detail.clone()} reload={reload.clone()} />
            </section>
        </EmbedProvider>
    }
}

/// The charts of the run that an embed can draw, in the order of the page. A run without
/// end-to-end numbers has no end-to-end chart, because the server answers 404 for it.
fn embed_charts(run: &OmbRunSummary) -> Vec<EmbedChart> {
    let end_to_end = matches!(EndToEnd::of(run), EndToEnd::Measured(_));
    OmbChartType::ALL
        .into_iter()
        .filter(|chart| end_to_end || *chart != OmbChartType::EndToEndLatency)
        .map(EmbedChart::Omb)
        .collect()
}

/// The heading names the run by its workload values, not by the workload `name`. A crawler indexes
/// the heading, and the name is free text from the workload file.
fn head(run: &OmbRunSummary) -> Html {
    let environment = run.environment().and_then(environment_text);
    let detail = run
        .version
        .as_ref()
        .and_then(|version| version_detail(version.commit.as_deref(), version.release));
    let above = html! {
        <>
            { breadcrumbs(&[omb_crumb()], &short_id(run.id)) }
            <p class="run-meta">
                { driver_tag(&run.driver) }
                { version_view(run) }
                if let Some(detail) = detail {
                    <span>{ detail }</span>
                }
                <span>
                    { time_view(&run.time) }
                    <span class="omb-run-time-source">{ format!(" ({})", run.time.source) }</span>
                </span>
                if let Some(start) = estimated_start(run) {
                    <span class="omb-run-estimate" title={ESTIMATE_HINT}>
                        { format!("start about {start}, estimated") }
                    </span>
                }
                if let Some(environment) = environment {
                    <span>{ environment }</span>
                }
            </p>
        </>
    };
    omb_page_head(
        above,
        None,
        &workload_heading(&run.workload),
        &size_and_length(run),
        html! { <ShareButton class={classes!("run-action")} labeled={true} /> },
    )
}

fn headline_strip(view: &RunView) -> Html {
    let run = &view.run;
    let baseline = view.baseline();
    html! {
        <section class="page-container run-headlines" aria-label="Headline numbers">
            <div class="run-kpis">
                { for KPIS.iter().map(|&kpi| {
                    let change = baseline
                        .as_ref()
                        .and_then(|baseline| kpi.change(run, baseline))
                        .map(|change| change.chip())
                        .unwrap_or_default();
                    kpi_card(
                        kpi.label(),
                        Some(kpi.hint()),
                        kpi.text(kpi.value(run)),
                        kpi.detail(run),
                        change,
                    )
                }) }
            </div>
            { for view.comparison_notes().into_iter().map(|note| html! {
                <p class="run-note">{ note }</p>
            }) }
            if let Some(note) = EndToEnd::of(run).note() {
                <p class="run-note">{ note }</p>
            }
            if !run.badges.is_empty() {
                <ul class="run-badges" aria-label="Badges">
                    { for run.badges.iter().map(|badge| html! {
                        <li>
                            <span class="badge">{ badge.to_string() }</span>
                            <span>{ badge.explanation() }</span>
                        </li>
                    }) }
                </ul>
            }
        </section>
    }
}

fn latency_charts(detail: &DetailRef, run: &OmbRunSummary) -> Html {
    let end_to_end = EndToEnd::of(run);
    html! {
        <>
            <div class="omb-run-charts">
                { window_card(detail, WindowMetric::PublishLatency, None) }
                if matches!(end_to_end, EndToEnd::Measured(_)) {
                    { window_card(detail, WindowMetric::EndToEndLatency, Some(CLOCK_NOTE)) }
                }
            </div>
            if let Some(note) = end_to_end.note() {
                <p class="section-text">{ note }</p>
            }
        </>
    }
}

fn window_card(detail: &DetailRef, metric: WindowMetric, note: Option<&'static str>) -> Html {
    html! {
        <article class="card run-chart-card">
            <h3>{ metric.title() }</h3>
            if let Some(chart) = OmbChartType::of_window(metric) {
                <EmbedButton chart={EmbedChart::Omb(chart)} />
            }
            <WindowChart detail={detail.clone()} {metric} />
            if let Some(note) = note {
                <p class="card-note">{ note }</p>
            }
        </article>
    }
}

fn latency_cards(view: &RunView) -> Html {
    let run = &view.run;
    let publish = ladder_card(
        "Publish latency",
        &run.publish_latency_ms,
        &publish_side(run),
        PUBLISH_LADDER_NOTE,
    );
    let end_to_end = match EndToEnd::of(run) {
        EndToEnd::Measured(latency) => ladder_card(
            END_TO_END_TITLE,
            &latency,
            &end_to_end_side(run, &latency),
            CLOCK_NOTE,
        ),
        missing => html! {
            <article class="card">
                <h3>{ END_TO_END_TITLE }</h3>
                <p>
                    <strong>{ NOT_MEASURED }</strong>
                    {". "}
                    { missing.note().unwrap_or_default() }
                </p>
            </article>
        },
    };
    html! {
        <div class="omb-run-ladders">
            { publish }
            { end_to_end }
        </div>
    }
}

/// The headline numbers above show the changes, so the rungs have none.
fn ladder_card(title: &str, latency: &OmbPercentiles, side: &[Fact], note: &'static str) -> Html {
    let rungs: Vec<Rung> = ladder(latency)
        .into_iter()
        .map(|row| Rung {
            label: row.label,
            hint: Some(row.hint),
            value: milliseconds(row.value_ms),
            bar_pct: row.bar_pct,
            key: row.key,
            change: Html::default(),
        })
        .collect();
    html! {
        <article class="card run-ladder-card">
            <h3>{ title.to_owned() }</h3>
            <EmbedButton chart={EmbedChart::Omb(OmbChartType::Ladder)} />
            { percentile_ladder(&rungs, false) }
            <div>
                { fact_list(side) }
                <p class="card-note">{ note }</p>
            </div>
        </article>
    }
}

/// The publish and consume rates in the line colors of the throughput chart, with the change of
/// the publish rate from the headline.
fn rates_card(view: &RunView) -> Html {
    let items = rate_rows(view)
        .into_iter()
        .zip(SERIES_COLOR_VARS)
        .map(|(row, color)| {
            html! {
                <li class="rates-row" style={series_color(color)}>
                    <span class="rates-name">
                        <span class="series-swatch" aria-hidden="true"></span>
                        <span class="rates-label">{ row.label }</span>
                    </span>
                    <span class="rates-change">
                        if let Some(change) = &row.change {
                            { change.chip() }
                        }
                    </span>
                    if let Some((mebibytes, messages)) = row.rates {
                        <strong class="rates-value" title={row.rate_hint}>
                            { mebibytes_text(mebibytes) }
                        </strong>
                        <span class="rates-messages" title={row.messages_hint}>
                            { messages_text(messages) }
                        </span>
                    } else {
                        <span class="omb-missing" title={row.rate_hint}>{ NOT_MEASURED }</span>
                    }
                    <span class="rates-bar" aria-hidden="true">
                        <span style={format!("width: {:.1}%", row.share_pct)}></span>
                    </span>
                </li>
            }
        });
    html! {
        <article class="card rates-card">
            <h3>{ RATES_TITLE }</h3>
            <ul class="rates-list">{ for items }</ul>
        </article>
    }
}

/// The publish line, then the consume line, in the order that the throughput chart draws them.
fn rate_rows(view: &RunView) -> [RateRow; 2] {
    let run = &view.run;
    let publish = (
        run.publish_mebibytes_per_second,
        run.publish_messages_per_second,
    );
    let consume = (run.workload.consumers_per_topic > 0).then_some((
        run.consume_mebibytes_per_second,
        run.consume_messages_per_second,
    ));
    let fastest = consume.map_or(publish.0, |(mebibytes, _)| publish.0.max(mebibytes));
    let share_pct = |rates: Option<(f64, f64)>| match rates {
        Some((mebibytes, _)) if fastest > 0.0 => mebibytes / fastest * 100.0,
        _ => 0.0,
    };
    [
        RateRow {
            label: PUBLISH_LABEL,
            rate_hint: PUBLISH_THROUGHPUT_HINT,
            messages_hint: MESSAGES_HINT,
            rates: Some(publish),
            change: view
                .baseline()
                .and_then(|baseline| Kpi::PublishThroughput.change(run, &baseline)),
            share_pct: share_pct(Some(publish)),
        },
        RateRow {
            label: CONSUME_LABEL,
            rate_hint: CONSUME_HINT,
            messages_hint: CONSUME_HINT,
            rates: consume,
            change: None,
            share_pct: share_pct(consume),
        },
    ]
}

fn configuration(run: &OmbRunSummary) -> Html {
    let groups = run.setup.as_ref().map(property_groups).unwrap_or_default();
    html! {
        <>
            if run.setup.is_none() {
                <p class="section-text">{ NO_SIDECAR_NOTE }</p>
            }
            <div class="card-columns">
                <article class="card">
                    <h3>{"Workload"}</h3>
                    { fact_list(&workload_facts(run)) }
                </article>
                { for groups.iter().map(fact_group_card) }
                <article class="card">
                    <h3>{"Not in the result"}</h3>
                    <p>{ UNRECORDED_LEAD }</p>
                    <ul class="omb-run-unrecorded">
                        { for UNRECORDED.iter().map(|item| html! { <li>{ *item }</li> }) }
                    </ul>
                </article>
            </div>
        </>
    }
}

fn environment(run: &OmbRunSummary, setup: &OmbSetup) -> Html {
    html! {
        <div class="card-grid">
            { for environment_groups(run, setup).iter().map(fact_group_card) }
        </div>
    }
}

fn artifacts(run: &OmbRunSummary) -> Html {
    html! {
        <div class="card-grid">
            <article class="card">
                <h3>{"Result"}</h3>
                <p>{ format!("{}, as OMB wrote it.", run.file_name) }</p>
                <a class="run-action" href={omb_result_url(run.id)} download={run.file_name.clone()}>
                    {"Download result"}
                </a>
            </article>
            if run.setup.as_ref().is_some_and(|setup| !setup.file_holds_secrets) {
                <article class="card">
                    <h3>{"Setup"}</h3>
                    <p>{"The setup.toml next to the result, with the machines, versions and \
                         properties of the run."}</p>
                    <a
                        class="run-action"
                        href={omb_setup_url(run.id)}
                        download={omb_setup_download_name(&run.file_name)}
                    >
                        {"Download setup"}
                    </a>
                </article>
            }
            <EmbedCard />
        </div>
    }
}

fn fact_group_card(group: &FactGroup) -> Html {
    html! {
        <article class="card">
            <h3>{ group.title.clone() }</h3>
            { fact_list(&group.facts) }
        </article>
    }
}

fn fact_list(facts: &[Fact]) -> Html {
    html! {
        <dl class="run-facts">
            { for facts.iter().map(|fact| html! {
                <div>
                    <dt>{ fact.label.clone() }</dt>
                    <dd>{ fact.value.clone() }</dd>
                </div>
            }) }
        </dl>
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::error::IggyBenchDashboardError;
    use crate::test_support::{
        OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED, omb_rerun,
        omb_run_list,
    };

    fn loaded() -> Loadable<OmbRunList> {
        Loadable::Ready(Rc::new(omb_run_list()))
    }

    #[test]
    fn given_fixture_id_when_looked_up_should_find_that_run() {
        let id = OMB_NO_CONSUMERS.id();

        let Lookup::Found(view) = Lookup::new(&loaded(), Some(id)) else {
            panic!("the fixture is in the list");
        };

        assert_eq!(view.run.id, id);
        assert_eq!(view.run.file_name, OMB_NO_CONSUMERS.file_name);
    }

    #[test]
    fn given_iggy_bench_id_or_unknown_id_when_looked_up_should_be_missing() {
        let uuid = Uuid::from_u128(1).to_string();
        let unknown = OMB_RC1_DEFAULT
            .id()
            .to_string()
            .chars()
            .rev()
            .collect::<String>();

        for id in [uuid, unknown] {
            let parsed = id.parse::<OmbRunId>().ok();
            assert!(
                matches!(Lookup::new(&loaded(), parsed), Lookup::Missing),
                "{id}"
            );
        }
    }

    #[test]
    fn given_list_loading_or_failed_when_looked_up_should_wait() {
        let failed = Loadable::Failed(IggyBenchDashboardError::Network("offline".to_owned()));

        for runs in [Loadable::Loading, failed] {
            assert!(matches!(
                Lookup::new(&runs, Some(OMB_RC1_DEFAULT.id())),
                Lookup::Pending
            ));
        }
    }

    #[test]
    fn given_fixture_run_when_viewed_should_compare_with_previous_version_and_relate_drivers() {
        let Lookup::Found(view) = Lookup::new(&loaded(), Some(OMB_RC1_DEFAULT.id())) else {
            panic!("the fixture is in the list");
        };

        let Some(Baseline::Version(step)) = view.baseline() else {
            panic!("the fixture's line follows the versions");
        };
        assert_eq!(step.previous, "0.9.1-edge.8");
        assert_eq!(
            step.base.iter().map(|run| run.id).collect::<Vec<_>>(),
            [OMB_EDGE8_DEFAULT.id()]
        );
        assert_eq!(
            view.related.iter().map(|run| run.id).collect::<Vec<_>>(),
            [OMB_RC1_PERSISTED.id()]
        );
    }

    #[test]
    fn given_fixture_run_when_rated_should_give_publish_change_and_consume_rate() {
        let Lookup::Found(view) = Lookup::new(&loaded(), Some(OMB_RC1_DEFAULT.id())) else {
            panic!("the fixture is in the list");
        };
        let run = &view.run;

        let [publish, consume] = rate_rows(&view);

        assert_eq!(
            publish.rates,
            Some((
                run.publish_mebibytes_per_second,
                run.publish_messages_per_second
            ))
        );
        assert_eq!(
            publish.change.as_ref().map(|change| change.versus.as_str()),
            Some("0.9.1-edge.8")
        );
        assert_eq!(
            consume.rates,
            Some((
                run.consume_mebibytes_per_second,
                run.consume_messages_per_second
            ))
        );
        assert_eq!(consume.change, None);
        assert_eq!(publish.share_pct.max(consume.share_pct), 100.0);
    }

    #[test]
    fn given_fixture_run_without_consumers_when_rated_should_leave_consume_unmeasured() {
        let Lookup::Found(view) = Lookup::new(&loaded(), Some(OMB_NO_CONSUMERS.id())) else {
            panic!("the fixture is in the list");
        };

        let [publish, consume] = rate_rows(&view);

        assert_eq!(publish.share_pct, 100.0);
        assert_eq!(consume.rates, None);
        assert_eq!(consume.share_pct, 0.0);
    }

    #[test]
    fn given_fixture_run_on_known_machines_when_noted_should_say_these_machines() {
        let Lookup::Found(view) = Lookup::new(&loaded(), Some(OMB_RC1_DEFAULT.id())) else {
            panic!("the fixture is in the list");
        };

        let notes = view.comparison_notes();

        assert_eq!(view.history.machines, Machines::Known);
        assert_eq!(
            notes,
            [format!(
                "Each change compares the median of this version, 0.10.0-rc.1, with the median \
                 of the previous version, 0.9.1-edge.8, over the runs of this workload and driver \
                 on these machines. {NO_NOISE_RULE}"
            )]
        );
        assert_eq!(view.history_note(), VERSION_ORDER_NOTE);
        assert_eq!(view.related_note(), RELATED_NOTE);
    }

    #[test]
    fn given_two_runs_without_sidecar_when_noted_should_warn_that_machines_are_unknown() {
        let first = OMB_NO_CONSUMERS.summary();
        let second = omb_rerun(&first, "00000000000000cd", 2);
        let runs = vec![second.clone(), first.clone()];

        let view = RunView::new(&second, &runs);
        let notes = view.comparison_notes();

        assert_eq!(view.baseline(), Some(Baseline::Run(&view.history.runs[0])));
        assert_eq!(view.history.runs[0].id, first.id);
        assert_eq!(view.history.machines, Machines::Unknown);
        assert_eq!(
            notes,
            [
                format!(
                    "Each change compares with the previous run of this workload and driver, {}. \
                     {NO_NOISE_RULE}",
                    run_name(&first)
                ),
                OmbComparability::SetupUnknown.explanation().to_owned(),
            ]
        );
        assert_eq!(view.history_note(), TIME_ORDER_UNKNOWN_MACHINES_NOTE);
        assert_eq!(view.related_note(), RELATED_UNKNOWN_MACHINES_NOTE);
        for note in [
            VERSION_ORDER_UNKNOWN_MACHINES_NOTE,
            TIME_ORDER_UNKNOWN_MACHINES_NOTE,
            RELATED_UNKNOWN_MACHINES_NOTE,
            notes[0].as_str(),
        ] {
            assert!(!note.contains("these machines"), "{note}");
        }
    }

    #[test]
    fn given_run_without_sidecar_when_viewed_should_have_no_baseline_and_no_environment() {
        let Lookup::Found(view) = Lookup::new(&loaded(), Some(OMB_NO_CONSUMERS.id())) else {
            panic!("the fixture is in the list");
        };

        assert_eq!(view.baseline(), None);
        assert!(view.related.is_empty());
        assert!(view.run.setup.is_none());
        assert!(!PLAIN_SECTIONS.contains(&ENVIRONMENT));
        assert!(SIDECAR_SECTIONS.contains(&ENVIRONMENT));
    }
}
