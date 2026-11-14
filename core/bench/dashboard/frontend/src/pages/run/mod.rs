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

//! One run's page at `/benchmarks/{uuid}`. It shows exactly the run in the URL: the summary comes
//! from the loaded list by uuid and the report from the server by uuid, so no other run with the
//! same parameters can take its place.

mod advanced;
pub(crate) mod change;
pub(crate) mod chart;
pub(crate) mod facts;
pub(crate) mod history;
mod related;

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::identity::KindFamily;
use bench_dashboard_shared::series::{SeriesMetric, SeriesScope};
use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;
use yew::prelude::*;
use yew_router::prelude::Link;

use self::advanced::Advanced;
use self::change::{
    Change, Headline, HistoryLine, change, headlines, metric_name, regression_text,
};
use self::chart::{LATENCY_CHART_NOTE, SeriesChart};
use self::facts::{
    Command, Fact, FactGroup, NO_OPTIONS_NOTE, bench_host, conditions, environment, ladder,
    ladder_side, option_groups, parameters, topic_note,
};
use self::history::{History, history_view};
use self::related::{RelatedGroup, related_runs, related_view};
use crate::api::{artifacts_url, fetch_benchmark_by_uuid, report_url};
use crate::components::command_block::CommandBlock;
use crate::components::embed_code::{EmbedCode, MeasurementType};
use crate::components::headline::{change_chip, headline_card, metric_hint};
use crate::components::page::{
    Crumb, breadcrumbs, data_status, loadable, page_head, page_shell, section_head,
};
use crate::components::run_labels::{
    DayLabel, END_TO_END_THROUGHPUT_NOTE, NO_RATE_LIMIT_NOTE, RELEASE_LABEL, messages_text,
    milliseconds, short_id, version_tag, version_tag_view,
};
use crate::components::section_nav::{NavSection, SectionNav};
use crate::components::share_button::ShareButton;
use crate::pages::explore::family_query;
use crate::router::AppRoute;
use crate::state::data::{DashboardData, Loadable, fetch_into, use_dashboard_data};

const RUN_CRUMB: &str = "Run";
const REBUILT_LABEL: &str = "rebuilt";
const REBUILT_NOTE: &str = "The report predates the recorded command line, so this is the command \
                            the bench rebuilt from the parameters. It may differ from the command \
                            that actually ran.";
const NO_COMMAND_NOTE: &str = "The report doesn't record a command.";
const PERFORMANCE: NavSection = NavSection {
    id: "performance",
    label: "Performance",
};
const LATENCY: NavSection = NavSection {
    id: "latency",
    label: "Latency",
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
static RATE_LIMITED_SECTIONS: [NavSection; 8] = [
    PERFORMANCE,
    LATENCY,
    HISTORY,
    RELATED,
    CONFIGURATION,
    ENVIRONMENT,
    ARTIFACTS,
    ADVANCED,
];
/// A run without a rate limit measures throughput only, so it has no latency section.
static UNCAPPED_SECTIONS: [NavSection; 7] = [
    PERFORMANCE,
    HISTORY,
    RELATED,
    CONFIGURATION,
    ENVIRONMENT,
    ARTIFACTS,
    ADVANCED,
];
/// The ladder percentiles that the delta rule compares.
const LADDER_METRICS: [Metric; 5] = [
    Metric::P50,
    Metric::P90,
    Metric::P95,
    Metric::P99,
    Metric::P999,
];

#[derive(Properties, PartialEq)]
pub struct RunPageProps {
    /// As written in the URL.
    pub uuid: AttrValue,
}

#[function_component(RunPage)]
pub fn run_page(props: &RunPageProps) -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let id = Uuid::parse_str(&props.uuid).ok();
    let report = use_state_eq(|| Loadable::Loading);
    let load_report = {
        let report = report.clone();
        Callback::from(move |()| {
            if let Some(id) = id {
                fetch_into(report.clone(), async move {
                    fetch_benchmark_by_uuid(&id.to_string()).await
                });
            }
        })
    };
    {
        let load_report = load_report.clone();
        use_effect_with(id, move |_| load_report.emit(()));
    }
    let lookup = use_memo(
        (data.summaries.clone(), data.config.clone(), id),
        |(summaries, config, id)| Lookup::new(summaries, config, *id),
    );

    let content = match &*lookup {
        Lookup::Pending => pending_view(&data),
        Lookup::Missing => missing_view(&props.uuid),
        Lookup::Found(view) => found_view(view, &report, &load_report, &scroll_ref),
    };
    page_shell(&scroll_ref, content)
}

/// The URL's run in the loaded list.
enum Lookup {
    /// The list or the configuration is loading or failed.
    Pending,
    Missing,
    Found(Box<RunView>),
}

/// What the page shows about the run before its report arrives. Built once per load.
struct RunView {
    run: BenchmarkSummary,
    official: bool,
    conditions: String,
    headlines: Vec<Headline>,
    /// Each ladder percentile's change against the previous version.
    ladder_changes: Vec<(Metric, Change)>,
    regression: Option<String>,
    history: History,
    related: Vec<RelatedGroup>,
}

impl Lookup {
    fn new(
        summaries: &Loadable<Vec<BenchmarkSummary>>,
        config: &Loadable<DashboardConfig>,
        id: Option<Uuid>,
    ) -> Self {
        let (Loadable::Ready(summaries), Loadable::Ready(config)) = (summaries, config) else {
            return Self::Pending;
        };
        match id.and_then(|id| summaries.iter().find(|summary| summary.uuid == id)) {
            Some(run) => Self::Found(Box::new(RunView::new(run, summaries, config))),
            None => Self::Missing,
        }
    }
}

impl RunView {
    fn new(
        run: &BenchmarkSummary,
        summaries: &[BenchmarkSummary],
        config: &DashboardConfig,
    ) -> Self {
        let thresholds = &config.thresholds;
        let line = HistoryLine::of(run, summaries);
        let step = line.step(run);
        let headlines = headlines(run, step.as_ref(), thresholds);
        let ladder_changes = step
            .as_ref()
            .map(|step| {
                LADDER_METRICS
                    .iter()
                    .filter_map(|&metric| {
                        change(metric, run, step, thresholds).map(|change| (metric, change))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            official: config.is_official(run.suite.as_deref()),
            conditions: conditions(run),
            regression: regression_text(&headlines),
            headlines,
            ladder_changes,
            history: History::new(run, &line),
            related: related_runs(run, summaries),
            run: run.clone(),
        }
    }

    fn rate_limited(&self) -> bool {
        self.run.workload_key.rate_limit.is_some()
    }
}

fn pending_view(data: &DashboardData) -> Html {
    html! {
        <>
            { plain_head("Benchmark run", "") }
            <section class="page-container">{ data_status(data) }</section>
        </>
    }
}

fn missing_view(uuid: &str) -> Html {
    html! {
        <>
            { plain_head("Run not found", &format!("No loaded run has the id {uuid}.")) }
            <section class="page-container">
                <Link<AppRoute> to={AppRoute::Explore} classes={classes!("run-action")}>
                    {"Explore all runs"}
                </Link<AppRoute>>
            </section>
        </>
    }
}

fn plain_head(title: &str, lead: &str) -> Html {
    page_head(
        breadcrumbs(&[Crumb::new("Explore", AppRoute::Explore)], RUN_CRUMB),
        None,
        title,
        lead,
        Html::default(),
    )
}

fn found_view(
    view: &RunView,
    report: &Loadable<BenchmarkReportLight>,
    reload: &Callback<()>,
    scroll_ref: &NodeRef,
) -> Html {
    let run = &view.run;
    let sections: &'static [NavSection] = if view.rate_limited() {
        &RATE_LIMITED_SECTIONS
    } else {
        &UNCAPPED_SECTIONS
    };
    let with_report = |ready: &dyn Fn(&BenchmarkReportLight) -> Html| {
        loadable(report, "the report of the run", reload, ready)
    };
    html! {
        <>
            { head(view) }
            { headline_strip(view) }
            <SectionNav {sections} scroll_ref={scroll_ref.clone()} />

            <section id={PERFORMANCE.id} class="page-section page-container">
                { section_head(
                    PERFORMANCE.label,
                    "Over the run",
                    "One line per actor group. The bench samples the run in 10 ms windows by \
                     default, and a group's line is a moving average over them.",
                ) }
                <div class="run-charts">
                    if view.rate_limited() {
                        <article class="card run-chart-card">
                            <h3>{"Average latency"}</h3>
                            <SeriesChart
                                uuid={run.uuid}
                                metric={SeriesMetric::Latency}
                                scope={SeriesScope::Group}
                            />
                            <p class="card-note">{ LATENCY_CHART_NOTE }</p>
                        </article>
                    }
                    <article class="card run-chart-card">
                        <h3>{"Throughput"}</h3>
                        <SeriesChart
                            uuid={run.uuid}
                            metric={SeriesMetric::Throughput}
                            scope={SeriesScope::Group}
                        />
                    </article>
                </div>
            </section>

            if view.rate_limited() {
                <section id={LATENCY.id} class="page-section page-container">
                    { section_head(
                        LATENCY.label,
                        "Every percentile",
                        "On a log scale, so the median and the tail fit in one view. A group's \
                         value is the mean of its actors' percentiles.",
                    ) }
                    { with_report(&|report| latency_card(view, report)) }
                </section>
            }

            <section id={HISTORY.id} class="page-section page-container">
                { section_head(
                    HISTORY.label,
                    "Same workload across versions",
                    "Every run of this workload on this machine and topology. The line goes \
                     through each version's median, which is what the changes above compare.",
                ) }
                { history_view(&view.history, run) }
            </section>

            <section id={RELATED.id} class="page-section page-container">
                { section_head(
                    RELATED.label,
                    "One parameter apart",
                    "Runs on this machine and topology whose workload differs from this run in \
                     exactly one parameter, with the newest run for each value.",
                ) }
                { related_view(&view.related, run) }
            </section>

            <section id={CONFIGURATION.id} class="page-section page-container">
                { section_head(
                    CONFIGURATION.label,
                    "Reproduce this run",
                    "The command, parameters and options as the report recorded them.",
                ) }
                { with_report(&|report| configuration(run, report)) }
            </section>

            <section id={ENVIRONMENT.id} class="page-section page-container">
                { section_head(
                    ENVIRONMENT.label,
                    "Hardware and software",
                    "As the bench recorded them when the run started.",
                ) }
                { with_report(&|report| html! {
                    <div class="card-grid">
                        <article class="card">
                            <h3>{"Run"}</h3>
                            { fact_list(&environment(run, report)) }
                        </article>
                        <article class="card">
                            <h3>{"Bench host"}</h3>
                            { fact_list(&bench_host(&report.hardware)) }
                        </article>
                    </div>
                }) }
            </section>

            <section id={ARTIFACTS.id} class="page-section page-container">
                { section_head(
                    ARTIFACTS.label,
                    "Files of this run",
                    "The stored report, the files with the server logs, and the code to show this \
                     run's chart on another page.",
                ) }
                { artifacts(view) }
            </section>

            <section id={ADVANCED.id} class="page-section page-container">
                { section_head(
                    ADVANCED.label,
                    "For engineers",
                    "Every actor over time, the latency distribution with its tail, and the server \
                     stats. The actor charts load when you open them.",
                ) }
                <Advanced
                    uuid={run.uuid}
                    report={report.clone()}
                    reload={reload.clone()}
                />
            </section>
        </>
    }
}

fn head(view: &RunView) -> Html {
    let run = &view.run;
    let family = Crumb {
        query: family_query(run.kind_family, view.official),
        ..Crumb::new(run.kind_family.to_string(), AppRoute::Explore)
    };
    let short_id = short_id(&run.uuid);
    let detail = match (&run.commit, run.release) {
        (Some(commit), _) => Some(format!("commit {commit}")),
        (None, true) => Some(RELEASE_LABEL.to_owned()),
        (None, false) => None,
    };
    let start = run
        .version_order
        .timestamp_us
        .and_then(DayLabel::from_unix_micros);
    let above = html! {
        <>
            { breadcrumbs(&[Crumb::new("Explore", AppRoute::Explore), family], &short_id) }
            <p class="run-meta">
                { version_tag_view(&version_tag(run), run.release) }
                if let Some(detail) = detail {
                    <span>{ detail }</span>
                }
                if let Some(start) = start {
                    <span>{ start.view() }</span>
                }
            </p>
        </>
    };
    page_head(
        above,
        None,
        &run.title,
        &view.conditions,
        html! { <ShareButton class={classes!("run-action")} labeled={true} /> },
    )
}

fn headline_strip(view: &RunView) -> Html {
    let run = &view.run;
    let messages = run
        .group(run.kind_family.primary_group())
        .map(|group| messages_text(group.throughput_messages_per_second));
    let notes = [
        (!view.rate_limited()).then_some(NO_RATE_LIMIT_NOTE),
        (run.kind_family == KindFamily::EndToEnd).then_some(END_TO_END_THROUGHPUT_NOTE),
    ];
    html! {
        <section class="page-container run-headlines" aria-label="Headline numbers">
            <div class="run-kpis">
                { for view.headlines.iter().map(|headline| {
                    let detail = (headline.metric == Metric::Throughput)
                        .then(|| messages.clone())
                        .flatten();
                    headline_card(metric_name(headline.metric), headline, detail, Html::default())
                }) }
            </div>
            { for notes.into_iter().flatten().map(|note| html! {
                <p class="run-note">{ note }</p>
            }) }
            if let Some(regression) = &view.regression {
                <p class="run-banner" role="status">{ regression.as_str() }</p>
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

fn latency_card(view: &RunView, report: &BenchmarkReportLight) -> Html {
    let kind = view.run.kind_family.primary_group();
    let Some(group) = report
        .group_metrics
        .iter()
        .find(|group| group.summary.kind == kind)
    else {
        return html! {
            <p class="data-status">{ format!("The report has no numbers for {kind}.") }</p>
        };
    };
    let change_of = |metric: Option<Metric>| {
        metric.and_then(|metric| {
            view.ladder_changes
                .iter()
                .find(|(changed, _)| *changed == metric)
                .map(|(_, change)| change)
        })
    };
    let plain = view.ladder_changes.is_empty();
    html! {
        <article class="card run-ladder-card">
            <h3>{ kind.to_string() }</h3>
            <ol class={classes!("run-ladder", plain.then_some("run-ladder-plain"))}>
                { for ladder(&group.summary).into_iter().map(|row| html! {
                    <li class={classes!(
                        "run-ladder-row",
                        (row.metric == Some(Metric::P99)).then_some("run-ladder-key"),
                    )}>
                        <span class="run-ladder-label" title={row.metric.map(metric_hint)}>
                            { row.label }
                        </span>
                        <span class="run-ladder-track" aria-hidden="true">
                            <i style={format!("width: {:.1}%", row.bar_pct)}></i>
                        </span>
                        <strong>{ milliseconds(row.value_ms) }</strong>
                        <span class="run-ladder-change">
                            if let Some(change) = change_of(row.metric) {
                                { change_chip(change) }
                            }
                        </span>
                    </li>
                }) }
            </ol>
            { fact_list(&ladder_side(&group.summary)) }
        </article>
    }
}

fn configuration(run: &BenchmarkSummary, report: &BenchmarkReportLight) -> Html {
    let params = &report.params;
    let command = match Command::of(params) {
        Some(Command { text, rebuilt }) => html! {
            <div class="run-command">
                <p class="run-command-head">
                    {"Command"}
                    if rebuilt {
                        <span class="run-tag" title={REBUILT_NOTE}>{ REBUILT_LABEL }</span>
                    }
                </p>
                <CommandBlock command={text} />
                if rebuilt {
                    <p class="run-note">{ REBUILT_NOTE }</p>
                }
            </div>
        },
        None => html! { <p class="section-text">{ NO_COMMAND_NOTE }</p> },
    };
    let note = match &params.options {
        Some(options) => topic_note(run, options),
        None => Some(NO_OPTIONS_NOTE),
    };
    html! {
        <>
            { command }
            if let Some(note) = note {
                <p class="section-text">{ note }</p>
            }
            <div class="card-columns">
                <article class="card">
                    <h3>{"Parameters"}</h3>
                    { fact_list(&parameters(run, params)) }
                </article>
                { for option_groups(params).iter().map(fact_group_card) }
            </div>
        </>
    }
}

fn artifacts(view: &RunView) -> Html {
    let uuid = view.run.uuid;
    let chart = if view.rate_limited() {
        MeasurementType::Latency
    } else {
        MeasurementType::Throughput
    };
    html! {
        <div class="card-grid">
            <article class="card">
                <h3>{"Report"}</h3>
                <p>{"The full report as JSON, with every time series of every actor."}</p>
                <a class="run-action" href={report_url(&uuid)} download={format!("{uuid}.json")}>
                    {"Download report"}
                </a>
            </article>
            <article class="card">
                <h3>{"Artifacts"}</h3>
                <p>{"A zip archive of the run's files, including the server logs."}</p>
                <a class="run-action" href={artifacts_url(&uuid)}>{"Download artifacts"}</a>
            </article>
            <EmbedCard {uuid} {chart} />
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct EmbedCardProps {
    uuid: Uuid,
    chart: MeasurementType,
}

#[function_component(EmbedCard)]
fn embed_card(props: &EmbedCardProps) -> Html {
    let is_dark = use_context::<(bool, Callback<()>)>().is_some_and(|(is_dark, _)| is_dark);
    let open = use_state_eq(|| false);
    let toggle = {
        let open = open.clone();
        Callback::from(move |_: MouseEvent| open.set(!*open))
    };
    html! {
        <article class="card">
            <h3>{"Embed"}</h3>
            <p>{"The code to show this run's chart on another page, as an iframe or as an image \
                 that links here."}</p>
            <button
                type="button"
                class="run-action"
                aria-expanded={(*open).to_string()}
                onclick={toggle}
            >
                { if *open { "Hide embed code" } else { "Show embed code" } }
            </button>
            if *open {
                <EmbedCode
                    uuid={props.uuid.to_string()}
                    measurement_type={props.chart.clone()}
                    {is_dark}
                />
            }
        </article>
    }
}

fn fact_group_card(group: &FactGroup) -> Html {
    html! {
        <article class="card">
            <h3>{ group.title }</h3>
            { fact_list(&group.facts) }
        </article>
    }
}

fn fact_list(facts: &[Fact]) -> Html {
    html! {
        <dl class="run-facts">
            { for facts.iter().map(|fact| html! {
                <div>
                    <dt>{ fact.label }</dt>
                    <dd>{ fact.value.as_str() }</dd>
                </div>
            }) }
        </dl>
    }
}
