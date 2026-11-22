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

//! One run's page at `/runs/{uuid}`. It shows exactly the run in the URL: the summary comes
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
use bench_dashboard_shared::embed::ChartType;
use bench_dashboard_shared::identity::KindFamily;
use bench_dashboard_shared::metrics::GroupMetrics;
use bench_dashboard_shared::series::{SeriesMetric, SeriesScope};
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::group_metrics_kind::GroupMetricsKind;
use uuid::Uuid;
use yew::prelude::*;
use yew_router::prelude::Link;

use self::advanced::Advanced;
use self::change::{
    Change, Headline, HistoryLine, change, headlines, metric_name, metric_value_text,
    regression_text,
};
use self::chart::{LATENCY_CHART_NOTE, SERIES_COLOR_VARS, SeriesChart};
use self::facts::{
    Command, Fact, FactGroup, NO_OPTIONS_NOTE, bench_host, conditions, environment, ladder,
    ladder_side, option_groups, parameters, topic_note,
};
use self::history::{History, history_view};
use self::related::{RelatedGroup, related_runs, related_view};
use crate::api::{artifacts_url, fetch_benchmark_by_uuid, report_url};
use crate::components::chart::series_color;
use crate::components::command_block::CommandBlock;
use crate::components::embed::{EmbedButton, EmbedCard, EmbedChart, EmbedProvider, EmbedRun};
use crate::components::framework::{Framework, framework_chip};
use crate::components::headline::{change_chip, compact_change_chip, headline_card, metric_hint};
use crate::components::ladder::{Rung, percentile_ladder};
use crate::components::page::{
    Crumb, breadcrumbs, data_status, loadable, page_head, page_shell, section_head,
};
use crate::components::run_labels::{
    DayLabel, END_TO_END_THROUGHPUT_NOTE, NO_RATE_LIMIT_NOTE, messages_text, milliseconds,
    short_id, version_detail, version_tag, version_tag_view,
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
const LATENCY_NOTE: &str = "One line per actor group. The bench samples the run in 10 ms windows by \
                            default, and a group's line is a moving average over them.";
const THROUGHPUT_NOTE: &str = "One line per actor group, a moving average over the 10 ms sampling \
                               windows of the bench. The card next to the chart gives each group's \
                               rates over the whole run.";
const RATES_TITLE: &str = "Over the whole run";
const RATE_HINT: &str = "The group's user data over the run time of its slowest actor.";
const MESSAGES_HINT: &str = "The group's messages over the run time of its slowest actor.";
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
static RATE_LIMITED_SECTIONS: [NavSection; 9] = [
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
/// A run without a rate limit measures throughput only, so it has no latency sections.
static UNCAPPED_SECTIONS: [NavSection; 7] = [
    THROUGHPUT,
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

/// One row of the rates card next to the throughput chart.
#[derive(Debug, PartialEq)]
struct GroupRate<'a> {
    group: &'a GroupMetrics,
    /// The headline's change, on the main group only.
    change: Option<&'a Change>,
    /// The group's rate against the fastest group, for the length of its bar.
    share_pct: f64,
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
        <EmbedProvider run={EmbedRun::IggyBench(run.uuid)} charts={embed_charts(view, report)}>
            { head(view) }
            { headline_strip(view) }
            <SectionNav {sections} scroll_ref={scroll_ref.clone()} />

            if view.rate_limited() {
                <section id={LATENCY.id} class="page-section page-container">
                    { section_head(LATENCY.label, "Every window", LATENCY_NOTE) }
                    <article class="card run-chart-card">
                        <h3>{"Average latency"}</h3>
                        <EmbedButton chart={EmbedChart::IggyBench(ChartType::Latency)} />
                        <SeriesChart
                            uuid={run.uuid}
                            metric={SeriesMetric::Latency}
                            scope={SeriesScope::Group}
                        />
                        <p class="card-note">{ LATENCY_CHART_NOTE }</p>
                    </article>
                </section>

                <section id={PERCENTILES.id} class="page-section page-container">
                    { section_head(
                        PERCENTILES.label,
                        "Every percentile",
                        "On a log scale, so the median and the tail fit in one view. A group's \
                         value is the mean of its actors' percentiles.",
                    ) }
                    { with_report(&|report| latency_card(view, report)) }
                </section>
            }

            <section id={THROUGHPUT.id} class="page-section page-container">
                { section_head(THROUGHPUT.label, "Start to finish", THROUGHPUT_NOTE) }
                <div class="chart-with-card">
                    <article class="card run-chart-card">
                        <h3>{"Throughput"}</h3>
                        <EmbedButton chart={EmbedChart::IggyBench(ChartType::Throughput)} />
                        <SeriesChart
                            uuid={run.uuid}
                            metric={SeriesMetric::Throughput}
                            scope={SeriesScope::Group}
                        />
                    </article>
                    { rates_card(view) }
                </div>
            </section>

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
                     run's charts on another page.",
                ) }
                { artifacts(view) }
            </section>

            <section id={ADVANCED.id} class="page-section page-container">
                { section_head(
                    ADVANCED.label,
                    "Additional insights",
                    "Every actor over time, the latency distribution with its tail, and the server \
                     stats. The actor charts load when you open them.",
                ) }
                <Advanced
                    uuid={run.uuid}
                    report={report.clone()}
                    reload={reload.clone()}
                />
            </section>
        </EmbedProvider>
    }
}

/// The charts of the run that an embed can draw, in the order of the page. A run without a rate
/// limit shows its throughput first, and a report without a histogram has no distribution.
fn embed_charts(view: &RunView, report: &Loadable<BenchmarkReportLight>) -> Vec<EmbedChart> {
    let over_time = if view.rate_limited() {
        [ChartType::Latency, ChartType::Throughput]
    } else {
        [ChartType::Throughput, ChartType::Latency]
    };
    let distribution =
        matches!(report, Loadable::Ready(report) if report.has_latency_distribution())
            .then_some(ChartType::Distribution);
    over_time
        .into_iter()
        .chain(distribution)
        .map(EmbedChart::IggyBench)
        .collect()
}

fn head(view: &RunView) -> Html {
    let run = &view.run;
    let family = Crumb {
        query: family_query(run.kind_family, view.official),
        ..Crumb::new(run.kind_family.to_string(), AppRoute::Explore)
    };
    let short_id = short_id(&run.uuid);
    let detail = version_detail(run.commit.as_deref(), run.release);
    let start = run
        .version_order
        .timestamp_us
        .and_then(DayLabel::from_unix_micros);
    let above = html! {
        <>
            { breadcrumbs(&[Crumb::new("Explore", AppRoute::Explore), family], &short_id) }
            <p class="run-meta">
                { framework_chip(Framework::IggyBench) }
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
    let rungs: Vec<Rung> = ladder(&group.summary)
        .into_iter()
        .map(|row| Rung {
            label: row.label,
            hint: row.metric.map(metric_hint),
            value: milliseconds(row.value_ms),
            bar_pct: row.bar_pct,
            key: row.metric == Some(Metric::P99),
            change: change_of(row.metric).map(change_chip).unwrap_or_default(),
        })
        .collect();
    html! {
        <article class="card run-ladder-card">
            <h3>{ kind.to_string() }</h3>
            { percentile_ladder(&rungs, !view.ladder_changes.is_empty()) }
            { fact_list(&ladder_side(&group.summary)) }
        </article>
    }
}

/// The groups of the throughput chart in its line colors, with the change of the main group from
/// the headline.
fn rates_card(view: &RunView) -> Html {
    let items = group_rates(&view.run, &view.headlines)
        .into_iter()
        .zip(SERIES_COLOR_VARS)
        .map(|(rate, color)| {
            let group = rate.group;
            html! {
                <li class="rates-row" style={series_color(color)}>
                    <span class="rates-name">
                        <span class="series-swatch" aria-hidden="true"></span>
                        <span class="rates-label">{ group.kind.to_string() }</span>
                    </span>
                    <span class="rates-change">
                        if let Some(change) = rate.change {
                            { compact_change_chip(change) }
                        }
                    </span>
                    <strong class="rates-value" title={RATE_HINT}>
                        { metric_value_text(Metric::Throughput, group.throughput_megabytes_per_second) }
                    </strong>
                    <span class="rates-messages" title={MESSAGES_HINT}>
                        { messages_text(group.throughput_messages_per_second) }
                    </span>
                    <span class="rates-bar" aria-hidden="true">
                        <span style={format!("width: {:.1}%", rate.share_pct)}></span>
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

/// The groups that the throughput chart draws, in its order. Like the chart, it leaves out the
/// combined group of producers and consumers.
fn group_rates<'a>(run: &'a BenchmarkSummary, headlines: &'a [Headline]) -> Vec<GroupRate<'a>> {
    let groups: Vec<&GroupMetrics> = run
        .groups
        .iter()
        .filter(|group| group.kind != GroupMetricsKind::ProducersAndConsumers)
        .collect();
    let fastest = groups
        .iter()
        .map(|group| group.throughput_megabytes_per_second)
        .fold(0.0, f64::max);
    let primary = run.kind_family.primary_group();
    let change = headlines
        .iter()
        .find(|headline| headline.metric == Metric::Throughput)
        .and_then(|headline| headline.change.as_ref());
    groups
        .into_iter()
        .map(|group| GroupRate {
            group,
            change: change.filter(|_| group.kind == primary),
            share_pct: if fastest > 0.0 {
                group.throughput_megabytes_per_second / fastest * 100.0
            } else {
                0.0
            },
        })
        .collect()
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
            <EmbedCard />
        </div>
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

#[cfg(test)]
mod tests {
    use bench_report::benchmark_kind::BenchmarkKind;

    use super::*;
    use crate::test_support::{
        CANDIDATE, MACHINE, RELEASE, THRESHOLDS, params, rate_limited, run, with_numbers,
    };

    /// A producer and consumer run whose report holds both groups and their combination.
    fn producer_and_consumer(id: u128, gitref: &str, producers: f64) -> BenchmarkSummary {
        let workload = rate_limited(params(BenchmarkKind::PinnedProducerAndConsumer, 1, 1));
        let mut summary = with_numbers(run(id, workload, MACHINE, gitref), producers, 2.0);
        let main = summary.groups[0].clone();
        summary.groups.extend([
            GroupMetrics {
                kind: GroupMetricsKind::Consumers,
                throughput_megabytes_per_second: producers / 2.0,
                ..main.clone()
            },
            GroupMetrics {
                kind: GroupMetricsKind::ProducersAndConsumers,
                throughput_megabytes_per_second: producers * 1.5,
                ..main
            },
        ]);
        summary
    }

    #[test]
    fn given_run_with_combined_group_when_rated_should_give_chart_groups_with_main_change() {
        let runs = vec![
            producer_and_consumer(1, RELEASE, 500.0),
            producer_and_consumer(2, CANDIDATE, 600.0),
        ];
        let current = &runs[1];
        let line = HistoryLine::of(current, &runs);
        let headlines = headlines(current, line.step(current).as_ref(), &THRESHOLDS);

        let rates = group_rates(current, &headlines);

        let shown: Vec<_> = rates
            .iter()
            .map(|rate| {
                (
                    rate.group.kind,
                    rate.share_pct,
                    rate.change.map(Change::text),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                (
                    GroupMetricsKind::Producers,
                    100.0,
                    Some("+20.0% vs 0.9.0".to_owned())
                ),
                (GroupMetricsKind::Consumers, 50.0, None),
            ]
        );
    }

    #[test]
    fn given_first_run_of_its_line_when_rated_should_give_no_change() {
        let current = producer_and_consumer(1, RELEASE, 500.0);
        let headlines = headlines(&current, None, &THRESHOLDS);

        let rates = group_rates(&current, &headlines);

        assert!(rates.iter().all(|rate| rate.change.is_none()));
        assert_eq!(rates.len(), 2);
    }
}
