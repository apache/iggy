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

//! The Overview at `/`: the headline workloads with the latest release's numbers, the trend across
//! releases, the change in newer builds, and how the runs are measured.

mod headlines;

use yew::prelude::*;
use yew_router::prelude::Link;

use self::headlines::{HeadlineCard, Headlines, ReleaseLink, SinceRelease};
use crate::components::headline::{change_chip, headline_card};
use crate::components::page::{data_status, page_head, page_shell, section_head};
use crate::components::run_labels::{MachineName, version_tag_view};
use crate::format::runs_text;
use crate::pages::methodology::{NOISE_LEAD, PERCENTILE_RULE, THROUGHPUT_RULE};
use crate::pages::run::change::{metric_name, metric_value_text};
use crate::pages::run::chart::VersionChart;
use crate::router::AppRoute;
use crate::state::data::{Loadable, use_dashboard_data};

const EYEBROW: &str = "Overview";
const PRODUCT: &str = "Apache Iggy";
const LEAD_START: &str = "Numbers for the workloads the maintainers chose";
const LEAD_END: &str = "Each card shows the median of a release's runs and its change against the \
                        previous release.";
const CHOSEN_NOTE: &str =
    "The maintainers choose these workloads, so this page never picks a run by its numbers.";
const NO_HEADLINES_NOTE: &str = "The maintainers haven't chosen any workloads for this page yet.";
const NO_RELEASE_NOTE: &str = "No release has run this workload yet.";
const NEWEST_RUN_LABEL: &str = "Open the newest run";
const TREND_EYEBROW: &str = "Release trend";
const TREND_TITLE: &str = "Across releases";
const TREND_NOTE: &str = "One point per run and a line through each release's median. A workload \
                          gets a chart once two releases have run it.";
const SINCE_EYEBROW: &str = "Since the last release";
const SINCE_TITLE: &str = "Newer builds";
const SINCE_NOTE: &str = "Each row compares the latest release's runs with the runs of the newest \
                          build after it, on the same machine and topology.";
const WORKLOAD_HEADER: &str = "Workload";
const METRIC_HEADER: &str = "Metric";
const RELEASE_HEADER: &str = "Release";
const BUILD_HEADER: &str = "Build";
const CHANGE_HEADER: &str = "Change";
const RELEASE_LINKS_LEAD: &str = "Every workload in a suite, the release against the newest build:";
const MEASURE_EYEBROW: &str = "How we measure";
const MEASURE_TITLE: &str = "Reading the numbers";
const MEASURE_NOTE: &str = "The rules behind every number on this page, in the Methodology page's \
                            words.";
const METHODOLOGY_LINK: &str = "Read the methodology";
/// Titles with the opening sentences of Methodology paragraphs, so the two pages never disagree.
const FACTS: [(&str, &str); 3] = [
    ("Throughput", THROUGHPUT_RULE),
    ("Latency", PERCENTILE_RULE),
    ("Run-to-run noise", NOISE_LEAD),
];
type LinkQuery = Vec<(&'static str, String)>;

#[function_component(Overview)]
pub fn overview() -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let headlines = use_memo(
        (data.summaries.clone(), data.config.clone()),
        |(summaries, config)| match (summaries, config) {
            (Loadable::Ready(summaries), Loadable::Ready(config)) => {
                Some(Headlines::new(summaries, config))
            }
            _ => None,
        },
    );

    let content = match &*headlines {
        Some(headlines) => html! {
            <>
                { hero(headlines) }
                { cards(headlines) }
                { trends(headlines) }
                { since_release(headlines) }
                { how_we_measure() }
            </>
        },
        None => html! {
            <>
                { page_head(Html::default(), Some(EYEBROW), PRODUCT, LEAD_END, Html::default()) }
                <section class="page-container">{ data_status(&data) }</section>
            </>
        },
    };
    page_shell(&scroll_ref, content)
}

fn hero(headlines: &Headlines) -> Html {
    let title = headlines.latest_release.as_ref().map_or_else(
        || PRODUCT.to_owned(),
        |release| format!("{PRODUCT} {release}"),
    );
    let lead = match &headlines.machine {
        Some(machine) => format!("{LEAD_START}, on {}. {LEAD_END}", MachineName(machine)),
        None => format!("{LEAD_START}. {LEAD_END}"),
    };
    page_head(
        Html::default(),
        Some(EYEBROW),
        &title,
        &lead,
        Html::default(),
    )
}

fn cards(headlines: &Headlines) -> Html {
    let latest = headlines.latest_release.as_deref();
    html! {
        <section class="page-container" aria-label="Headline workloads">
            if headlines.cards.is_empty() {
                <p class="data-status">{ NO_HEADLINES_NOTE }</p>
            } else {
                <div class="card-grid overview-cards">
                    { for headlines.cards.iter().map(|card| card_view(card, latest)) }
                </div>
                <p class="run-note">{ CHOSEN_NOTE }</p>
            }
        </section>
    }
}

/// A card names its release when it isn't the latest one.
fn card_view(card: &HeadlineCard, latest: Option<&str>) -> Html {
    let Some(release) = &card.release else {
        return headline_card(
            &card.title,
            &card.headline,
            None,
            html! { <p class="run-note">{ NO_RELEASE_NOTE }</p> },
        );
    };
    let runs = match release.runs {
        1 => runs_text(1),
        count => format!("median of {}", runs_text(count)),
    };
    let detail = format!(
        "{} of {}, {runs}",
        metric_name(card.headline.metric),
        release.version
    );
    let older = latest.filter(|latest| *latest != release.version);
    let extra = html! {
        <>
            if let Some(latest) = older {
                <p class="run-note">{ format!("No run of {latest} yet.") }</p>
            }
            <p class="overview-card-run">{ release.run_title.as_str() }</p>
            <p class="overview-card-conditions">{ release.conditions.as_str() }</p>
            <Link<AppRoute>
                to={AppRoute::Benchmark { uuid: release.newest.to_string() }}
                classes={classes!("overview-card-link")}
            >
                { NEWEST_RUN_LABEL }
            </Link<AppRoute>>
        </>
    };
    headline_card(&card.title, &card.headline, Some(detail), extra)
}

/// Hidden while no headline workload has two releases.
fn trends(headlines: &Headlines) -> Html {
    if headlines.trends.is_empty() {
        return Html::default();
    }
    html! {
        <section class="page-section page-container">
            { section_head(TREND_EYEBROW, TREND_TITLE, TREND_NOTE) }
            <div class="run-charts">
                { for headlines.trends.iter().map(|trend| html! {
                    <article class="card run-chart-card">
                        // The chart's y axis names the metric.
                        <h3>{ trend.title.as_str() }</h3>
                        <VersionChart uuid={trend.uuid} chart={trend.chart.clone()} />
                    </article>
                }) }
            </div>
        </section>
    }
}

/// Hidden while no build ran a headline workload after its release.
fn since_release(headlines: &Headlines) -> Html {
    let rows = &headlines.since_release;
    if rows.is_empty() {
        return Html::default();
    }
    html! {
        <section class="page-section page-container">
            { section_head(SINCE_EYEBROW, SINCE_TITLE, SINCE_NOTE) }
            <div class="card run-table-card">
                <table class="run-table">
                    <thead>
                        <tr>
                            <th scope="col">{ WORKLOAD_HEADER }</th>
                            <th scope="col">{ METRIC_HEADER }</th>
                            <th scope="col">{ RELEASE_HEADER }</th>
                            <th scope="col">{ BUILD_HEADER }</th>
                            <th scope="col">{ CHANGE_HEADER }</th>
                        </tr>
                    </thead>
                    <tbody>
                        { for rows.iter().map(since_row) }
                    </tbody>
                </table>
            </div>
            <p class="overview-release-links">
                { RELEASE_LINKS_LEAD }
                { for headlines.release_links.iter().map(release_link) }
            </p>
        </section>
    }
}

fn release_link(link: &ReleaseLink) -> Html {
    html! {
        <Link<AppRoute, LinkQuery>
            to={AppRoute::CompareReleases}
            query={Some(link.query.to_pairs())}
            classes={classes!("run-action")}
        >
            { link.title.as_str() }
        </Link<AppRoute, LinkQuery>>
    }
}

/// On a phone, the first cell labels the row, and every other cell names its column.
fn since_row(row: &SinceRelease) -> Html {
    let value = |value| metric_value_text(row.metric, value);
    html! {
        <tr>
            <td>{ row.title.as_str() }</td>
            <td data-label={METRIC_HEADER}>{ metric_name(row.metric) }</td>
            <td data-label={RELEASE_HEADER}>
                <span class="overview-version-value">
                    { version_tag_view(&row.release, true) }
                    { value(row.release_value) }
                </span>
            </td>
            <td data-label={BUILD_HEADER}>
                <span class="overview-version-value">
                    { version_tag_view(&row.build, false) }
                    { value(row.build_value) }
                </span>
            </td>
            <td data-label={CHANGE_HEADER}>{ change_chip(&row.change) }</td>
        </tr>
    }
}

fn how_we_measure() -> Html {
    html! {
        <section class="page-section page-container">
            { section_head(MEASURE_EYEBROW, MEASURE_TITLE, MEASURE_NOTE) }
            <div class="card-grid">
                { for FACTS.iter().map(|(title, fact)| html! {
                    <article class="card">
                        <h3>{ *title }</h3>
                        <p>{ *fact }</p>
                    </article>
                }) }
            </div>
            <Link<AppRoute> to={AppRoute::Methodology} classes={classes!("run-action")}>
                { METHODOLOGY_LINK }
            </Link<AppRoute>>
        </section>
    }
}
