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

//! The releases compare at `/compare/releases`: two versions across the workloads of one
//! official suite. The link holds the suite and both versions, so a shared link shows the same
//! rows.

mod model;

pub use self::model::ReleasesQuery;

use std::rc::Rc;

use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::delta::DeltaVerdict;
use gloo::console::error;
use yew::prelude::*;
use yew_router::prelude::{use_location, use_navigator};

use self::model::{MetricCell, ReleaseRow, ReleaseSummary, ReleasesCompare, SuiteVersion};
use super::{CompareLink, SUMMARY_TITLE};
use crate::components::headline::{Judgment, judged_chip, metric_hint, verdict_look};
use crate::components::page::{
    Crumb, breadcrumbs, data_status, page_head, page_shell, section_head,
};
use crate::components::run_labels::{
    CONDITION_SEPARATOR, NO_RATE_LIMIT_NOTE, RELEASE_LABEL, version_tag_view,
};
use crate::components::select_field::SelectField;
use crate::components::share_button::ShareButton;
use crate::format::runs_text;
use crate::pages::run::change::{
    Change, RATE_LIMITED_HEADLINES, metric_name, metric_value_text, percent_text,
};
use crate::pages::run::history::NO_VALUE;
use crate::router::AppRoute;
use crate::state::data::{Loadable, use_dashboard_data};

const PAGE_TITLE: &str = "Compare releases";
const LEAD: &str = "Two versions across the workloads of one official suite. Each row compares \
                    both versions' runs on one workload, machine and topology, using the same rule \
                    as every other page.";
const SUITE_LABEL: &str = "Suite";
const BASE_LABEL: &str = "Base";
const HEAD_LABEL: &str = "Head";
/// Fewer versions leave nothing to compare.
const MIN_VERSIONS: usize = 2;
const DEFAULT_NOTE: &str = "The page shows the default instead.";
const NO_SUITE_NOTE: &str = "The dashboard configuration doesn't name an official suite yet.";
const FEW_VERSIONS_NOTE: &str =
    "Fewer than two versions of this suite have run, so there's nothing to compare yet.";
const SAME_VERSION_NOTE: &str = "Base and head are the same version. Pick two versions to compare.";
const NO_COMMON_NOTE: &str = "No workload in this suite ran on both versions.";
const COUNT_RULE: &str = "A workload counts once, by its strongest change: worse, then better, \
                          then not judged, then within noise.";
const ROWS_EYEBROW: &str = "Workloads";
const ROWS_TITLE: &str = "Workload by workload";
const ROWS_NOTE: &str = "Each value is the median of one version's runs, head first. Worse \
                         workloads come first, starting with the largest regression, measured in \
                         thresholds of its metric.";
const WORKLOAD_HEADER: &str = "Workload";
const RUNS_HEADER: &str = "Runs";
const LINK_HEADER: &str = "Link";
const COMPARE_LABEL: &str = "Compare";
const COMPARE_HINT: &str = "Compare the newest run of each version";

#[function_component(ReleasesPage)]
pub fn releases_page() -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let navigator = use_navigator();
    let query = use_location()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .map(ReleasesQuery::from_pairs)
        .unwrap_or_default();
    let compare = use_memo(
        (data.summaries.clone(), data.config.clone(), query),
        |(summaries, config, query)| match (summaries, config) {
            (Loadable::Ready(summaries), Loadable::Ready(config)) => Some((
                Rc::clone(config),
                ReleasesCompare::new(summaries, config, query),
            )),
            _ => None,
        },
    );
    let show = Callback::from(move |next: ReleasesQuery| {
        let Some(navigator) = &navigator else {
            return;
        };
        if let Err(cause) = navigator.push_with_query(&AppRoute::CompareReleases, &next.to_pairs())
        {
            error!(format!("Couldn't write the versions into the URL: {cause}"));
        }
    });

    let content = match &*compare {
        Some((config, compare)) => compare_view(config, compare, &show),
        None => html! {
            <>
                { head() }
                <section class="page-container">{ data_status(&data) }</section>
            </>
        },
    };
    page_shell(&scroll_ref, content)
}

fn head() -> Html {
    page_head(
        breadcrumbs(&[Crumb::overview()], PAGE_TITLE),
        None,
        PAGE_TITLE,
        LEAD,
        html! { <ShareButton class={classes!("run-action")} labeled={true} /> },
    )
}

fn compare_view(
    config: &DashboardConfig,
    compare: &ReleasesCompare,
    show: &Callback<ReleasesQuery>,
) -> Html {
    html! {
        <>
            { head() }
            <section class="page-container release-page">
                { for compare.not_found.iter().map(|missing| html! {
                    <p class="data-status data-failed" role="alert">
                        { format!("{missing} {DEFAULT_NOTE}") }
                    </p>
                }) }
                { controls(config, compare, show) }
                { body(compare) }
            </section>
        </>
    }
}

/// A new suite opens on its own default versions. A new version keeps the other side as the
/// page shows it, so the link names both versions from then on.
fn controls(
    config: &DashboardConfig,
    compare: &ReleasesCompare,
    show: &Callback<ReleasesQuery>,
) -> Html {
    let Some(suite) = &compare.suite else {
        return Html::default();
    };
    let suites: Vec<(String, String)> = config
        .suites
        .iter()
        .map(|suite| (suite.name.clone(), suite.title.clone()))
        .collect();
    let on_suite = show.reform(|name: String| ReleasesQuery {
        suite: Some(name),
        ..ReleasesQuery::default()
    });
    let versions: Vec<(String, String)> = compare
        .versions
        .iter()
        .map(|version| (version.version.clone(), option_text(version)))
        .collect();
    let shown = ReleasesQuery {
        suite: Some(suite.name.clone()),
        base: compare.base.as_ref().map(|base| base.version.clone()),
        head: compare.head.as_ref().map(|head| head.version.clone()),
    };
    let on_base = {
        let shown = shown.clone();
        show.reform(move |base: String| ReleasesQuery {
            base: Some(base),
            ..shown.clone()
        })
    };
    let on_head = show.reform(move |head: String| ReleasesQuery {
        head: Some(head),
        ..shown.clone()
    });
    html! {
        <div class="explore-facets release-controls">
            <SelectField
                label={SUITE_LABEL}
                options={suites}
                selected={suite.name.clone()}
                onchange={on_suite}
            />
            if let (Some(base), Some(head)) = (&compare.base, &compare.head) {
                { version_select(BASE_LABEL, &versions, base, on_base) }
                { version_select(HEAD_LABEL, &versions, head, on_head) }
            }
        </div>
    }
}

/// An option can only hold text, so a release says so in words. The field's face shows it styled
/// instead.
fn option_text(version: &SuiteVersion) -> String {
    if version.release {
        format!("{}{CONDITION_SEPARATOR}{RELEASE_LABEL}", version.version)
    } else {
        version.version.clone()
    }
}

fn version_select(
    label: &'static str,
    options: &[(String, String)],
    chosen: &SuiteVersion,
    onchange: Callback<String>,
) -> Html {
    html! {
        <SelectField
            {label}
            options={options.to_vec()}
            selected={chosen.version.clone()}
            {onchange}
            face={version_tag_view(&chosen.version, chosen.release)}
        />
    }
}

fn body(compare: &ReleasesCompare) -> Html {
    let note = |text: &'static str| html! { <p class="data-status">{ text }</p> };
    if compare.suite.is_none() {
        return note(NO_SUITE_NOTE);
    }
    let (Some(base), Some(head)) = (&compare.base, &compare.head) else {
        return note(FEW_VERSIONS_NOTE);
    };
    if compare.versions.len() < MIN_VERSIONS {
        return note(FEW_VERSIONS_NOTE);
    }
    if base == head {
        return note(SAME_VERSION_NOTE);
    }
    if compare.rows.is_empty() {
        return note(NO_COMMON_NOTE);
    }
    html! {
        <>
            { summary_card(&compare.summary) }
            <section class="page-section">
                { section_head(ROWS_EYEBROW, ROWS_TITLE, ROWS_NOTE) }
                { rows_table(&compare.rows, &base.version) }
            </section>
        </>
    }
}

/// The strongest verdict among the rows colors the card, the same way the strongest verdict decides
/// where a row counts.
fn summary_card(summary: &ReleaseSummary) -> Html {
    let strongest = summary
        .counts
        .iter()
        .find(|(_, count)| *count > 0)
        .map(|(verdict, _)| *verdict);
    let look = match strongest {
        Some(DeltaVerdict::Worse) => "release-summary-worse",
        Some(DeltaVerdict::Better) => "release-summary-better",
        _ => "release-summary-calm",
    };
    html! {
        <div class={classes!("card", "compare-summary", look)} role="note">
            <span class="eyebrow">{ SUMMARY_TITLE }</span>
            <strong class="compare-summary-level release-summary-total">
                { workloads_text(summary.workloads) }
            </strong>
            <ul class="release-summary-counts">
                { for summary.counts.iter().map(|&(verdict, count)| count_chip(verdict, count)) }
            </ul>
            <p>{ COUNT_RULE }</p>
        </div>
    }
}

/// A count of zero shows in gray, so only a change that actually happened gets a color.
fn count_chip(verdict: DeltaVerdict, count: usize) -> Html {
    let look = if count == 0 {
        verdict_look(DeltaVerdict::Noise)
    } else {
        verdict_look(verdict)
    };
    let label = match verdict {
        DeltaVerdict::Worse => "worse",
        DeltaVerdict::Better => "better",
        DeltaVerdict::Flagged => "not judged",
        DeltaVerdict::Noise => "within noise",
    };
    html! { <li class={classes!("run-change", look)}>{ format!("{count} {label}") }</li> }
}

fn workloads_text(count: usize) -> String {
    match count {
        1 => "1 workload".to_owned(),
        _ => format!("{count} workloads"),
    }
}

fn rows_table(rows: &[ReleaseRow], base: &str) -> Html {
    html! {
        <div class="card run-table-card">
            <table class="run-table compare-table release-table">
                <thead>
                    <tr>
                        <th scope="col">{ WORKLOAD_HEADER }</th>
                        { for RATE_LIMITED_HEADLINES.iter().map(|&metric| html! {
                            <th scope="col" class="run-table-number" title={metric_hint(metric)}>
                                { metric_name(metric) }
                            </th>
                        }) }
                        <th scope="col" class="run-table-number">{ RUNS_HEADER }</th>
                        <th scope="col"><span class="visually-hidden">{ LINK_HEADER }</span></th>
                    </tr>
                </thead>
                <tbody>
                    { for rows.iter().map(|row| row_view(row, base)) }
                </tbody>
            </table>
        </div>
    }
}

/// The head's run count first, as in the metric cells.
fn row_view(row: &ReleaseRow, base: &str) -> Html {
    html! {
        <tr>
            <th scope="row">
                <span class="compare-value">{ row.title.as_str() }</span>
                if let Some(workload) = &row.workload {
                    <small class="release-sub">{ workload.as_str() }</small>
                }
            </th>
            { for row.cells.iter().map(|cell| metric_cell(cell, base)) }
            <td class="run-table-number release-runs" data-label={RUNS_HEADER}>
                <span class="compare-value">{ runs_text(row.head.count) }</span>
                <small class="release-sub">{ format!("{base}: {}", runs_text(row.base.count)) }</small>
            </td>
            <td class="run-table-link">
                <CompareLink runs={vec![row.base.newest, row.head.newest]} title={COMPARE_HINT}>
                    { COMPARE_LABEL }
                </CompareLink>
            </td>
        </tr>
    }
}

/// The head's median, its change, then the base's median.
fn metric_cell(cell: &MetricCell, base: &str) -> Html {
    let label = metric_name(cell.metric);
    if !cell.measured {
        return html! {
            <td class="run-table-number" data-label={label} title={NO_RATE_LIMIT_NOTE}>
                <span class="compare-value">{ NO_VALUE }</span>
            </td>
        };
    }
    let text = |value: Option<f64>| {
        value.map_or_else(
            || NO_VALUE.to_owned(),
            |value| metric_value_text(cell.metric, value),
        )
    };
    html! {
        <td class="run-table-number" data-label={label}>
            <span class="compare-value">{ text(cell.head) }</span>
            if let Some(change) = &cell.change {
                { judged_chip(chip_text(change), change.ranges.clone(), Judgment::Rule(change.verdict)) }
            }
            <small class="release-sub">{ format!("{base}: {}", text(cell.base)) }</small>
        </td>
    }
}

/// Only the percent, as in the runs compare matrix, because the line below the chip already names
/// the base version.
fn chip_text(change: &Change) -> String {
    change.percent.map_or_else(|| change.text(), percent_text)
}
