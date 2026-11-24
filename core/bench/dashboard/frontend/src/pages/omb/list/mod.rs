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

//! The OMB run list at `/omb`: every loaded OMB run in one list, narrowed by the search and facets
//! in the URL, with a compare tray of its own.

mod catalog;
mod query;

use bench_dashboard_shared::omb::labels::NO_VALUE;
use std::rc::Rc;

use gloo::console::error;
use gloo::events::{EventListener, EventListenerOptions};
use web_sys::HtmlInputElement;
use yew::prelude::*;
use yew_router::prelude::{Link, Navigator, use_location, use_navigator};

use self::catalog::{FacetCounts, Latency, OmbCatalog, OmbListView, OmbRunRow, ViewStats};
use self::query::{Facet, OmbQuery, Sort};
use crate::api::{api_name, from_api_name};
use crate::components::page::{Crumb, breadcrumbs, page_shell};
use crate::components::run_labels::version_tag_view;
use crate::components::select_field::SelectField;
use crate::pages::explore::{HistoryEntry, SEARCH_KEY, focus_search};
use crate::pages::omb::common::{
    DRIVER_HINT, END_TO_END_LATENCY_HINT, NOT_MEASURED, OMB_CRUMB, OmbCompareToggle,
    OmbCompareTray, PUBLISH_LATENCY_HINT, PUBLISH_THROUGHPUT_HINT, day_view, loaded_runs,
    omb_page_head, run_route,
};
use crate::router::AppRoute;
use crate::state::data::Loadable;
use crate::state::omb::use_omb_data;

const TITLE: &str = "OpenMessaging Benchmark runs";
const LEAD: &str = "Apache Iggy measured with the OpenMessaging Benchmark (OMB). Search the runs, \
                    narrow them by workload, driver, version and setup, open a run for its numbers \
                    over time, or pick runs to compare.";
const NOT_MEASURED_NOTE: &str = "OMB recorded no end-to-end latency for this run, for example \
                                 because it had no consumers.";
const SETUP_HINT: &str = "The driver configuration, and the machines from the setup file";
const NO_VERSION_NOTE: &str = "The run has no setup file, so its Iggy version is not known.";

#[derive(Properties)]
struct RunRowProps {
    row: Rc<OmbRunRow>,
}

/// A row never changes after the catalog builds it, so its address identifies the row.
impl PartialEq for RunRowProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.row, &other.row)
    }
}

#[function_component(OmbRunsPage)]
pub fn omb_runs_page() -> Html {
    let data = use_omb_data();
    let scroll_ref = use_node_ref();
    let search_ref = use_node_ref();
    let navigator = use_navigator();
    let query = use_location()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .map(OmbQuery::from_pairs)
        .unwrap_or_default();

    let catalog = use_memo(data.runs.clone(), |runs| match runs {
        Loadable::Ready(list) => Some(OmbCatalog::new(&list.runs)),
        _ => None,
    });
    let view = {
        let catalog = Rc::clone(&catalog);
        use_memo((data.runs.clone(), query.clone()), move |(_, query)| {
            (*catalog).as_ref().map(|catalog| catalog.view(query))
        })
    };
    {
        let search_ref = search_ref.clone();
        use_effect_with((), move |()| {
            let listener = web_sys::window()
                .and_then(|window| window.document())
                .map(|document| {
                    EventListener::new_with_options(
                        &document,
                        "keydown",
                        EventListenerOptions::enable_prevent_default(),
                        move |event| focus_search(event, &search_ref),
                    )
                });
            move || drop(listener)
        });
    }
    let show = Callback::from(move |(next, entry): (OmbQuery, HistoryEntry)| {
        if next != query
            && let Some(navigator) = &navigator
        {
            write_view(navigator, &next, entry);
        }
    });

    let stats = match (&*view, &data.runs) {
        (Some(view), Loadable::Ready(list)) if !list.runs.is_empty() => stat_tiles(&view.stats),
        _ => Html::default(),
    };
    let body = loaded_runs(&data, |_| match &*view {
        Some(view) => html! {
            <>
                { controls(view, &search_ref, &show) }
                { results(view, &show) }
            </>
        },
        None => Html::default(),
    });
    let content = html! {
        <>
            { omb_page_head(
                breadcrumbs(&[Crumb::overview()], OMB_CRUMB),
                Some(OMB_CRUMB),
                TITLE,
                LEAD,
                stats,
            ) }
            <section class="page-container omb-runs" aria-label="Runs">
                { body }
                <OmbCompareTray />
            </section>
        </>
    };
    page_shell(&scroll_ref, content)
}

#[function_component(RunRowView)]
fn run_row_view(props: &RunRowProps) -> Html {
    let row = &props.row;
    let numbers = &row.numbers;
    let end_to_end = match &numbers.end_to_end_latency {
        Some(latency) => latency_metric(latency),
        None => html! {
            <span class="explore-main omb-runs-missing" title={NOT_MEASURED_NOTE}>
                { NOT_MEASURED }
            </span>
        },
    };
    let version = match &row.version {
        Some(version) => html! {
            <>
                { version_tag_view(&version.text, version.release) }
                if let Some(detail) = &version.detail {
                    <span class="explore-sub">{ detail.as_str() }</span>
                }
            </>
        },
        None => html! {
            <span class="explore-main omb-runs-missing" title={NO_VERSION_NOTE}>{ NO_VALUE }</span>
        },
    };
    html! {
        <li>
            <Link<AppRoute> to={run_route(row.id)} classes={classes!("explore-row")}>
                <span class="explore-cell explore-run">
                    <span class="explore-run-text">
                        <span class="explore-title">{ row.workload_name.as_str() }</span>
                        if !row.badges.is_empty() {
                            <span class="explore-badges">
                                { for row.badges.iter().map(|badge| html! {
                                    <span class="badge" title={badge.explanation()}>
                                        { badge.to_string() }
                                    </span>
                                }) }
                            </span>
                        }
                    </span>
                </span>
                { cell("Setup", html! {
                    <>
                        <span class="explore-main" title={DRIVER_HINT}>{ row.driver.as_str() }</span>
                        if let Some(environment) = &row.environment {
                            <span class="explore-sub">{ environment.as_str() }</span>
                        }
                    </>
                }) }
                { cell("Version", version) }
                { cell("Publish", html! {
                    <>
                        <span class="explore-main">{ numbers.publish.as_str() }</span>
                        <span class="explore-sub">{ numbers.messages.as_str() }</span>
                    </>
                }) }
                { cell("Publish P99", latency_metric(&numbers.publish_latency)) }
                { cell("End-to-end P99", end_to_end) }
                { cell("Date", html! {
                    <span class="omb-runs-date">{ day_view(&row.time) }</span>
                }) }
            </Link<AppRoute>>
            <OmbCompareToggle run={row.id} />
        </li>
    }
}

fn write_view(navigator: &Navigator, query: &OmbQuery, entry: HistoryEntry) {
    let pairs = query.to_pairs();
    let written = match entry {
        HistoryEntry::Push => navigator.push_with_query(&AppRoute::OmbRuns, &pairs),
        HistoryEntry::Replace => navigator.replace_with_query(&AppRoute::OmbRuns, &pairs),
    };
    if let Err(cause) = written {
        error!(format!("Couldn't write the view into the URL: {cause}"));
    }
}

fn stat_tiles(stats: &ViewStats) -> Html {
    let newest = match &stats.newest {
        Some(time) => day_view(time),
        None => html! { NO_VALUE },
    };
    let tiles = [
        ("Runs", html! { stats.runs.to_string() }),
        ("Drivers", html! { stats.drivers.to_string() }),
        ("Workloads", html! { stats.workloads.to_string() }),
        ("Newest run", newest),
    ];
    html! {
        <dl class="explore-stats">
            { for tiles.into_iter().map(|(label, value)| html! {
                <div class="explore-stat">
                    <dt>{ label }</dt>
                    <dd>{ value }</dd>
                </div>
            }) }
        </dl>
    }
}

fn controls(
    view: &OmbListView,
    search_ref: &NodeRef,
    show: &Callback<(OmbQuery, HistoryEntry)>,
) -> Html {
    let query = &view.query;
    let on_search = {
        let query = query.clone();
        let show = show.clone();
        Callback::from(move |event: InputEvent| {
            let Some(input) = event.target_dyn_into::<HtmlInputElement>() else {
                return;
            };
            // The first key of a search is a step that Back undoes. The keys after it aren't.
            let entry = if query.search.is_empty() {
                HistoryEntry::Push
            } else {
                HistoryEntry::Replace
            };
            let next = OmbQuery {
                search: input.value(),
                ..query.clone()
            };
            show.emit((next, entry));
        })
    };
    let sorts: Vec<_> = Sort::ALL
        .into_iter()
        .filter_map(|sort| api_name(sort).map(|name| (name, sort.label().to_owned())))
        .collect();
    let on_sort = {
        let query = query.clone();
        let show = show.clone();
        Callback::from(move |name: String| {
            if let Some(sort) = from_api_name(&name) {
                let next = OmbQuery {
                    sort,
                    ..query.clone()
                };
                show.emit((next, HistoryEntry::Push));
            }
        })
    };
    html! {
        <div class="explore-controls">
            <div class="explore-toolbar">
                <label class="explore-search">
                    <span class="visually-hidden">{"Search OMB runs"}</span>
                    <input
                        ref={search_ref.clone()}
                        type="search"
                        class="explore-search-input"
                        placeholder="Workload, driver, version, environment or property"
                        autocomplete="off"
                        spellcheck="false"
                        value={query.search.clone()}
                        oninput={on_search}
                    />
                    <kbd class="explore-search-key" title={format!("Press {SEARCH_KEY} to search")}>
                        { SEARCH_KEY }
                    </kbd>
                </label>
                <SelectField
                    label="Sort"
                    options={sorts}
                    selected={api_name(query.sort).unwrap_or_default()}
                    onchange={on_sort}
                />
            </div>
            <div class="explore-facets">
                { for Facet::ALL.into_iter().filter_map(|facet| {
                    let counts = view.facets[facet.index()].as_ref()?;
                    Some(facet_select(facet, counts, query, show))
                }) }
            </div>
        </div>
    }
}

fn facet_select(
    facet: Facet,
    counts: &FacetCounts,
    query: &OmbQuery,
    show: &Callback<(OmbQuery, HistoryEntry)>,
) -> Html {
    let options: Vec<_> = [(
        String::new(),
        format!("{} ({})", facet.any_label(), counts.any),
    )]
    .into_iter()
    .chain(
        counts
            .options
            .iter()
            .map(|(option, count)| (option.value.clone(), format!("{} ({count})", option.label))),
    )
    .collect();
    let onchange = {
        let query = query.clone();
        let show = show.clone();
        Callback::from(move |value: String| {
            let mut next = query.clone();
            next.set_facet(facet, Some(value));
            show.emit((next, HistoryEntry::Push));
        })
    };
    html! {
        <SelectField
            label={facet.label()}
            {options}
            selected={query.facet(facet).unwrap_or_default().to_owned()}
            {onchange}
        />
    }
}

fn results(view: &OmbListView, show: &Callback<(OmbQuery, HistoryEntry)>) -> Html {
    if view.rows.is_empty() {
        return no_match(&view.query, show);
    }
    html! {
        <div class="explore-table omb-runs-table">
            <div class="explore-head" aria-hidden="true">
                <span>{"Workload"}</span>
                <span title={SETUP_HINT}>{"Setup"}</span>
                <span>{"Version"}</span>
                <span title={PUBLISH_THROUGHPUT_HINT}>{"Publish"}</span>
                <span title={PUBLISH_LATENCY_HINT}>{"Publish P99"}</span>
                <span title={END_TO_END_LATENCY_HINT}>{"End-to-end P99"}</span>
                <span>{"Date"}</span>
            </div>
            <ol class="explore-rows">
                { for view.rows.iter().map(|row| html! {
                    <RunRowView key={row.id.to_string()} row={Rc::clone(row)} />
                }) }
            </ol>
        </div>
    }
}

/// The list has runs, else the page shows the empty state of the list instead, so an empty view
/// comes from the search and the facets.
fn no_match(query: &OmbQuery, show: &Callback<(OmbQuery, HistoryEntry)>) -> Html {
    let reset = query.without_filters();
    html! {
        <div class="explore-empty" role="status">
            <p class="explore-empty-title">{"No runs match"}</p>
            <p class="explore-empty-note">{"No OMB run matches the search and filters."}</p>
            <button
                type="button"
                class="explore-reset"
                onclick={show.reform(move |_| (reset.clone(), HistoryEntry::Push))}
            >
                {"Reset filters"}
            </button>
        </div>
    }
}

/// The table head names the cells on a wide screen. On a narrow one, the rows become cards and
/// each cell shows its label.
fn cell(label: &'static str, content: Html) -> Html {
    html! {
        <span class="explore-cell">
            <span class="explore-label visually-hidden">{ label }</span>
            { content }
        </span>
    }
}

fn latency_metric(latency: &Latency) -> Html {
    html! {
        <>
            <span class="explore-main">{ latency.p99.as_str() }</span>
            <span class="explore-sub">{ format!("P50 {}", latency.p50) }</span>
        </>
    }
}
