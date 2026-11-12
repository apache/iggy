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

//! The Explore page: every loaded run in one list, narrowed by the scope, search and facets in the
//! URL.

mod catalog;
mod query;
mod tray;

use std::rc::Rc;

use bench_dashboard_shared::identity::KindFamily;
use gloo::console::error;
use gloo::events::{EventListener, EventListenerOptions};
use wasm_bindgen::JsCast;
use web_sys::{HtmlElement, HtmlInputElement, KeyboardEvent};
use yew::prelude::*;
use yew_router::prelude::{Link, Navigator, use_location, use_navigator};

use self::catalog::{Catalog, ExploreView, FacetCounts, RunRow, ViewStats};
use self::query::{ExploreQuery, Facet, Scope, Sort};
use self::tray::{CompareToggle, CompareTray};
use crate::api::{api_name, from_api_name};
use crate::components::page::{Crumb, breadcrumbs, data_status, page_head, page_shell};
use crate::components::run_labels::{
    END_TO_END_THROUGHPUT_NOTE, NO_RATE_LIMIT_NOTE, RELEASE_LABEL, family_mark, version_tag_view,
};
use crate::components::select_field::SelectField;
use crate::router::AppRoute;
use crate::state::data::{Loadable, use_dashboard_data};

const SEARCH_KEY: &str = "/";
/// The elements where "/" is text, not the search shortcut.
const TEXT_FIELD_TAGS: [&str; 2] = ["INPUT", "TEXTAREA"];
const NO_VALUE: &str = "-";
const NO_RELEASE: &str = "None";

/// How a view change enters the browser history.
#[derive(Clone, Copy)]
enum HistoryEntry {
    /// A step that Back undoes.
    Push,
    /// The next key typed into the search. Otherwise every key would add a step.
    Replace,
}

#[derive(Properties)]
struct RunRowProps {
    row: Rc<RunRow>,
}

/// A row never changes after the catalog builds it, so its address identifies the row.
impl PartialEq for RunRowProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.row, &other.row)
    }
}

#[function_component(Explore)]
pub fn explore() -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let search_ref = use_node_ref();
    let navigator = use_navigator();
    let query = use_location()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .map(ExploreQuery::from_pairs)
        .unwrap_or_default();

    let catalog = use_memo(
        (data.summaries.clone(), data.config.clone()),
        |(summaries, config)| match (summaries, config) {
            (Loadable::Ready(summaries), Loadable::Ready(config)) => {
                Some(Catalog::new(summaries, config))
            }
            _ => None,
        },
    );
    let view = {
        let catalog = Rc::clone(&catalog);
        use_memo(
            (data.summaries.clone(), data.config.clone(), query.clone()),
            move |(_, _, query)| (*catalog).as_ref().map(|catalog| catalog.view(query)),
        )
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
    let show = Callback::from(move |(next, entry): (ExploreQuery, HistoryEntry)| {
        if next != query
            && let Some(navigator) = &navigator
        {
            write_view(navigator, &next, entry);
        }
    });

    let stats = match &*view {
        Some(view) => stat_tiles(&view.stats),
        None => Html::default(),
    };
    let body = match &*view {
        Some(view) => html! {
            <>
                { controls(view, &search_ref, &show) }
                { results(view, &show) }
            </>
        },
        None => data_status(&data),
    };
    let content = html! {
        <>
            { page_head(
                breadcrumbs(&[Crumb::overview()], "Explore"),
                Some("Explore"),
                "Benchmark runs",
                "Search the saved runs, narrow them by machine, version, kind and conditions, and \
                 open a run for its full report.",
                stats,
            ) }
            <section class="page-container" aria-label="Runs">
                { body }
                <CompareTray />
            </section>
        </>
    };
    page_shell(&scroll_ref, content)
}

/// The query for the Explore view that lists the runs of `family`, either in the official scope or
/// across all runs.
pub fn family_query(family: KindFamily, official: bool) -> Vec<(&'static str, String)> {
    let mut query = ExploreQuery {
        scope: if official {
            Scope::Official
        } else {
            Scope::All
        },
        ..ExploreQuery::default()
    };
    query.set_facet(Facet::Kind, api_name(family));
    query.to_pairs()
}

#[function_component(RunRowView)]
fn run_row_view(props: &RunRowProps) -> Html {
    let row = &props.row;
    let throughput_note =
        (row.family == KindFamily::EndToEnd).then_some(END_TO_END_THROUGHPUT_NOTE);
    let (throughput, latency) = match &row.numbers {
        Some(numbers) => (
            metric(&numbers.throughput, &numbers.messages, throughput_note),
            match &numbers.latency {
                Some(latency) => metric(&latency.p99, &format!("P50 {}", latency.p50), None),
                None => no_value(Some(NO_RATE_LIMIT_NOTE)),
            },
        ),
        None => (no_value(None), no_value(None)),
    };
    let version_detail = row
        .commit
        .as_deref()
        .or(row.release.then_some(RELEASE_LABEL));
    html! {
        <li>
            <Link<AppRoute>
                to={AppRoute::Benchmark { uuid: row.uuid.to_string() }}
                classes={classes!("explore-row")}
            >
                <span class="explore-cell explore-run">
                    { family_mark(row.family) }
                    <span class="explore-run-text">
                        <span class="explore-title">{ row.title.as_str() }</span>
                        <span class="explore-sub">{ row.conditions.as_str() }</span>
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
                { cell("Machine", "explore-machine", html! {
                    <>
                        <span class="explore-main">{ row.machine.as_str() }</span>
                        if let Some(cpu) = &row.cpu {
                            <span class="explore-sub" title={AttrValue::from(Rc::clone(&cpu.brand))}>
                                { cpu.name.as_str() }
                            </span>
                        }
                        if let Some(specs) = &row.specs {
                            <span class="explore-sub">{ specs.as_str() }</span>
                        }
                        <span class="explore-sub">{ row.topology.as_str() }</span>
                    </>
                }) }
                { cell("Version", "explore-version", html! {
                    <>
                        { version_tag_view(&row.version_tag, row.release) }
                        if let Some(detail) = version_detail {
                            <span class="explore-sub">{ detail }</span>
                        }
                    </>
                }) }
                { cell("Throughput", "explore-throughput", throughput) }
                { cell("P99 latency", "explore-latency", latency) }
                { cell("Date", "explore-date", match &row.start {
                    Some(start) => start.view(),
                    None => no_value(None),
                }) }
            </Link<AppRoute>>
            <CompareToggle run={row.uuid} />
        </li>
    }
}

/// "/" moves the focus to the search box, unless the focus is in a field where "/" is text.
fn focus_search(event: &Event, search_ref: &NodeRef) {
    let Some(event) = event.dyn_ref::<KeyboardEvent>() else {
        return;
    };
    if event.key() != SEARCH_KEY || event.ctrl_key() || event.meta_key() || event.alt_key() {
        return;
    }
    let in_text_field = event
        .target()
        .and_then(|target| target.dyn_into::<HtmlElement>().ok())
        .is_some_and(|element| {
            element.is_content_editable() || TEXT_FIELD_TAGS.contains(&element.tag_name().as_str())
        });
    if in_text_field {
        return;
    }
    if let Some(search) = search_ref.cast::<HtmlInputElement>() {
        event.prevent_default();
        if let Err(cause) = search.focus() {
            error!("Couldn't focus the search box:", cause);
        }
    }
}

fn write_view(navigator: &Navigator, query: &ExploreQuery, entry: HistoryEntry) {
    let pairs = query.to_pairs();
    let written = match entry {
        HistoryEntry::Push => navigator.push_with_query(&AppRoute::Explore, &pairs),
        HistoryEntry::Replace => navigator.replace_with_query(&AppRoute::Explore, &pairs),
    };
    if let Err(cause) = written {
        error!(format!("Couldn't write the view into the URL: {cause}"));
    }
}

fn stat_tiles(stats: &ViewStats) -> Html {
    let tiles = [
        ("Runs", stats.runs.to_string()),
        ("Machines", stats.machines.to_string()),
        ("Versions", stats.versions.to_string()),
        (
            "Latest release",
            stats
                .latest_release
                .clone()
                .unwrap_or_else(|| NO_RELEASE.to_owned()),
        ),
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
    view: &ExploreView,
    search_ref: &NodeRef,
    show: &Callback<(ExploreQuery, HistoryEntry)>,
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
            let next = ExploreQuery {
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
                let next = ExploreQuery {
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
                <div class="explore-scope" role="group" aria-label="Runs to list">
                    { for Scope::ALL.map(|scope| {
                        let next = ExploreQuery { scope, ..query.clone() };
                        html! {
                            <button
                                type="button"
                                class="explore-scope-option"
                                aria-pressed={(query.scope == scope).to_string()}
                                onclick={show.reform(move |_| (next.clone(), HistoryEntry::Push))}
                            >
                                { scope.label() }
                            </button>
                        }
                    }) }
                </div>
                <label class="explore-search">
                    <span class="visually-hidden">{"Search runs"}</span>
                    <input
                        ref={search_ref.clone()}
                        type="search"
                        class="explore-search-input"
                        placeholder="Title, version, commit, suite or workload"
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
                { for Facet::ALL
                    .into_iter()
                    .filter(|facet| *facet != Facet::Kind)
                    .map(|facet| facet_select(facet, &view.facets[facet.index()], query, show)) }
            </div>
            { kind_chips(&view.facets[Facet::Kind.index()], query, show) }
        </div>
    }
}

fn facet_select(
    facet: Facet,
    counts: &FacetCounts,
    query: &ExploreQuery,
    show: &Callback<(ExploreQuery, HistoryEntry)>,
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

/// The kind facet as chips, because four families fit in one row and read faster than a list.
fn kind_chips(
    counts: &FacetCounts,
    query: &ExploreQuery,
    show: &Callback<(ExploreQuery, HistoryEntry)>,
) -> Html {
    let selected = query.facet(Facet::Kind);
    let chip = |value: Option<&str>, label: &str, count: usize| {
        let mut next = query.clone();
        next.set_facet(Facet::Kind, value.map(str::to_owned));
        html! {
            <button
                type="button"
                class="explore-kind"
                aria-pressed={(selected == value).to_string()}
                onclick={show.reform(move |_| (next.clone(), HistoryEntry::Push))}
            >
                { label }
                <span class="explore-kind-count">{ count }</span>
            </button>
        }
    };
    html! {
        <div class="explore-kinds" role="group" aria-label={Facet::Kind.label()}>
            { chip(None, Facet::Kind.any_label(), counts.any) }
            { for counts.options.iter().map(|(option, count)| {
                chip(Some(option.value.as_str()), &option.label, *count)
            }) }
        </div>
    }
}

fn results(view: &ExploreView, show: &Callback<(ExploreQuery, HistoryEntry)>) -> Html {
    if view.rows.is_empty() {
        return empty_state(&view.query, show);
    }
    html! {
        <div class="explore-table">
            <div class="explore-head" aria-hidden="true">
                <span>{"Run"}</span>
                <span>{"Machine"}</span>
                <span>{"Version"}</span>
                <span>{"Throughput"}</span>
                <span>{"P99 latency"}</span>
                <span>{"Date"}</span>
            </div>
            <ol class="explore-rows">
                { for view.rows.iter().map(|row| html! {
                    <RunRowView key={row.uuid.to_string()} row={Rc::clone(row)} />
                }) }
            </ol>
        </div>
    }
}

/// Without filters, an empty list means that no run is official, as in a local setup, so the
/// way out is the other scope.
fn empty_state(query: &ExploreQuery, show: &Callback<(ExploreQuery, HistoryEntry)>) -> Html {
    let (title, note, action) = if query.has_filters() {
        (
            "No runs match",
            "No run matches the search and filters.",
            Some(("Reset filters", query.without_filters())),
        )
    } else if query.scope == Scope::Official {
        (
            "No official runs",
            "None of the runs belong to one of this dashboard's suites.",
            Some((
                "Show all runs",
                ExploreQuery {
                    scope: Scope::All,
                    ..query.clone()
                },
            )),
        )
    } else {
        (
            "No runs yet",
            "Save a report with iggy-bench's output subcommand, and it shows up here.",
            None,
        )
    };
    html! {
        <div class="explore-empty" role="status">
            <p class="explore-empty-title">{ title }</p>
            <p class="explore-empty-note">{ note }</p>
            if let Some((label, next)) = action {
                <button
                    type="button"
                    class="explore-reset"
                    onclick={show.reform(move |_| (next.clone(), HistoryEntry::Push))}
                >
                    { label }
                </button>
            }
        </div>
    }
}

/// The table head names the cells on a wide screen. On a narrow one, the rows become cards and
/// each cell shows its label.
fn cell(label: &'static str, class: &'static str, content: Html) -> Html {
    html! {
        <span class={classes!("explore-cell", class)}>
            <span class="explore-label visually-hidden">{ label }</span>
            { content }
        </span>
    }
}

fn metric(main: &str, sub: &str, note: Option<&'static str>) -> Html {
    html! {
        <>
            <span class="explore-main" title={note}>{ main }</span>
            <span class="explore-sub">{ sub }</span>
        </>
    }
}

fn no_value(note: Option<&'static str>) -> Html {
    html! { <span class="explore-main" title={note}>{ NO_VALUE }</span> }
}
