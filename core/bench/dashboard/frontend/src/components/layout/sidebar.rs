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

use crate::api;
use crate::components::selectors::benchmarks_list::BenchmarksList;
use crate::components::selectors::param_filters_panel::ParamFiltersPanel;
use crate::router::AppRoute;
use crate::state::benchmark::{BenchmarkAction, recency_cmp, use_benchmark};
use crate::state::ui::{KindGroup, SidebarSort, UiAction, use_ui};
use bench_dashboard_shared::BenchmarkReportLight;
use gloo::console::log;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::rc::Rc;
use web_sys::HtmlInputElement;
use web_sys::HtmlSelectElement;
use yew::platform::spawn_local;
use yew::prelude::*;
use yew_router::prelude::{use_navigator, use_route};

const RECENT_LIMIT: u32 = 10_000;

#[derive(Properties, PartialEq)]
pub struct SidebarProps;

#[function_component(Sidebar)]
pub fn sidebar(_props: &SidebarProps) -> Html {
    let ui = use_ui();
    let benchmark_ctx = use_benchmark();
    let navigator = use_navigator();
    let route = use_route::<AppRoute>();

    let benchmarks = use_state(Vec::<BenchmarkReportLight>::new);
    let is_loading = use_state(|| true);

    {
        let benchmarks_handle = benchmarks.clone();
        let is_loading_handle = is_loading.clone();
        let dispatch = benchmark_ctx.dispatch.clone();
        let navigator = navigator.clone();
        let url_has_benchmark = matches!(
            route,
            Some(AppRoute::Benchmark { .. }) | Some(AppRoute::Compare { .. })
        );
        let cancelled = Rc::new(Cell::new(false));
        let cancelled_async = cancelled.clone();
        use_effect_with((), move |_| {
            spawn_local(async move {
                match api::fetch_recent_benchmarks(Some(RECENT_LIMIT)).await {
                    Ok(mut data) => {
                        data.sort_by(|left, right| {
                            recency_cmp(right, left).then_with(|| right.uuid.cmp(&left.uuid))
                        });
                        if cancelled_async.get() {
                            return;
                        }
                        if !url_has_benchmark && let Some(newest) = data.first().cloned() {
                            if let Some(nav) = navigator.as_ref() {
                                nav.push(&AppRoute::Benchmark {
                                    uuid: newest.uuid.to_string(),
                                });
                            }
                            dispatch.emit(BenchmarkAction::SelectBenchmark(Box::new(Some(newest))));
                        }
                        benchmarks_handle.set(data);
                    }
                    Err(error) => log!(format!("Sidebar: fetch_recent_benchmarks failed: {error}")),
                }
                if !cancelled_async.get() {
                    is_loading_handle.set(false);
                }
            });
            move || cancelled.set(true)
        });
    }

    let current_search = ui.sidebar_search.clone();

    let on_search = {
        let ui = ui.clone();
        Callback::from(move |event: InputEvent| {
            let input: HtmlInputElement = event.target_unchecked_into();
            ui.dispatch(UiAction::SetSidebarSearch(input.value()));
        })
    };

    let on_clear_search = {
        let ui = ui.clone();
        Callback::from(move |_: MouseEvent| {
            ui.dispatch(UiAction::SetSidebarSearch(String::new()));
        })
    };

    let on_kind_toggle = {
        let ui = ui.clone();
        Callback::from(move |group: KindGroup| ui.dispatch(UiAction::ToggleKindFilter(group)))
    };

    let on_sort_change = {
        let ui = ui.clone();
        Callback::from(move |event: Event| {
            let input: HtmlInputElement = event.target_unchecked_into();
            if let Ok(sort) = match input.value().as_str() {
                "MostRecent" => Ok(SidebarSort::MostRecent),
                "PeakThroughput" => Ok(SidebarSort::PeakThroughput),
                "LowestP99" => Ok(SidebarSort::LowestP99),
                "Name" => Ok(SidebarSort::Name),
                _ => Err(()),
            } {
                ui.dispatch(UiAction::SetSidebarSort(sort));
            }
        })
    };

    let on_hardware_change = {
        let ui = ui.clone();
        Callback::from(move |event: Event| {
            let input: HtmlSelectElement = event.target_unchecked_into();
            let value = input.value();
            let next = if value.is_empty() { None } else { Some(value) };
            ui.dispatch(UiAction::SetHardwareFilter(next));
        })
    };

    let on_gitref_change = {
        let ui = ui.clone();
        Callback::from(move |event: Event| {
            let input: HtmlSelectElement = event.target_unchecked_into();
            let value = input.value();
            let next = if value.is_empty() { None } else { Some(value) };
            ui.dispatch(UiAction::SetGitrefFilter(next));
        })
    };

    let hardware_options = collect_hardware(&benchmarks);
    let gitref_options = collect_gitrefs(&benchmarks, ui.hardware_filter.as_deref());
    let active_kind_filter = ui.sidebar_kind_filter.clone();
    let current_sort = ui.sidebar_sort;
    let current_hardware = ui.hardware_filter.clone().unwrap_or_default();
    let current_gitref = ui.gitref_filter.clone().unwrap_or_default();

    html! {
        <aside class="sidebar">
            <div class="sidebar-fixed-header">
                <div class="sidebar-search">
                    <svg class="sidebar-search-icon" xmlns="http://www.w3.org/2000/svg"
                         width="16" height="16" viewBox="0 0 24 24" fill="none"
                         stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="11" cy="11" r="7" />
                        <line x1="21" y1="21" x2="16.65" y2="16.65" />
                    </svg>
                    <input
                        type="search"
                        class="sidebar-search-input"
                        placeholder="Search benchmarks..."
                        value={current_search.clone()}
                        oninput={on_search}
                    />
                    if !current_search.is_empty() {
                        <button
                            type="button"
                            class="sidebar-search-clear"
                            onclick={on_clear_search}
                            aria-label="Clear search"
                        >{"×"}</button>
                    } else {
                        <kbd class="sidebar-search-hint" aria-hidden="true">{"/"}</kbd>
                    }
                </div>

                <div class="sidebar-facet-row">
                    <label class="sidebar-facet">
                        <span class="sidebar-facet-label">{"Hardware"}</span>
                        <select class="sidebar-facet-select" onchange={on_hardware_change}>
                            <option value="" selected={current_hardware.is_empty()}>
                                {"All"}
                            </option>
                            { for hardware_options.iter().map(|option| html! {
                                <option
                                    value={option.clone()}
                                    selected={option.as_str() == current_hardware}
                                >
                                    {option.clone()}
                                </option>
                            })}
                        </select>
                    </label>

                    <label class="sidebar-facet">
                        <span class="sidebar-facet-label">{"Version"}</span>
                        <select class="sidebar-facet-select" onchange={on_gitref_change}>
                            <option value="" selected={current_gitref.is_empty()}>
                                {"All"}
                            </option>
                            { for gitref_options.iter().map(|option| html! {
                                <option
                                    value={option.clone()}
                                    selected={option.as_str() == current_gitref}
                                >
                                    {option.clone()}
                                </option>
                            })}
                        </select>
                    </label>
                </div>

                <div class="sidebar-kind-chips">
                    { for KindGroup::all().iter().map(|group| {
                        let is_active = active_kind_filter.contains(group);
                        let group_copy = *group;
                        let on_click = {
                            let on_kind_toggle = on_kind_toggle.clone();
                            Callback::from(move |_: MouseEvent| on_kind_toggle.emit(group_copy))
                        };
                        html! {
                            <button
                                type="button"
                                class={classes!("sidebar-chip", is_active.then_some("active"))}
                                onclick={on_click}
                                aria-pressed={is_active.to_string()}
                            >
                                {group.label()}
                            </button>
                        }
                    })}
                </div>

                <label class="sidebar-sort">
                    <svg class="sidebar-sort-icon" xmlns="http://www.w3.org/2000/svg"
                         width="14" height="14" viewBox="0 0 24 24" fill="none"
                         stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M3 6h18" />
                        <path d="M7 12h10" />
                        <path d="M10 18h4" />
                    </svg>
                    <span class="sidebar-sort-label">{"Sort"}</span>
                    <select class="sidebar-sort-select" onchange={on_sort_change}>
                        { for SidebarSort::all().iter().map(|sort| {
                            let value = format!("{sort:?}");
                            html! {
                                <option value={value.clone()} selected={*sort == current_sort}>
                                    { sort.label() }
                                </option>
                            }
                        })}
                    </select>
                    <svg class="sidebar-sort-chevron" xmlns="http://www.w3.org/2000/svg"
                         width="14" height="14" viewBox="0 0 24 24" fill="none"
                         stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="6 9 12 15 18 9" />
                    </svg>
                </label>
                <ParamFiltersPanel />
                { render_compare_hint(&ui) }
            </div>

            <div class="sidebar-scrollable-content">
                <BenchmarksList
                    benchmarks={(*benchmarks).clone()}
                    is_loading={*is_loading}
                />
            </div>
        </aside>
    }
}

fn render_compare_hint(ui: &yew::UseReducerHandle<crate::state::ui::UiState>) -> Html {
    let pinned_name = ui
        .compare_pin
        .as_ref()
        .map(|pin| short_name(&pin.params.pretty_name));

    let on_clear = {
        let ui = ui.clone();
        Callback::from(move |_: MouseEvent| ui.dispatch(UiAction::SetComparePin(Box::new(None))))
    };

    match pinned_name {
        Some(name) => html! {
            <div class="compare-hint pinned">
                <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
                     fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M12 17v5" />
                    <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" />
                </svg>
                <span class="compare-hint-body">
                    <strong>{"Pinned:"}</strong>
                    <span class="compare-hint-name" title={name.clone()}>{name}</span>
                    <span class="compare-hint-cta">{"Click any benchmark to compare"}</span>
                </span>
                <button type="button" class="compare-hint-clear" onclick={on_clear}
                        title="Clear pin">{"×"}</button>
            </div>
        },
        None => html! {
            <div class="compare-hint">
                <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
                     fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M12 17v5" />
                    <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" />
                </svg>
                <span class="compare-hint-body">
                    {"Tip: pin any benchmark to compare side-by-side"}
                </span>
            </div>
        },
    }
}

fn short_name(full: &str) -> String {
    full.split('(').next().unwrap_or(full).trim().to_string()
}

fn collect_hardware(benchmarks: &[BenchmarkReportLight]) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for benchmark in benchmarks {
        if let Some(id) = benchmark.hardware.identifier.as_deref()
            && !id.is_empty()
        {
            set.insert(id.to_string());
        }
    }
    set.into_iter().collect()
}

fn collect_gitrefs(
    benchmarks: &[BenchmarkReportLight],
    hardware_filter: Option<&str>,
) -> Vec<String> {
    let mut newest: std::collections::HashMap<String, &BenchmarkReportLight> =
        std::collections::HashMap::new();
    for benchmark in benchmarks {
        if let Some(expected) = hardware_filter
            && benchmark.hardware.identifier.as_deref() != Some(expected)
        {
            continue;
        }
        let Some(gitref) = benchmark.params.gitref.as_deref() else {
            continue;
        };
        if gitref.is_empty() {
            continue;
        }
        newest
            .entry(gitref.to_string())
            .and_modify(|existing| {
                if recency_cmp(benchmark, existing).is_gt() {
                    *existing = benchmark;
                }
            })
            .or_insert(benchmark);
    }

    let mut ordered: Vec<(&BenchmarkReportLight, String)> = newest
        .into_iter()
        .map(|(gitref, benchmark)| (benchmark, gitref))
        .collect();
    ordered.sort_by(|left, right| recency_cmp(right.0, left.0));
    ordered.into_iter().map(|(_, gitref)| gitref).collect()
}
