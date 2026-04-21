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

use crate::components::selectors::benchmark_selector::BenchmarkSelector;
use crate::components::selectors::gitref_selector::GitrefSelector;
use crate::components::selectors::hardware_selector::HardwareSelector;
use crate::components::selectors::param_filters_panel::ParamFiltersPanel;
use crate::components::selectors::recent_benchmarks_selector::RecentBenchmarksSelector;
use crate::state::gitref::use_gitref;
use crate::state::ui::{KindGroup, SidebarSort, UiAction, ViewMode, use_ui};
use web_sys::HtmlInputElement;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SidebarProps {
    pub on_gitref_select: Callback<String>,
    pub on_hardware_select: Callback<String>,
}

#[function_component(Sidebar)]
pub fn sidebar(props: &SidebarProps) -> Html {
    let gitref_ctx = use_gitref();
    let ui = use_ui();
    let is_recent_view = matches!(ui.view_mode, ViewMode::RecentBenchmarks);
    let active_kind_filter = ui.sidebar_kind_filter.clone();
    let current_sort = ui.sidebar_sort;
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

    let on_scope_change = |mode: ViewMode| {
        let ui = ui.clone();
        Callback::from(move |_: MouseEvent| ui.dispatch(UiAction::SetViewMode(mode.clone())))
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
                        placeholder="Search benchmarks, gitrefs, hardware..."
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

                <div class="sidebar-scope" role="tablist">
                    <button
                        type="button"
                        role="tab"
                        aria-selected={(!is_recent_view).to_string()}
                        class={classes!("sidebar-scope-btn", (!is_recent_view).then_some("active"))}
                        onclick={on_scope_change(ViewMode::SingleGitref)}
                    >
                        {"Version"}
                    </button>
                    <button
                        type="button"
                        role="tab"
                        aria-selected={is_recent_view.to_string()}
                        class={classes!("sidebar-scope-btn", is_recent_view.then_some("active"))}
                        onclick={on_scope_change(ViewMode::RecentBenchmarks)}
                    >
                        {"Recent"}
                    </button>
                </div>

                if !is_recent_view {
                    <HardwareSelector on_hardware_select={props.on_hardware_select.clone()} />
                    <GitrefSelector
                        gitrefs={gitref_ctx.state.gitrefs.clone()}
                        selected_gitref={gitref_ctx.state.selected_gitref.clone().unwrap_or_default()}
                        on_gitref_select={props.on_gitref_select.clone()}
                    />
                }

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
                if is_recent_view {
                    <RecentBenchmarksSelector limit={10000} />
                } else {
                    <BenchmarkSelector />
                }
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
