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

use super::logo::Logo;
use crate::components::selectors::benchmark_search_box::BenchmarkSearchBox;
use crate::components::selectors::benchmark_selector::BenchmarkSelector;
use crate::components::selectors::gitref_selector::GitrefSelector;
use crate::components::selectors::hardware_selector::HardwareSelector;
use crate::components::selectors::recent_benchmarks_selector::RecentBenchmarksSelector;
use crate::components::selectors::view_mode_selector::ViewModeSelector;
use crate::state::benchmark::{BenchmarkAction, use_benchmark};
use crate::state::gitref::use_gitref;
use crate::state::ui::{ViewMode, use_ui};
use bench_report::benchmark_kind::BenchmarkKind;
use yew::prelude::*;

#[derive(Clone, PartialEq)]
pub enum BenchmarkTab {
    Pinned,
    Balanced,
    EndToEnd,
}

#[derive(Properties, PartialEq)]
pub struct SidebarProps {
    pub on_gitref_select: Callback<String>,
    pub on_hardware_select: Callback<String>,
}

#[function_component(Sidebar)]
pub fn sidebar(props: &SidebarProps) -> Html {
    let gitref_ctx = use_gitref();
    let ui_state = use_ui();
    let benchmark_ctx = use_benchmark();
    let search_query = use_state(String::new);

    let is_trend_view = matches!(ui_state.view_mode, ViewMode::GitrefTrend);

    let is_recent_view = matches!(ui_state.view_mode, ViewMode::RecentBenchmarks);

    let active_tab = match benchmark_ctx.state.selected_kind {
        BenchmarkKind::PinnedProducer
        | BenchmarkKind::PinnedConsumer
        | BenchmarkKind::PinnedProducerAndConsumer => BenchmarkTab::Pinned,
        BenchmarkKind::BalancedProducer
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducerAndConsumerGroup => BenchmarkTab::Balanced,
        _ => BenchmarkTab::EndToEnd,
    };

    let is_pinned = |b: &BenchmarkKind| {
        matches!(
            b,
            BenchmarkKind::PinnedProducer
                | BenchmarkKind::PinnedConsumer
                | BenchmarkKind::PinnedProducerAndConsumer
        )
    };
    let is_balanced = |b: &BenchmarkKind| {
        matches!(
            b,
            BenchmarkKind::BalancedProducer
                | BenchmarkKind::BalancedConsumerGroup
                | BenchmarkKind::BalancedProducerAndConsumerGroup
        )
    };
    let is_end_to_end = |b: &BenchmarkKind| {
        matches!(
            b,
            BenchmarkKind::EndToEndProducingConsumer
                | BenchmarkKind::EndToEndProducingConsumerGroup
        )
    };

    let has_benchmarks = |f: fn(&BenchmarkKind) -> bool| {
        benchmark_ctx
            .state
            .entries
            .values()
            .any(|benchmarks| benchmarks.iter().any(|b| f(&b.params.benchmark_kind)))
    };

    let count_benchmarks = |f: fn(&BenchmarkKind) -> bool| {
        benchmark_ctx
            .state
            .entries
            .values()
            .map(|benchmarks| {
                benchmarks
                    .iter()
                    .filter(|b| f(&b.params.benchmark_kind))
                    .count()
            })
            .sum::<usize>()
    };

    let has_pinned_benchmarks = has_benchmarks(is_pinned);
    let has_balanced_benchmarks = has_benchmarks(is_balanced);
    let has_end_to_end_benchmarks = has_benchmarks(is_end_to_end);

    let pinned_benchmark_count = count_benchmarks(is_pinned);
    let balanced_benchmark_count = count_benchmarks(is_balanced);
    let end_to_end_benchmark_count = count_benchmarks(is_end_to_end);

    fn get_default_kind_for_tab(tab: &BenchmarkTab) -> BenchmarkKind {
        match tab {
            BenchmarkTab::Pinned => BenchmarkKind::PinnedProducer,
            BenchmarkTab::Balanced => BenchmarkKind::BalancedProducer,
            BenchmarkTab::EndToEnd => BenchmarkKind::EndToEndProducingConsumer,
        }
    }

    let on_tab_click = {
        let benchmark_ctx = benchmark_ctx.clone();
        Callback::from(move |tab: BenchmarkTab| {
            let kind = get_default_kind_for_tab(&tab);
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmarkKind(kind));
        })
    };

    html! {
        <div class="sidebar">
            <div class="sidebar-fixed-header">
                <Logo />
                <ViewModeSelector />

                if !is_recent_view {
                    <HardwareSelector
                        on_hardware_select={props.on_hardware_select.clone()}
                    />
                }

                if !is_trend_view && !is_recent_view {
                    <GitrefSelector
                        gitrefs={gitref_ctx.state.gitrefs.clone()}
                        selected_gitref={gitref_ctx.state.selected_gitref.clone().unwrap_or_default()}
                        on_gitref_select={props.on_gitref_select.clone()}
                    />
                }

                <h3 class="benchmarks-heading">{"Benchmarks"}</h3>
                if is_recent_view {
                    <BenchmarkSearchBox search_query={search_query.clone()} />
                }
            </div>

            <div class="sidebar-scrollable-content">
                if is_recent_view {
                    <RecentBenchmarksSelector limit={10000} search_query={(*search_query).clone()} />
                } else {
                    <div class="sidebar-tabs">
                    <div class="tab-list">
                        <button
                            class={classes!(
                                "tab-button",
                                (active_tab == BenchmarkTab::Pinned).then_some("active"),
                                (!has_pinned_benchmarks).then_some("inactive")
                            )}
                            disabled={!has_pinned_benchmarks}
                            onclick={
                                let on_tab_click = on_tab_click.clone();
                                Callback::from(move |_| on_tab_click.emit(BenchmarkTab::Pinned))
                            }
                        >
                            { "Pinned (" }{pinned_benchmark_count}{")" }
                        </button>
                        <button
                            class={classes!(
                                "tab-button",
                                (active_tab == BenchmarkTab::Balanced).then_some("active"),
                                (!has_balanced_benchmarks).then_some("inactive")
                            )}
                            disabled={!has_balanced_benchmarks}
                            onclick={
                                let on_tab_click = on_tab_click.clone();
                                Callback::from(move |_| on_tab_click.emit(BenchmarkTab::Balanced))
                            }
                        >
                            { "Balanced (" }{balanced_benchmark_count}{")" }
                        </button>
                        <button
                            class={classes!(
                                "tab-button",
                                (active_tab == BenchmarkTab::EndToEnd).then_some("active"),
                                (!has_end_to_end_benchmarks).then_some("inactive")
                            )}
                            disabled={!has_end_to_end_benchmarks}
                            onclick={
                                let on_tab_click = on_tab_click.clone();
                                Callback::from(move |_| on_tab_click.emit(BenchmarkTab::EndToEnd))
                            }
                        >
                            { "End to End (" }{end_to_end_benchmark_count}{")" }
                        </button>
                    </div>

                    <div class={classes!(
                        "tab-content",
                        (active_tab == BenchmarkTab::Pinned).then_some("active")
                    )}>
                        <BenchmarkSelector kind={get_default_kind_for_tab(&BenchmarkTab::Pinned)} />
                    </div>
                    <div class={classes!(
                        "tab-content",
                        (active_tab == BenchmarkTab::Balanced).then_some("active")
                    )}>
                        <BenchmarkSelector kind={get_default_kind_for_tab(&BenchmarkTab::Balanced)} />
                    </div>
                    <div class={classes!(
                        "tab-content",
                        (active_tab == BenchmarkTab::EndToEnd).then_some("active")
                    )}>
                        <BenchmarkSelector kind={get_default_kind_for_tab(&BenchmarkTab::EndToEnd)} />
                    </div>
                </div>
            }
            </div>
        </div>
    }
}
