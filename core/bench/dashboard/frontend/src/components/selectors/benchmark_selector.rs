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

use crate::{
    components::selectors::benchmark_kind_selector::BenchmarkKindSelector,
    state::benchmark::{BenchmarkAction, use_benchmark},
    state::ui::{UiAction, use_ui},
};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use std::collections::HashSet;
use uuid::Uuid;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BenchmarkSelectorProps {
    pub kind: BenchmarkKind,
}

#[function_component(BenchmarkSelector)]
pub fn benchmark_selector(props: &BenchmarkSelectorProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let filters = ui_state.param_filters.clone();
    let pinned_uuid = ui_state.compare_pin.as_ref().map(|b| b.uuid);
    let selected_kind = benchmark_ctx.state.selected_kind;
    let selected_uuid = benchmark_ctx
        .state
        .selected_benchmark
        .as_ref()
        .map(|b| b.uuid);

    let available_kinds: HashSet<_> = benchmark_ctx
        .state
        .entries
        .keys()
        .filter(|k| matches_tab(&props.kind, k))
        .cloned()
        .collect();

    let empty_vec = Vec::new();
    let all_benchmarks = benchmark_ctx
        .state
        .entries
        .get(&selected_kind)
        .unwrap_or(&empty_vec);
    let current_benchmarks: Vec<_> = all_benchmarks
        .iter()
        .filter(|b| filters.matches(b))
        .collect();
    let hidden_by_filter = all_benchmarks.len() - current_benchmarks.len();

    let on_benchmark_select = {
        let dispatch = benchmark_ctx.dispatch.clone();
        let entries = benchmark_ctx.state.entries.clone();
        let ui_state = ui_state.clone();
        Callback::from(move |pretty_name: String| {
            let selected = entries.get(&selected_kind).and_then(|benchmarks| {
                benchmarks
                    .iter()
                    .find(|b| b.params.pretty_name == pretty_name)
            });
            ui_state.dispatch(UiAction::SetLanding(false));
            dispatch.emit(BenchmarkAction::SelectBenchmark(Box::new(
                selected.cloned(),
            )));
        })
    };

    let on_kind_select = {
        let dispatch = benchmark_ctx.dispatch.clone();
        Callback::from(move |kind: BenchmarkKind| {
            dispatch.emit(BenchmarkAction::SelectBenchmarkKind(kind));
        })
    };

    let on_toggle_pin = {
        let ui_state = ui_state.clone();
        Callback::from(move |benchmark: BenchmarkReportLight| {
            let next = if ui_state.compare_pin.as_ref().map(|b| b.uuid) == Some(benchmark.uuid) {
                None
            } else {
                Some(Box::new(benchmark))
            };
            ui_state.dispatch(UiAction::SetComparePin(next));
        })
    };

    html! {
        <div class="benchmark-select">
            <BenchmarkKindSelector
                selected_kind={selected_kind}
                on_kind_select={on_kind_select}
                available_kinds={available_kinds}
            />

            if current_benchmarks.is_empty() && hidden_by_filter > 0 {
                <div class="no-search-results">
                    <p>{format!("All {hidden_by_filter} benchmark(s) hidden by active filters.")}</p>
                </div>
            }
            <div class="benchmark-list">
                { for current_benchmarks.iter().map(|benchmark| render_item(
                    benchmark,
                    selected_uuid,
                    pinned_uuid,
                    &on_benchmark_select,
                    &on_toggle_pin,
                ))}
            </div>
        </div>
    }
}

fn matches_tab(tab_kind: &BenchmarkKind, candidate: &BenchmarkKind) -> bool {
    use BenchmarkKind::*;
    match tab_kind {
        PinnedProducer | PinnedConsumer | PinnedProducerAndConsumer => matches!(
            candidate,
            PinnedProducer | PinnedConsumer | PinnedProducerAndConsumer
        ),
        BalancedProducer | BalancedConsumerGroup | BalancedProducerAndConsumerGroup => matches!(
            candidate,
            BalancedProducer | BalancedConsumerGroup | BalancedProducerAndConsumerGroup
        ),
        EndToEndProducingConsumer | EndToEndProducingConsumerGroup => matches!(
            candidate,
            EndToEndProducingConsumer | EndToEndProducingConsumerGroup
        ),
    }
}

fn render_item(
    benchmark: &BenchmarkReportLight,
    selected_uuid: Option<Uuid>,
    pinned_uuid: Option<Uuid>,
    on_select: &Callback<String>,
    on_toggle_pin: &Callback<BenchmarkReportLight>,
) -> Html {
    let is_active = Some(benchmark.uuid) == selected_uuid;
    let is_pinned = Some(benchmark.uuid) == pinned_uuid;
    let full_name = benchmark.params.pretty_name.clone();
    let display_name = full_name
        .split('(')
        .next()
        .unwrap_or(&full_name)
        .to_string();

    let on_click = {
        let on_select = on_select.clone();
        let name = full_name.clone();
        Callback::from(move |_| on_select.emit(name.clone()))
    };

    let on_pin_click = {
        let on_toggle_pin = on_toggle_pin.clone();
        let benchmark = benchmark.clone();
        Callback::from(move |e: MouseEvent| {
            e.stop_propagation();
            on_toggle_pin.emit(benchmark.clone());
        })
    };

    let pin_title = if is_pinned {
        "Unpin from compare"
    } else {
        "Pin for compare"
    };

    html! {
        <div
            class={classes!(
                "benchmark-list-item",
                is_active.then_some("active"),
                is_pinned.then_some("pinned"),
            )}
            onclick={on_click}
        >
            <div class="benchmark-list-item-content">
                <div class="benchmark-list-item-title-row">
                    <div class="benchmark-list-item-title">
                        <span class="benchmark-list-item-dot" />
                        {display_name}
                    </div>
                    <button
                        type="button"
                        class={classes!("compare-pin-btn", is_pinned.then_some("active"))}
                        onclick={on_pin_click}
                        title={pin_title}
                        aria-label={pin_title}
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
                             fill={if is_pinned { "currentColor" } else { "none" }}
                             stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M12 17v5" />
                            <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" />
                        </svg>
                    </button>
                </div>

                { render_remark(&benchmark.params.remark) }
            </div>
        </div>
    }
}

fn render_remark(remark: &Option<String>) -> Html {
    let Some(remark) = remark.as_deref() else {
        return html! {};
    };
    if remark.is_empty() {
        return html! {};
    }
    let truncated = if remark.len() > 30 {
        format!("{}..", &remark[..28])
    } else {
        remark.to_string()
    };
    html! {
        <div class="benchmark-list-item-details">
            <div class="benchmark-list-item-subtitle">
                <span class="benchmark-list-item-label">{"Remark:"}</span>
                <span>{truncated}</span>
            </div>
        </div>
    }
}
