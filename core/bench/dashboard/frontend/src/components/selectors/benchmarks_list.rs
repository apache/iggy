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

use crate::components::selectors::dense_benchmark_row::DenseBenchmarkRow;
use crate::format::nan_safe_cmp;
use crate::router::AppRoute;
use crate::state::benchmark::{BenchmarkAction, recency_cmp, use_benchmark};
use crate::state::ui::{KindGroup, SidebarSort, UiAction, use_ui};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use std::collections::HashSet;
use yew::prelude::*;
use yew_router::prelude::{Navigator, use_navigator};

#[derive(Properties, PartialEq)]
pub struct BenchmarksListProps {
    pub benchmarks: Vec<BenchmarkReportLight>,
    pub is_loading: bool,
}

#[function_component(BenchmarksList)]
pub fn benchmarks_list(props: &BenchmarksListProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let navigator = use_navigator();

    let pinned_uuid = ui_state.compare_pin.as_ref().map(|pin| pin.uuid);
    let selected_uuid = benchmark_ctx
        .state
        .selected_benchmark
        .as_ref()
        .map(|selected| selected.uuid);

    let param_filters = ui_state.param_filters.clone();
    let search = ui_state.sidebar_search.to_lowercase();
    let kind_filter = ui_state.sidebar_kind_filter.clone();
    let hardware_filter = ui_state.hardware_filter.clone();
    let gitref_filter = ui_state.gitref_filter.clone();
    let sort = ui_state.sidebar_sort;

    let on_select = {
        let dispatch = benchmark_ctx.dispatch.clone();
        let navigator = navigator.clone();
        Callback::from(move |benchmark: BenchmarkReportLight| {
            if let Some(nav) = navigator.as_ref() {
                nav.push(&AppRoute::Benchmark {
                    uuid: benchmark.uuid.to_string(),
                });
            }
            dispatch.emit(BenchmarkAction::SelectBenchmark(Box::new(Some(benchmark))));
        })
    };

    let on_toggle_pin = {
        let ui_state = ui_state.clone();
        let navigator = navigator.clone();
        let benchmark_ctx = benchmark_ctx.clone();
        Callback::from(move |benchmark: BenchmarkReportLight| {
            let clicked_uuid = benchmark.uuid.to_string();
            let selected_uuid = benchmark_ctx
                .state
                .selected_benchmark
                .as_ref()
                .map(|selected| selected.uuid.to_string());
            let pinned_uuid = ui_state
                .compare_pin
                .as_ref()
                .map(|pin| pin.uuid.to_string());

            if let Some(pinned) = pinned_uuid.as_ref()
                && pinned == &clicked_uuid
                && let Some(selected) = selected_uuid.as_ref()
            {
                push_route(
                    &navigator,
                    AppRoute::Benchmark {
                        uuid: selected.clone(),
                    },
                );
                return;
            }

            if let Some(selected) = selected_uuid
                && selected != clicked_uuid
            {
                push_route(
                    &navigator,
                    AppRoute::Compare {
                        left: selected,
                        right: clicked_uuid,
                    },
                );
                return;
            }

            let same_pin =
                ui_state.compare_pin.as_ref().map(|pin| pin.uuid) == Some(benchmark.uuid);
            let next = if same_pin { None } else { Some(benchmark) };
            ui_state.dispatch(UiAction::SetComparePin(Box::new(next)));
        })
    };

    if props.is_loading {
        return render_skeleton();
    }

    let visible: Vec<BenchmarkReportLight> = props
        .benchmarks
        .iter()
        .filter(|benchmark| param_filters.matches(benchmark))
        .filter(|benchmark| kind_filter_matches(&kind_filter, benchmark.params.benchmark_kind))
        .filter(|benchmark| hardware_matches(hardware_filter.as_deref(), benchmark))
        .filter(|benchmark| gitref_matches(gitref_filter.as_deref(), benchmark))
        .filter(|benchmark| search_matches(&search, benchmark))
        .cloned()
        .collect();

    if visible.is_empty() {
        return html! {
            <div class="dense-list-empty">
                <p>{"No benchmarks match the current filters."}</p>
            </div>
        };
    }

    let sorted = sort_benchmarks(visible, sort);

    html! {
        <div class="dense-list">
            { for sorted.iter().map(|benchmark| html! {
                <DenseBenchmarkRow
                    benchmark={benchmark.clone()}
                    selected_uuid={selected_uuid}
                    pinned_uuid={pinned_uuid}
                    on_select={on_select.clone()}
                    on_toggle_pin={on_toggle_pin.clone()}
                    show_timestamp={true}
                />
            })}
        </div>
    }
}

fn push_route(navigator: &Option<Navigator>, route: AppRoute) {
    if let Some(nav) = navigator.as_ref() {
        nav.push(&route);
    }
}

fn render_skeleton() -> Html {
    html! {
        <div class="dense-list">
            { for (0..5).map(|index| html! {
                <div class="dense-row-skeleton" style={format!("--stagger: {index}")}>
                    <span class="skeleton-dot" />
                    <div class="skeleton-body">
                        <span class="skeleton-line title" />
                        <span class="skeleton-line meta" />
                    </div>
                </div>
            })}
        </div>
    }
}

fn kind_filter_matches(filter: &HashSet<KindGroup>, kind: BenchmarkKind) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().any(|group| group.matches(kind))
}

fn hardware_matches(filter: Option<&str>, benchmark: &BenchmarkReportLight) -> bool {
    match filter {
        Some(expected) => benchmark.hardware.identifier.as_deref() == Some(expected),
        None => true,
    }
}

fn gitref_matches(filter: Option<&str>, benchmark: &BenchmarkReportLight) -> bool {
    match filter {
        Some(expected) => benchmark.params.gitref.as_deref() == Some(expected),
        None => true,
    }
}

fn search_matches(query: &str, benchmark: &BenchmarkReportLight) -> bool {
    if query.is_empty() {
        return true;
    }
    let kind_label = benchmark.params.benchmark_kind.to_string().to_lowercase();
    if kind_label.contains(query) {
        return true;
    }
    if benchmark.params.pretty_name.to_lowercase().contains(query) {
        return true;
    }
    if let Some(remark) = benchmark.params.remark.as_deref()
        && remark.to_lowercase().contains(query)
    {
        return true;
    }
    if let Some(gitref) = benchmark.params.gitref.as_deref()
        && gitref.to_lowercase().contains(query)
    {
        return true;
    }
    if let Some(hardware) = benchmark.hardware.identifier.as_deref()
        && hardware.to_lowercase().contains(query)
    {
        return true;
    }
    if benchmark.timestamp.to_lowercase().contains(query) {
        return true;
    }
    false
}

fn sort_benchmarks(
    mut benchmarks: Vec<BenchmarkReportLight>,
    sort: SidebarSort,
) -> Vec<BenchmarkReportLight> {
    benchmarks.sort_by(|left, right| match sort {
        SidebarSort::MostRecent => recency_cmp(right, left),
        SidebarSort::PeakThroughput => nan_safe_cmp(throughput(right), throughput(left)),
        SidebarSort::LowestP99 => nan_safe_cmp(p99(left), p99(right)),
        SidebarSort::Name => left.params.pretty_name.cmp(&right.params.pretty_name),
    });
    benchmarks
}

fn throughput(benchmark: &BenchmarkReportLight) -> f64 {
    benchmark
        .group_metrics
        .first()
        .map(|metrics| metrics.summary.total_throughput_megabytes_per_second)
        .unwrap_or(0.0)
}

fn p99(benchmark: &BenchmarkReportLight) -> f64 {
    benchmark
        .group_metrics
        .first()
        .map(|metrics| metrics.summary.average_p99_latency_ms)
        .unwrap_or(f64::INFINITY)
}
