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
use crate::state::benchmark::{BenchmarkAction, use_benchmark};
use crate::state::ui::{KindGroup, SidebarSort, UiAction, use_ui};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use chrono::DateTime;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use yew::prelude::*;
use yew_router::prelude::use_navigator;

#[derive(Properties, PartialEq, Default)]
pub struct BenchmarkSelectorProps;

#[function_component(BenchmarkSelector)]
pub fn benchmark_selector(_props: &BenchmarkSelectorProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let navigator = use_navigator();

    let pinned_uuid = ui_state.compare_pin.as_ref().map(|pin| pin.uuid);
    let selected_uuid = benchmark_ctx
        .state
        .selected_benchmark
        .as_ref()
        .map(|selected| selected.uuid);

    let filters = ui_state.param_filters.clone();
    let search = ui_state.sidebar_search.to_lowercase();
    let kind_filter = ui_state.sidebar_kind_filter.clone();
    let sort = ui_state.sidebar_sort;

    let visible: Vec<BenchmarkReportLight> = benchmark_ctx
        .state
        .entries
        .values()
        .flatten()
        .filter(|benchmark| filters.matches(benchmark))
        .filter(|benchmark| kind_filter_matches(&kind_filter, benchmark.params.benchmark_kind))
        .filter(|benchmark| search_matches(&search, benchmark))
        .cloned()
        .collect();
    let hidden_by_filter = total_benchmarks(&benchmark_ctx.state.entries) - visible.len();
    let sorted = sort_benchmarks(visible, sort);

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
                navigate(
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
                navigate(
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

    if sorted.is_empty() {
        return html! {
            <div class="dense-list-empty">
                if hidden_by_filter > 0 {
                    <p>{format!("{hidden_by_filter} benchmark(s) hidden. Adjust filters or search.")}</p>
                } else {
                    <p>{"No benchmarks for this gitref yet."}</p>
                }
            </div>
        };
    }

    html! {
        <div class="dense-list">
            { for sorted.iter().map(|benchmark| html! {
                <DenseBenchmarkRow
                    benchmark={benchmark.clone()}
                    selected_uuid={selected_uuid}
                    pinned_uuid={pinned_uuid}
                    on_select={on_select.clone()}
                    on_toggle_pin={on_toggle_pin.clone()}
                    show_timestamp={false}
                />
            })}
        </div>
    }
}

fn navigate(navigator: &Option<yew_router::prelude::Navigator>, route: AppRoute) {
    if let Some(nav) = navigator.as_ref() {
        nav.push(&route);
    }
}

fn kind_filter_matches(filter: &HashSet<KindGroup>, kind: BenchmarkKind) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().any(|group| group.matches(kind))
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
    false
}

fn total_benchmarks(entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>) -> usize {
    entries.values().map(|values| values.len()).sum()
}

fn sort_benchmarks(
    mut benchmarks: Vec<BenchmarkReportLight>,
    sort: SidebarSort,
) -> Vec<BenchmarkReportLight> {
    benchmarks.sort_by(|left, right| match sort {
        SidebarSort::MostRecent => compare_timestamps(&right.timestamp, &left.timestamp),
        SidebarSort::PeakThroughput => nan_safe_cmp(throughput(right), throughput(left)),
        SidebarSort::LowestP99 => nan_safe_cmp(p99(left), p99(right)),
        SidebarSort::Name => left.params.pretty_name.cmp(&right.params.pretty_name),
    });
    benchmarks
}

fn compare_timestamps(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left_time), Ok(right_time)) => left_time.cmp(&right_time),
        _ => Ordering::Equal,
    }
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
