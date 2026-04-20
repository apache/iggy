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
    api,
    state::benchmark::{BenchmarkAction, use_benchmark},
    state::ui::{UiAction, use_ui},
};
use bench_dashboard_shared::BenchmarkReportLight;
use chrono::{DateTime, Utc};
use gloo::console::log;
use yew::platform::spawn_local;
use yew::prelude::*;

/// Format a timestamp string as a human-readable relative time (e.g., "2 hours ago")
fn format_relative_time(timestamp_str: &str) -> String {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str) {
        let now = Utc::now();
        let duration = now.signed_duration_since(timestamp.with_timezone(&Utc));

        if duration.num_seconds() < 60 {
            format!("{} seconds ago", duration.num_seconds())
        } else if duration.num_minutes() < 60 {
            format!("{} minutes ago", duration.num_minutes())
        } else if duration.num_hours() < 24 {
            format!("{} hours ago", duration.num_hours())
        } else if duration.num_days() < 30 {
            format!("{} days ago", duration.num_days())
        } else {
            timestamp.format("%Y-%m-%d").to_string()
        }
    } else {
        "Unknown time".to_string()
    }
}

#[derive(Properties, PartialEq)]
pub struct RecentBenchmarksSelectorProps {
    pub limit: u32,
    pub search_query: String,
}

#[function_component(RecentBenchmarksSelector)]
pub fn recent_benchmarks_selector(props: &RecentBenchmarksSelectorProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let filters = ui_state.param_filters.clone();
    let pinned_uuid = ui_state.compare_pin.as_ref().map(|b| b.uuid);

    let recent_benchmarks = use_state(Vec::<BenchmarkReportLight>::new);
    let is_loading = use_state(|| true);

    {
        let recent_benchmarks = recent_benchmarks.clone();
        let is_loading = is_loading.clone();
        let limit = props.limit;
        let benchmark_dispatch = benchmark_ctx.dispatch.clone();
        let ui_dispatch_handle = ui_state.clone();

        use_effect_with((), move |_| {
            spawn_local(async move {
                match api::fetch_recent_benchmarks(Some(limit)).await {
                    Ok(mut data) => {
                        data.sort_by(|left, right| {
                            let parse_left = DateTime::parse_from_rfc3339(&left.timestamp);
                            let parse_right = DateTime::parse_from_rfc3339(&right.timestamp);
                            match (parse_left, parse_right) {
                                (Ok(time_left), Ok(time_right)) => time_right.cmp(&time_left),
                                _ => std::cmp::Ordering::Equal,
                            }
                        });
                        if let Some(most_recent) = data.first().cloned() {
                            ui_dispatch_handle.dispatch(UiAction::SetLanding(false));
                            benchmark_dispatch.emit(BenchmarkAction::SelectBenchmark(Box::new(
                                Some(most_recent),
                            )));
                        }
                        recent_benchmarks.set(data);
                    }
                    Err(error) => {
                        log!(format!("Error fetching recent benchmarks: {}", error));
                    }
                }
                is_loading.set(false);
            });
        });
    }

    let on_benchmark_select = {
        let benchmark_ctx = benchmark_ctx.clone();
        let ui_state = ui_state.clone();
        Callback::from(move |benchmark: BenchmarkReportLight| {
            ui_state.dispatch(UiAction::SetLanding(false));
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmark(Box::new(Some(benchmark))));
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

    let filtered_benchmarks = (*recent_benchmarks)
        .iter()
        .filter(|benchmark| filters.matches(benchmark))
        .filter(|benchmark| {
            if props.search_query.is_empty() {
                return true;
            }

            let query = props.search_query.to_lowercase();

            if benchmark
                .params
                .benchmark_kind
                .to_string()
                .to_lowercase()
                .contains(&query)
            {
                return true;
            }

            if let Some(identifier) = &benchmark.hardware.identifier
                && identifier.to_lowercase().contains(&query)
            {
                return true;
            }

            if let Some(gitref) = &benchmark.params.gitref
                && gitref.to_lowercase().contains(&query)
            {
                return true;
            }

            if let Some(remark) = &benchmark.params.remark
                && remark.to_lowercase().contains(&query)
            {
                return true;
            }

            if benchmark.timestamp.to_lowercase().contains(&query) {
                return true;
            }

            false
        })
        .cloned()
        .collect::<Vec<BenchmarkReportLight>>();

    html! {
        <div class="sidebar-tabs">
            <div class="benchmark-list-container close-gap">
                <div class="benchmark-list-wrapper">
                if *is_loading {
                    <p class="loading-message">{"Loading recent benchmarks..."}</p>
                } else if filtered_benchmarks.is_empty() {
                    <div class="no-search-results">
                        { if props.search_query.is_empty() && filters.is_any_active() {
                            html! { <p>{"No benchmarks match the active filters."}</p> }
                        } else {
                            html! { <p>{format!("No benchmarks found matching \"{}\"", props.search_query)}</p> }
                        }}
                    </div>
                } else {
                    <ul class="benchmark-list">
                        {filtered_benchmarks.into_iter().map(|benchmark| {
                            let on_select = {
                                let on_benchmark_select = on_benchmark_select.clone();
                                let benchmark_clone = benchmark.clone();
                                Callback::from(move |_| {
                                    on_benchmark_select.emit(benchmark_clone.clone());
                                })
                            };
                            let on_pin_click = {
                                let on_toggle_pin = on_toggle_pin.clone();
                                let benchmark_clone = benchmark.clone();
                                Callback::from(move |e: MouseEvent| {
                                    e.stop_propagation();
                                    on_toggle_pin.emit(benchmark_clone.clone());
                                })
                            };
                            let timestamp_display = format_relative_time(&benchmark.timestamp);

                            let is_selected = benchmark_ctx.state.selected_benchmark.as_ref()
                                .map(|selected| selected.uuid == benchmark.uuid)
                                .unwrap_or(false);
                            let is_pinned = Some(benchmark.uuid) == pinned_uuid;
                            let pin_title = if is_pinned { "Unpin from compare" } else { "Pin for compare" };

                            html! {
                                <li class={classes!(
                                    "benchmark-list-item",
                                    "recent-benchmark-item",
                                    is_selected.then_some("active"),
                                    is_pinned.then_some("pinned"),
                                )} onclick={on_select}>
                                    <div class="benchmark-list-item-content">
                                        <div class="benchmark-list-item-header">
                                            <div class="benchmark-list-item-title">
                                                <span class={classes!("benchmark-list-item-dot", benchmark.params.benchmark_kind.to_string().to_lowercase().replace(" ", "-"))}></span>
                                                {benchmark.params.benchmark_kind.to_string()}
                                            </div>
                                            <div class="benchmark-list-item-time">{timestamp_display}</div>
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

                                        <div class="benchmark-list-item-details">
                                            <div class="benchmark-list-item-id-version-line">
                                                <div class="benchmark-list-item-subtitle">
                                                    <span class="benchmark-list-item-label">{"Identifier:"}</span>
                                                    <span>{benchmark.hardware.identifier.as_deref().unwrap_or("Unknown")}</span>
                                                </div>
                                                <div class="benchmark-list-item-subtitle benchmark-version-subtitle">
                                                    <span class="benchmark-list-item-label">{"Version:"}</span>
                                                    <span>{benchmark.params.gitref.as_deref().unwrap_or("Unknown")}</span>
                                                </div>
                                            </div>

                                            <div class="benchmark-list-item-metrics">
                                                <div class="metrics-group">
                                                    {if let Some(metrics) = benchmark.group_metrics.first() {
                                                        html! {
                                                            <>
                                                                <div class="benchmark-list-item-metric latency">
                                                                    <span class="benchmark-list-item-label">{"P99:"}</span>
                                                                    <span>{format!("{:.2} ms", metrics.summary.average_p99_latency_ms)}</span>
                                                                </div>
                                                                <div class="benchmark-list-item-metric throughput">
                                                                    <span>{format!("{:.2} MB/s", metrics.summary.total_throughput_megabytes_per_second)}</span>
                                                                </div>
                                                                <div class="benchmark-list-item-metric message-throughput">
                                                                    <span>{format!("{} msg/s", metrics.summary.total_throughput_messages_per_second as u32)}</span>
                                                                </div>
                                                            </>
                                                        }
                                                    } else {
                                                        html! {
                                                            <>
                                                                <div class="benchmark-list-item-metric latency">
                                                                    <span class="benchmark-list-item-label">{"P99:"}</span>
                                                                    <span>{"N/A"}</span>
                                                                </div>
                                                                <div class="benchmark-list-item-metric throughput">
                                                                    <span>{"N/A"}</span>
                                                                </div>
                                                                <div class="benchmark-list-item-metric message-throughput">
                                                                    <span>{"N/A"}</span>
                                                                </div>
                                                            </>
                                                        }
                                                    }}
                                                </div>

                                                {if let Some(remark) = benchmark.params.remark.as_deref()
                                                    && !remark.is_empty()
                                                {
                                                        let truncated_remark = if remark.len() > 30 {
                                                            format!("{}..", &remark[0..28])
                                                        } else {
                                                            remark.to_string()
                                                        };
                                                        html! {
                                                            <div class="benchmark-list-item-remark inline-remark">
                                                                {truncated_remark}
                                                            </div>
                                                        }
                                                } else {
                                                    html! {}
                                                }}
                                            </div>
                                        </div>
                                    </div>
                                </li>
                            }
                        }).collect::<Html>()}
                    </ul>
                }
                </div>
            </div>
        </div>
    }
}
