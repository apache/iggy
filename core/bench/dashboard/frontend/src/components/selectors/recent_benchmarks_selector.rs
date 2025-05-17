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
};
use bench_dashboard_shared::BenchmarkReportLight;
use chrono::{DateTime, Utc};
use gloo::console::log;
use yew::prelude::*;
use yew_hooks::use_async;

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

    let recent_benchmarks = use_state(Vec::<BenchmarkReportLight>::new);

    let fetch_benchmarks = {
        let recent_benchmarks = recent_benchmarks.clone();
        let limit = props.limit;

        use_async(async move {
            match api::fetch_recent_benchmarks(Some(limit)).await {
                Ok(mut data) => {
                    data.sort_by(|a, b| {
                        let parse_a = DateTime::parse_from_rfc3339(&a.timestamp);
                        let parse_b = DateTime::parse_from_rfc3339(&b.timestamp);
                        match (parse_a, parse_b) {
                            (Ok(time_a), Ok(time_b)) => time_b.cmp(&time_a),
                            _ => std::cmp::Ordering::Equal,
                        }
                    });
                    recent_benchmarks.set(data.clone());
                    Ok(data)
                }
                Err(e) => {
                    log!(format!("Error fetching recent benchmarks: {}", e));
                    Err(e)
                }
            }
        })
    };

    {
        let fetch_benchmarks_effect = fetch_benchmarks.clone();
        use_effect_with((), move |_| {
            fetch_benchmarks_effect.run();
            || ()
        });
    }

    let on_benchmark_select = {
        let benchmark_ctx = benchmark_ctx.clone();
        Callback::from(move |benchmark: BenchmarkReportLight| {
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmark(Box::new(Some(benchmark))));
        })
    };

    let filtered_benchmarks = (*recent_benchmarks)
        .iter()
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

            if let Some(identifier) = &benchmark.hardware.identifier {
                if identifier.to_lowercase().contains(&query) {
                    return true;
                }
            }

            if let Some(gitref) = &benchmark.params.gitref {
                if gitref.to_lowercase().contains(&query) {
                    return true;
                }
            }

            if let Some(remark) = &benchmark.params.remark {
                if remark.to_lowercase().contains(&query) {
                    return true;
                }
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
                if fetch_benchmarks.loading {
                    <p class="loading-message">{"Loading recent benchmarks..."}</p>
                } else if filtered_benchmarks.is_empty() {
                    <div class="no-search-results">
                        <p>{format!("No benchmarks found matching \"{}\"", props.search_query)}</p>
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
                            let timestamp_display = format_relative_time(&benchmark.timestamp);

                            let is_selected = benchmark_ctx.state.selected_benchmark.as_ref()
                                .map(|selected| selected.uuid == benchmark.uuid)
                                .unwrap_or(false);

                            html! {
                                <li class={classes!("benchmark-list-item", "recent-benchmark-item", is_selected.then_some("active"))} onclick={on_select}>
                                    <div class="benchmark-list-item-content">
                                        <div class="benchmark-list-item-header">
                                            <div class="benchmark-list-item-title">
                                                <span class={classes!("benchmark-list-item-dot", benchmark.params.benchmark_kind.to_string().to_lowercase().replace(" ", "-"))}></span>
                                                {benchmark.params.benchmark_kind.to_string()}
                                            </div>
                                            <div class="benchmark-list-item-time">{timestamp_display}</div>
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

                                                {if let Some(remark) = benchmark.params.remark.as_deref() {
                                                    if !remark.is_empty() {
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
