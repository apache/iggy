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

use crate::state::ui::ViewMode;
use bench_dashboard_shared::BenchmarkReportLight;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ServerStatsTooltipProps {
    pub benchmark_report: Option<BenchmarkReportLight>,
    pub visible: bool,
    pub view_mode: ViewMode,
}

#[function_component(ServerStatsTooltip)]
pub fn server_stats_tooltip(props: &ServerStatsTooltipProps) -> Html {
    if !props.visible || props.benchmark_report.is_none() {
        return html! {};
    }

    let benchmark_report = props.benchmark_report.as_ref().unwrap();
    let stats = &benchmark_report.server_stats;

    // Convert bytes to MB for better readability
    let memory_usage_mb = stats.memory_usage as f64 / 1024.0 / 1024.0;
    let total_memory_mb = stats.total_memory as f64 / 1024.0 / 1024.0;
    let available_memory_mb = stats.available_memory as f64 / 1024.0 / 1024.0;
    let messages_size_mb = stats.messages_size_bytes as f64 / 1024.0 / 1024.0;

    // Convert bytes to GB for read/written bytes
    let read_bytes_gb = stats.read_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
    let written_bytes_gb = stats.written_bytes as f64 / 1024.0 / 1024.0 / 1024.0;

    // Convert runtime from microseconds to a readable format
    let runtime_secs = stats.run_time / 1_000_000; // Convert microseconds to seconds
    let runtime_hours = runtime_secs / 3600;
    let runtime_mins = (runtime_secs % 3600) / 60;
    let runtime_secs = runtime_secs % 60;
    let runtime_str = format!("{}h {}m {}s", runtime_hours, runtime_mins, runtime_secs);

    html! {
        <div class="benchmark-info-tooltip server-stats-position">
            <div class="tooltip-section">
                <h4>{"Iggy Server Information"}</h4>
                <div class="tooltip-content">
                    <p><strong>{"Host: "}</strong>{&stats.hostname}</p>
                    <p><strong>{"OS: "}</strong>{&stats.os_name}</p>
                    <p><strong>{"Process ID: "}</strong>{stats.process_id}</p>
                    <p><strong>{"runtime: "}</strong>{runtime_str}</p>
                </div>
            </div>

            <div class="tooltip-section">
                <h4>{"Resource Usage"}</h4>
                <div class="tooltip-content">
                    <p><strong>{"CPU Usage: "}</strong>{format!("{:.1}%", stats.cpu_usage)}</p>
                    <p><strong>{"Total CPU Usage: "}</strong>{format!("{:.1}%", stats.total_cpu_usage)}</p>
                    <p><strong>{"Memory Usage: "}</strong>{format!("{:.1} MB / {:.1} MB", memory_usage_mb, total_memory_mb)}</p>
                    <p><strong>{"Available Memory: "}</strong>{format!("{:.1} MB", available_memory_mb)}</p>
                </div>
            </div>

            <div class="tooltip-section">
                <h4>{"Storage"}</h4>
                <div class="tooltip-content">
                    <p><strong>{"Read: "}</strong>{format!("{:.2} GB", read_bytes_gb)}</p>
                    <p><strong>{"Written: "}</strong>{format!("{:.2} GB", written_bytes_gb)}</p>
                    <p><strong>{"Messages Size: "}</strong>{format!("{:.2} MB", messages_size_mb)}</p>
                </div>
            </div>

            <div class="tooltip-section">
                <h4>{"Cache Metrics"}</h4>
                <div class="tooltip-content">
                    {
                        if stats.cache_metrics.is_empty() {
                            html! {
                                <p>{"No cache metrics available"}</p>
                            }
                        } else {
                            let mut total_hits = 0u64;
                            let mut total_misses = 0u64;

                            for metrics in stats.cache_metrics.values() {
                                total_hits += metrics.hits;
                                total_misses += metrics.misses;
                            }

                            let total_accesses = total_hits + total_misses;
                            let hit_ratio = if total_accesses > 0 {
                                (total_hits as f64 / total_accesses as f64) * 100.0
                            } else {
                                0.0
                            };

                            html! {
                                <>
                                    <p><strong>{"Cache Hits: "}</strong>{total_hits}</p>
                                    <p><strong>{"Cache Misses: "}</strong>{total_misses}</p>
                                    <p><strong>{"Hit Ratio: "}</strong>{format!("{:.1}%", hit_ratio)}</p>
                                </>
                            }
                        }
                    }
                </div>
            </div>

            <div class="tooltip-section">
                <h4>{"Messaging Stats"}</h4>
                <div class="tooltip-content">
                    <p><strong>{"Messages: "}</strong>{stats.messages_count}</p>
                    <p><strong>{"Streams: "}</strong>{stats.streams_count}</p>
                    <p><strong>{"Topics: "}</strong>{stats.topics_count}</p>
                    <p><strong>{"Partitions: "}</strong>{stats.partitions_count}</p>
                    <p><strong>{"Segments: "}</strong>{stats.segments_count}</p>
                    <p><strong>{"Clients: "}</strong>{stats.clients_count}</p>
                    <p><strong>{"Consumer Groups: "}</strong>{stats.consumer_groups_count}</p>
                </div>
            </div>
        </div>
    }
}
