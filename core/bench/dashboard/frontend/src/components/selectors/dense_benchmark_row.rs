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

use crate::format::{format_ms, format_throughput_mb_s};
use bench_dashboard_shared::BenchmarkReportLight;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct DenseBenchmarkRowProps {
    pub benchmark: BenchmarkReportLight,
    pub selected_uuid: Option<Uuid>,
    pub pinned_uuid: Option<Uuid>,
    pub on_select: Callback<BenchmarkReportLight>,
    pub on_toggle_pin: Callback<BenchmarkReportLight>,
    #[prop_or(true)]
    pub show_timestamp: bool,
}

#[function_component(DenseBenchmarkRow)]
pub fn dense_benchmark_row(props: &DenseBenchmarkRowProps) -> Html {
    let benchmark = &props.benchmark;
    let is_selected = Some(benchmark.uuid) == props.selected_uuid;
    let is_pinned = Some(benchmark.uuid) == props.pinned_uuid;

    let display_name = {
        let full = &benchmark.params.pretty_name;
        full.split('(').next().unwrap_or(full).trim().to_string()
    };

    let kind_class = benchmark
        .params
        .benchmark_kind
        .to_string()
        .to_lowercase()
        .replace(' ', "-");

    let on_select_click = {
        let on_select = props.on_select.clone();
        let benchmark = benchmark.clone();
        Callback::from(move |_: MouseEvent| on_select.emit(benchmark.clone()))
    };

    let on_pin_click = {
        let on_toggle_pin = props.on_toggle_pin.clone();
        let benchmark = benchmark.clone();
        Callback::from(move |_: MouseEvent| on_toggle_pin.emit(benchmark.clone()))
    };

    let pin_title = if is_pinned {
        "Stop comparing"
    } else {
        "Compare with this"
    };
    let select_aria = format!("Select benchmark {display_name}");

    html! {
        <div class={classes!(
            "dense-row",
            is_selected.then_some("active"),
            is_pinned.then_some("pinned"),
        )}>
            <button
                type="button"
                class="dense-row-select"
                onclick={on_select_click}
                aria-label={select_aria}
                aria-pressed={is_selected.to_string()}
            >
                <span class={classes!("dense-row-dot", kind_class)} />
                <div class="dense-row-body">
                    <div class="dense-row-title">{display_name}</div>
                    <div class="dense-row-meta">
                        { render_metrics(benchmark) }
                        if props.show_timestamp {
                            <span class="dense-row-meta-sep">{"·"}</span>
                            <span class="dense-row-time">{ relative_time(&benchmark.timestamp) }</span>
                        }
                    </div>
                </div>
            </button>
            <button
                type="button"
                class={classes!("dense-row-compare-btn", is_pinned.then_some("active"))}
                onclick={on_pin_click}
                title={pin_title}
                aria-label={pin_title}
            >
                if is_pinned {
                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
                         fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="20 6 9 17 4 12" />
                    </svg>
                } else {
                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
                         fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <rect x="3" y="4" width="7" height="16" rx="1.5" />
                        <rect x="14" y="4" width="7" height="16" rx="1.5" />
                    </svg>
                }
            </button>
        </div>
    }
}

fn render_metrics(benchmark: &BenchmarkReportLight) -> Html {
    let Some(summary) = benchmark
        .group_metrics
        .first()
        .map(|metrics| &metrics.summary)
    else {
        return html! { <span class="dense-row-metric">{"no metrics"}</span> };
    };
    let throughput = format_throughput_mb_s(summary.total_throughput_megabytes_per_second);
    let p99 = format_ms(summary.average_p99_latency_ms);

    html! {
        <>
            <span class="dense-row-metric tput">{throughput}</span>
            <span class="dense-row-meta-sep">{"·"}</span>
            <span class="dense-row-metric p99">
                <span class="dense-row-metric-label">{"P99"}</span>
                {p99}
                <span class="dense-row-metric-unit">{"ms"}</span>
            </span>
        </>
    }
}

fn relative_time(timestamp_str: &str) -> String {
    let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str) else {
        return "-".to_string();
    };
    let now = Utc::now();
    let duration = now.signed_duration_since(timestamp.with_timezone(&Utc));
    let seconds = duration.num_seconds();
    if seconds < 60 {
        format!("{seconds}s")
    } else if duration.num_minutes() < 60 {
        format!("{}m", duration.num_minutes())
    } else if duration.num_hours() < 24 {
        format!("{}h", duration.num_hours())
    } else if duration.num_days() < 30 {
        format!("{}d", duration.num_days())
    } else {
        timestamp.format("%Y-%m-%d").to_string()
    }
}
