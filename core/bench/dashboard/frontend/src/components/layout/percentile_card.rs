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

use bench_dashboard_shared::BenchmarkReportLight;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct PercentileCardProps {
    pub benchmark: BenchmarkReportLight,
}

#[function_component(PercentileCard)]
pub fn percentile_card(props: &PercentileCardProps) -> Html {
    let Some(summary) = props
        .benchmark
        .group_metrics
        .first()
        .map(|m| m.summary.clone())
    else {
        return html! {};
    };

    let rows = [
        ("P50", summary.average_p50_latency_ms),
        ("P95", summary.average_p95_latency_ms),
        ("P99", summary.average_p99_latency_ms),
        ("P99.9", summary.average_p999_latency_ms),
        ("P99.99", summary.average_p9999_latency_ms),
    ];
    let max_value = rows
        .iter()
        .map(|(_, latency)| *latency)
        .fold(0.0_f64, |acc, v| if v > acc { v } else { acc });

    if max_value <= 0.0 {
        return html! {};
    }

    html! {
        <div class="percentile-strip">
            <div class="percentile-strip-label">
                <span class="percentile-strip-title">{"Latency"}</span>
                <span class="percentile-strip-unit">{"ms"}</span>
            </div>
            <div class="percentile-strip-chips">
                { for rows.iter().map(|(label, value)| render_chip(label, *value, max_value)) }
            </div>
        </div>
    }
}

fn render_chip(label: &str, value: f64, max_value: f64) -> Html {
    let percent = if max_value > 0.0 {
        (value / max_value * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    html! {
        <div class="percentile-chip" title={format!("{label}: {value:.3} ms")}>
            <div class="percentile-chip-head">
                <span class="percentile-chip-label">{label}</span>
                <span class="percentile-chip-value">{format_latency(value)}</span>
            </div>
            <div class="percentile-chip-bar">
                <div class="percentile-chip-fill" style={format!("width: {percent:.1}%")} />
            </div>
        </div>
    }
}

fn format_latency(ms: f64) -> String {
    if ms >= 10.0 {
        format!("{ms:.1}")
    } else if ms >= 1.0 {
        format!("{ms:.2}")
    } else {
        format!("{ms:.3}")
    }
}
