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
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::params::BenchmarkParams;
use std::collections::BTreeMap;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SweepViewProps {
    pub benchmark: BenchmarkReportLight,
    pub entries: BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
    pub is_dark: bool,
}

#[function_component(SweepView)]
pub fn sweep_view(props: &SweepViewProps) -> Html {
    let Some((axis, points)) = find_sweep(&props.benchmark, &props.entries) else {
        return html! {};
    };

    let max_throughput = points
        .iter()
        .map(|point| point.throughput_mb_s)
        .fold(0.0_f64, f64::max);
    let max_p99 = points
        .iter()
        .map(|point| point.p99_ms)
        .fold(0.0_f64, f64::max);
    let min_axis = points.first().map(|point| point.axis_value).unwrap_or(0);
    let max_axis = points
        .last()
        .map(|point| point.axis_value)
        .unwrap_or(1)
        .max(1);

    html! {
        <div class="sweep-view">
            <div class="sweep-view-title">
                {"Scaling sweep: "}
                <span class="sweep-axis">{axis.label()}</span>
                <span class="sweep-view-hint">{format!("{} matched runs", points.len())}</span>
            </div>
            <svg class="sweep-chart" viewBox="0 0 800 260" preserveAspectRatio="none">
                { render_grid() }
                { render_tput_series(&points, min_axis, max_axis, max_throughput) }
                { render_p99_series(&points, min_axis, max_axis, max_p99) }
                { render_axes_labels(axis.label(), min_axis, max_axis, max_throughput, max_p99) }
            </svg>
            <div class="sweep-legend">
                <span class="sweep-legend-item sweep-legend-tput">{"Throughput (MB/s)"}</span>
                <span class="sweep-legend-item sweep-legend-p99">{"P99 latency (ms)"}</span>
            </div>
        </div>
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SweepAxis {
    Producers,
    Consumers,
    Streams,
    Partitions,
    ConsumerGroups,
    MessageSize,
    MessagesPerBatch,
    MessageBatches,
}

impl SweepAxis {
    const ALL: [SweepAxis; 8] = [
        SweepAxis::Producers,
        SweepAxis::Consumers,
        SweepAxis::Streams,
        SweepAxis::Partitions,
        SweepAxis::ConsumerGroups,
        SweepAxis::MessageSize,
        SweepAxis::MessagesPerBatch,
        SweepAxis::MessageBatches,
    ];

    fn label(self) -> &'static str {
        match self {
            SweepAxis::Producers => "producers",
            SweepAxis::Consumers => "consumers",
            SweepAxis::Streams => "streams",
            SweepAxis::Partitions => "partitions",
            SweepAxis::ConsumerGroups => "consumer groups",
            SweepAxis::MessageSize => "message size",
            SweepAxis::MessagesPerBatch => "messages per batch",
            SweepAxis::MessageBatches => "message batches",
        }
    }

    fn value(self, params: &BenchmarkParams) -> u32 {
        match self {
            SweepAxis::Producers => params.producers,
            SweepAxis::Consumers => params.consumers,
            SweepAxis::Streams => params.streams,
            SweepAxis::Partitions => params.partitions,
            SweepAxis::ConsumerGroups => params.consumer_groups,
            SweepAxis::MessageSize => params.message_size.min(),
            SweepAxis::MessagesPerBatch => params.messages_per_batch.min(),
            SweepAxis::MessageBatches => params.message_batches as u32,
        }
    }
}

#[derive(Clone, Copy)]
struct Point {
    axis_value: u32,
    throughput_mb_s: f64,
    p99_ms: f64,
}

fn find_sweep(
    selected: &BenchmarkReportLight,
    entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
) -> Option<(SweepAxis, Vec<Point>)> {
    let mut best: Option<(SweepAxis, Vec<Point>)> = None;
    for axis in SweepAxis::ALL {
        let mut points: Vec<Point> = entries
            .values()
            .flatten()
            .filter(|benchmark| siblings_match(selected, benchmark, axis))
            .filter_map(|benchmark| {
                benchmark
                    .group_metrics
                    .first()
                    .map(|group| (benchmark, group))
            })
            .map(|(benchmark, group)| Point {
                axis_value: axis.value(&benchmark.params),
                throughput_mb_s: group.summary.total_throughput_megabytes_per_second,
                p99_ms: group.summary.average_p99_latency_ms,
            })
            .collect();

        points.sort_by_key(|point| point.axis_value);
        points.dedup_by_key(|point| point.axis_value);

        if points.len() >= 2
            && best
                .as_ref()
                .is_none_or(|(_, previous)| points.len() > previous.len())
        {
            best = Some((axis, points));
        }
    }
    best
}

fn siblings_match(
    selected: &BenchmarkReportLight,
    candidate: &BenchmarkReportLight,
    varying_axis: SweepAxis,
) -> bool {
    if selected.params.benchmark_kind != candidate.params.benchmark_kind {
        return false;
    }
    if selected.params.transport != candidate.params.transport
        || selected.params.remark != candidate.params.remark
    {
        return false;
    }
    SweepAxis::ALL
        .iter()
        .filter(|axis| **axis != varying_axis)
        .all(|axis| axis.value(&selected.params) == axis.value(&candidate.params))
}

fn render_grid() -> Html {
    html! {
        <g class="sweep-grid">
            <line x1="60" y1="20" x2="60" y2="220" />
            <line x1="60" y1="220" x2="780" y2="220" />
            <line x1="780" y1="20" x2="780" y2="220" />
        </g>
    }
}

fn render_tput_series(points: &[Point], min_axis: u32, max_axis: u32, max_throughput: f64) -> Html {
    if max_throughput <= 0.0 {
        return html! {};
    }
    let path = polyline_path(points, min_axis, max_axis, max_throughput, |point| {
        point.throughput_mb_s
    });
    html! {
        <g class="sweep-series sweep-series-tput">
            <polyline points={path} fill="none" stroke="currentColor" stroke-width="2.5" />
            { for points.iter().map(|point| {
                let (screen_x, screen_y) = project(point.axis_value, point.throughput_mb_s, min_axis, max_axis, max_throughput);
                html! { <circle cx={screen_x.to_string()} cy={screen_y.to_string()} r="3.5" fill="currentColor" /> }
            })}
        </g>
    }
}

fn render_p99_series(points: &[Point], min_axis: u32, max_axis: u32, max_p99: f64) -> Html {
    if max_p99 <= 0.0 {
        return html! {};
    }
    let path = polyline_path(points, min_axis, max_axis, max_p99, |point| point.p99_ms);
    html! {
        <g class="sweep-series sweep-series-p99">
            <polyline points={path} fill="none" stroke="currentColor" stroke-width="2" stroke-dasharray="4 4" />
            { for points.iter().map(|point| {
                let (screen_x, screen_y) = project(point.axis_value, point.p99_ms, min_axis, max_axis, max_p99);
                html! { <circle cx={screen_x.to_string()} cy={screen_y.to_string()} r="3" fill="currentColor" /> }
            })}
        </g>
    }
}

fn render_axes_labels(
    axis_label: &str,
    min_axis: u32,
    max_axis: u32,
    max_throughput: f64,
    max_p99: f64,
) -> Html {
    html! {
        <g class="sweep-labels">
            <text x="60" y="240" text-anchor="start">{min_axis}</text>
            <text x="780" y="240" text-anchor="end">{max_axis}</text>
            <text x="420" y="254" text-anchor="middle" class="sweep-axis-label">{axis_label}</text>
            <text x="55" y="25" text-anchor="end" class="sweep-y-tput">{format!("{max_throughput:.0} MB/s")}</text>
            <text x="55" y="220" text-anchor="end" class="sweep-y-tput">{"0"}</text>
            <text x="785" y="25" text-anchor="start" class="sweep-y-p99">{format!("{max_p99:.1} ms")}</text>
            <text x="785" y="220" text-anchor="start" class="sweep-y-p99">{"0"}</text>
        </g>
    }
}

fn polyline_path(
    points: &[Point],
    min_axis: u32,
    max_axis: u32,
    max_value: f64,
    extract_value: impl Fn(&Point) -> f64,
) -> String {
    points
        .iter()
        .map(|point| {
            let (screen_x, screen_y) = project(
                point.axis_value,
                extract_value(point),
                min_axis,
                max_axis,
                max_value,
            );
            format!("{screen_x:.1},{screen_y:.1}")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn project(
    axis_value: u32,
    y_value: f64,
    min_axis: u32,
    max_axis: u32,
    max_value: f64,
) -> (f64, f64) {
    let span = (max_axis.saturating_sub(min_axis)).max(1) as f64;
    let normalized_x = (axis_value.saturating_sub(min_axis)) as f64 / span;
    let screen_x = 60.0 + normalized_x * 720.0;
    let screen_y = if max_value > 0.0 {
        220.0 - (y_value / max_value).clamp(0.0, 1.0) * 200.0
    } else {
        220.0
    };
    (screen_x, screen_y)
}
