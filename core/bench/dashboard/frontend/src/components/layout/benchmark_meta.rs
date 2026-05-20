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

use crate::format::{format_bytes, format_count, format_ms, format_throughput_mb_s};
use bench_dashboard_shared::{BenchmarkGroupMetricsLight, BenchmarkReportLight};
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::group_metrics_kind::GroupMetricsKind;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BenchmarkMetaProps {
    pub benchmark: BenchmarkReportLight,
}

#[function_component(BenchmarkMeta)]
pub fn benchmark_meta(props: &BenchmarkMetaProps) -> Html {
    let benchmark = &props.benchmark;
    let is_open = use_state(|| true);

    let on_toggle = {
        let is_open = is_open.clone();
        Callback::from(move |_: MouseEvent| is_open.set(!*is_open))
    };

    let chevron = if *is_open { "v" } else { ">" };
    let toggle_label = if *is_open {
        "Hide details"
    } else {
        "Show details"
    };

    html! {
        <div class={classes!("benchmark-meta", (!*is_open).then_some("collapsed"))}>
            <button type="button" class="benchmark-meta-toggle" onclick={on_toggle}
                    aria-expanded={(*is_open).to_string()}>
                <span class="benchmark-meta-toggle-chevron">{chevron}</span>
                <span class="benchmark-meta-toggle-label">{"Benchmark details"}</span>
                <span class="benchmark-meta-toggle-hint">{toggle_label}</span>
            </button>
            if *is_open {
                <div class="benchmark-meta-body">
                    { render_config_row(benchmark) }
                    { for benchmark.group_metrics.iter()
                        .filter(|metrics| metrics.summary.kind != GroupMetricsKind::ProducersAndConsumers)
                        .map(render_metrics_rows) }
                </div>
            }
        </div>
    }
}

fn render_config_row(benchmark: &BenchmarkReportLight) -> Html {
    let params = &benchmark.params;
    let sent = benchmark.total_messages_sent();
    let polled = benchmark.total_messages_received();
    let total_bytes = benchmark.total_bytes();

    let mut chips = vec![(actors_label(benchmark), actors_value(benchmark))];
    if params.streams > 0 {
        chips.push(("Streams", params.streams.to_string()));
        // Benchmarks currently create 1 topic per stream; expose it so the
        // streams -> topics -> partitions hierarchy is explicit.
        chips.push(("Topics", params.streams.to_string()));
    }
    if params.partitions > 0 {
        chips.push(("Partitions", params.partitions.to_string()));
    }
    if params.consumer_groups > 0 && kind_uses_consumer_groups(params.benchmark_kind) {
        chips.push(("Consumer groups", params.consumer_groups.to_string()));
    }
    chips.push((
        "Batch",
        format!("{} × {}", params.messages_per_batch, params.message_batches),
    ));
    chips.push(("Msg size", format!("{} B", params.message_size)));
    if sent > 0 {
        chips.push(("Sent", format_count(sent)));
    }
    if polled > 0 {
        chips.push(("Polled", format_count(polled)));
    }
    chips.push(("Volume", format_bytes(total_bytes)));

    html! {
        <div class="benchmark-meta-row">
            <span class="benchmark-meta-label">{"Config"}</span>
            <div class="benchmark-meta-chips">
                { for chips.into_iter().map(|(label, value)| render_chip(label, &value, "config")) }
            </div>
        </div>
    }
}

fn render_metrics_rows(metrics: &BenchmarkGroupMetricsLight) -> Html {
    let summary = &metrics.summary;
    let kind_name = summary.kind.to_string();
    let actor = summary.kind.actor();

    let mut latency_chips: Vec<(&'static str, String)> = vec![
        ("Avg", format_ms(summary.average_latency_ms)),
        ("Median", format_ms(summary.average_median_latency_ms)),
        ("P95", format_ms(summary.average_p95_latency_ms)),
        ("P99", format_ms(summary.average_p99_latency_ms)),
        ("P99.9", format_ms(summary.average_p999_latency_ms)),
        ("P99.99", format_ms(summary.average_p9999_latency_ms)),
    ];
    if summary.min_latency_ms > 0.0 {
        latency_chips.push(("Min", format_ms(summary.min_latency_ms)));
    }
    if summary.max_latency_ms > 0.0 {
        latency_chips.push(("Max", format_ms(summary.max_latency_ms)));
    }
    if summary.std_dev_latency_ms > 0.0 {
        latency_chips.push(("Std dev", format_ms(summary.std_dev_latency_ms)));
    }

    let throughput_chips: Vec<(String, String)> = vec![
        (
            "Total".to_string(),
            format!(
                "{} · {} msg/s",
                format_throughput_mb_s(summary.total_throughput_megabytes_per_second),
                format_count(summary.total_throughput_messages_per_second as u64),
            ),
        ),
        (
            format!("Per {}", actor.to_lowercase()),
            format!(
                "{} · {} msg/s",
                format_throughput_mb_s(summary.average_throughput_megabytes_per_second),
                format_count(summary.average_throughput_messages_per_second as u64),
            ),
        ),
    ];

    html! {
        <>
            <div class="benchmark-meta-row">
                <span class="benchmark-meta-label">
                    {format!("{kind_name} · Latency (ms)")}
                </span>
                <div class="benchmark-meta-chips">
                    { for latency_chips.into_iter().map(|(label, value)| render_chip(label, &value, "latency")) }
                </div>
            </div>
            <div class="benchmark-meta-row">
                <span class="benchmark-meta-label">
                    {format!("{kind_name} · Throughput")}
                </span>
                <div class="benchmark-meta-chips">
                    { for throughput_chips.into_iter().map(|(label, value)| render_chip(&label, &value, "throughput")) }
                </div>
            </div>
        </>
    }
}

fn render_chip(label: &str, value: &str, flavor: &str) -> Html {
    html! {
        <span class={classes!("benchmark-meta-chip", format!("flavor-{flavor}"))}>
            <span class="benchmark-meta-chip-label">{label}</span>
            <span class="benchmark-meta-chip-value">{value}</span>
        </span>
    }
}

fn actors_label(benchmark: &BenchmarkReportLight) -> &'static str {
    let has_producers = benchmark
        .individual_metrics
        .iter()
        .any(|m| m.summary.actor_kind == ActorKind::Producer);
    let has_consumers = benchmark
        .individual_metrics
        .iter()
        .any(|m| m.summary.actor_kind == ActorKind::Consumer);
    match (has_producers, has_consumers) {
        (true, true) => "Actors",
        (true, false) => "Producers",
        (false, true) => "Consumers",
        _ => "Actors",
    }
}

fn kind_uses_consumer_groups(kind: BenchmarkKind) -> bool {
    matches!(
        kind,
        BenchmarkKind::BalancedConsumerGroup
            | BenchmarkKind::BalancedProducerAndConsumerGroup
            | BenchmarkKind::EndToEndProducingConsumerGroup
    )
}

fn actors_value(benchmark: &BenchmarkReportLight) -> String {
    let params = &benchmark.params;
    match (params.producers, params.consumers) {
        (0, 0) => "-".to_string(),
        (producers, 0) => producers.to_string(),
        (0, consumers) => consumers.to_string(),
        (producers, consumers) => format!("{producers}P / {consumers}C"),
    }
}
