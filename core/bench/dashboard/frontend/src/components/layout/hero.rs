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

use crate::api;
use crate::components::chart::tail_chart::TailChart;
use crate::format::{format_ms, nan_safe_cmp};
use crate::router::AppRoute;
use crate::state::benchmark::{pick_best_from_recent_batch, use_benchmark};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use chrono::DateTime;
use gloo::console::log;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;
use yew::platform::spawn_local;
use yew::prelude::*;
use yew_router::prelude::{Navigator, use_navigator};

#[derive(Properties, PartialEq)]
pub struct HeroProps {
    pub selected_gitref: String,
}

#[function_component(Hero)]
pub fn hero(props: &HeroProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let navigator = use_navigator();
    let recent = use_state(Vec::<BenchmarkReportLight>::new);

    {
        let recent = recent.clone();
        let cancelled = Rc::new(Cell::new(false));
        let cancelled_async = cancelled.clone();
        use_effect_with((), move |_| {
            spawn_local(async move {
                match api::fetch_recent_benchmarks(Some(10_000)).await {
                    Ok(data) => {
                        if !cancelled_async.get() {
                            recent.set(data);
                        }
                    }
                    Err(error) => log!(format!("Hero: fetch_recent_benchmarks failed: {}", error)),
                }
            });
            move || cancelled.set(true)
        });
    }

    let recent_vec = (*recent).clone();
    let source: Vec<&BenchmarkReportLight> = if recent_vec.is_empty() {
        benchmark_ctx.state.entries.values().flatten().collect()
    } else {
        recent_vec.iter().collect()
    };
    let unrestricted: Vec<&BenchmarkReportLight> = source
        .iter()
        .copied()
        .filter(|benchmark| benchmark.params.rate_limit.is_none())
        .collect();
    let mut stats = compute_stats(unrestricted.iter().copied());
    stats.showcase = unrestricted
        .iter()
        .copied()
        .max_by(|left, right| nan_safe_cmp(throughput_mb(left), throughput_mb(right)))
        .cloned();
    if stats.showcase.is_none() && !source.is_empty() {
        stats.showcase = pick_best_from_recent_batch(&source);
    }

    if stats.total == 0 {
        return html! {
            <div class="hero-v2 hero-v2-empty">
                { render_background_grid() }
                <div class="hero-v2-empty-inner">
                    <div class="hero-v2-eyebrow">{"Apache Iggy"}</div>
                    <h1 class="hero-v2-empty-title">{"Benchmarks"}</h1>
                    <p class="hero-v2-empty-sub">
                        {"Pick a hardware and gitref in the sidebar to view performance data."}
                    </p>
                </div>
            </div>
        };
    }

    let hardware = benchmark_ctx
        .state
        .current_hardware
        .clone()
        .unwrap_or_default();
    let gitref_suffix = if props.selected_gitref.is_empty() {
        String::new()
    } else {
        format!(" @ {}", props.selected_gitref)
    };

    let on_view_details = stats.showcase.as_ref().map(|showcase| {
        let uuid = showcase.uuid.to_string();
        let navigator = navigator.clone();
        Callback::from(move |_| {
            if let Some(nav) = navigator.as_ref() {
                nav.push(&AppRoute::Benchmark { uuid: uuid.clone() });
            }
        })
    });

    let on_browse_click = {
        let latest_uuid = latest_uuid_from_entries(&benchmark_ctx.state.entries);
        let navigator = navigator.clone();
        Callback::from(move |_: MouseEvent| {
            if let Some(uuid) = latest_uuid.clone() {
                navigate_to_benchmark(&navigator, uuid);
            } else {
                let navigator = navigator.clone();
                spawn_local(async move {
                    if let Some(uuid) = fetch_latest_uuid().await {
                        navigate_to_benchmark(&navigator, uuid);
                    }
                });
            }
        })
    };

    html! {
        <div class="hero-v2">
            { render_background_grid() }
            <div class="hero-v2-inner">
                { render_headline(&stats, &hardware, &gitref_suffix, &on_browse_click) }
                { render_stat_cards(&stats) }
                {
                    match (stats.showcase.as_ref(), on_view_details) {
                        (Some(showcase), Some(details_callback)) => html! {
                            <TailChart
                                benchmark={showcase.clone()}
                                on_details={details_callback}
                            />
                        },
                        _ => html! {},
                    }
                }
            </div>
        </div>
    }
}

fn latest_uuid_from_entries(
    entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
) -> Option<String> {
    entries
        .values()
        .flatten()
        .max_by(|left, right| left.timestamp.cmp(&right.timestamp))
        .map(|benchmark| benchmark.uuid.to_string())
}

async fn fetch_latest_uuid() -> Option<String> {
    match api::fetch_recent_benchmarks(Some(1)).await {
        Ok(recent) => recent.into_iter().next().map(|b| b.uuid.to_string()),
        Err(error) => {
            log!(format!("Browse: fetch_recent_benchmarks failed: {}", error));
            None
        }
    }
}

fn navigate_to_benchmark(navigator: &Option<Navigator>, uuid: String) {
    if let Some(nav) = navigator.as_ref() {
        nav.push(&AppRoute::Benchmark { uuid });
    }
}

#[derive(Default)]
struct HeroStats {
    peak_mb_s: Option<(f64, String)>,
    peak_msg_s: Option<(f64, String)>,
    max_scale: Option<MaxScale>,
    total: usize,
    latest_ts: Option<String>,
    showcase: Option<BenchmarkReportLight>,
}

struct MaxScale {
    producers: u32,
    consumers: u32,
    pretty_name: String,
}

impl MaxScale {
    fn total_actors(&self) -> u32 {
        self.producers + self.consumers
    }
}

fn compute_stats<'a>(benchmarks: impl Iterator<Item = &'a BenchmarkReportLight>) -> HeroStats {
    let mut stats = HeroStats::default();

    for benchmark in benchmarks {
        stats.total += 1;
        let Some(summary) = benchmark
            .group_metrics
            .first()
            .map(|metrics| &metrics.summary)
        else {
            continue;
        };
        let throughput_megabytes = summary.total_throughput_megabytes_per_second;
        let throughput_messages = summary.total_throughput_messages_per_second;
        let pretty_name = benchmark.params.pretty_name.clone();

        if stats
            .peak_mb_s
            .as_ref()
            .is_none_or(|(current, _)| throughput_megabytes > *current)
        {
            stats.peak_mb_s = Some((throughput_megabytes, pretty_name.clone()));
        }
        if stats
            .peak_msg_s
            .as_ref()
            .is_none_or(|(current, _)| throughput_messages > *current)
        {
            stats.peak_msg_s = Some((throughput_messages, pretty_name.clone()));
        }
        let producers = benchmark.params.producers;
        let consumers = benchmark.params.consumers;
        let total_actors = producers + consumers;
        if stats
            .max_scale
            .as_ref()
            .is_none_or(|current| total_actors > current.total_actors())
        {
            stats.max_scale = Some(MaxScale {
                producers,
                consumers,
                pretty_name: pretty_name.clone(),
            });
        }
        if stats
            .latest_ts
            .as_ref()
            .is_none_or(|current| &benchmark.timestamp > current)
        {
            stats.latest_ts = Some(benchmark.timestamp.clone());
        }
    }
    stats
}

fn render_background_grid() -> Html {
    html! {
        <div class="hero-v2-bg" aria-hidden="true">
            <div class="hero-v2-bg-dot-grid" />
            <div class="hero-v2-bg-glow hero-v2-bg-glow-primary" />
            <div class="hero-v2-bg-glow hero-v2-bg-glow-secondary" />
        </div>
    }
}

fn render_headline(
    stats: &HeroStats,
    hardware: &str,
    gitref_suffix: &str,
    on_browse_click: &Callback<MouseEvent>,
) -> Html {
    let (value, unit, subject) = match &stats.peak_mb_s {
        Some((throughput, name)) => {
            let (formatted, unit) = format_throughput_bytes(*throughput);
            (formatted, unit, name.clone())
        }
        None => ("-".to_string(), "MB/s", String::new()),
    };
    let sub = if subject.is_empty() {
        format!("{hardware}{gitref_suffix}")
    } else {
        format!("{subject} · {hardware}{gitref_suffix}")
    };

    html! {
        <div class="hero-v2-headline">
            <div class="hero-v2-eyebrow">{"Peak sustained throughput"}</div>
            <h1 class="hero-v2-title">
                <span class="hero-v2-big">{value}</span>
                <span class="hero-v2-unit">{unit}</span>
            </h1>
            <p class="hero-v2-sub">{sub}</p>
            <p class="hero-v2-tagline">
                {"Modern hardware is incredibly capable. "}
                <span class="hero-v2-tagline-accent">{"Apache Iggy was built for it."}</span>
            </p>
            <div class="hero-v2-actions">
                <button
                    type="button"
                    class="hero-v2-browse-btn"
                    onclick={on_browse_click.clone()}
                >
                    {"Browse all benchmarks"}
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
                         fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <line x1="5" y1="12" x2="19" y2="12" />
                        <polyline points="12 5 19 12 12 19" />
                    </svg>
                </button>
            </div>
        </div>
    }
}

fn render_stat_cards(stats: &HeroStats) -> Html {
    html! {
        <div class="hero-v2-cards">
            {
                render_stat_card(0, "Peak throughput", stats.peak_msg_s.as_ref().map(|(rate, name)| {
                    let (formatted, unit) = format_msg_rate(*rate);
                    (formatted, unit, name.clone())
                }))
            }
            { render_scale_card(1, stats.max_scale.as_ref()) }
            { render_showcase_card(2, stats.showcase.as_ref()) }
            { render_summary_card(3, stats.total, stats.latest_ts.as_deref()) }
        </div>
    }
}

fn render_stat_card(
    stagger: usize,
    label: &'static str,
    value: Option<(String, &'static str, String)>,
) -> Html {
    let Some((formatted, unit, name)) = value else {
        return html! {};
    };
    html! {
        <div class="hero-v2-card" style={format!("--stagger: {stagger}")}>
            <div class="hero-v2-card-value-row">
                <span class="hero-v2-card-value">{formatted}</span>
                <span class="hero-v2-card-unit">{unit}</span>
            </div>
            <div class="hero-v2-card-label">{label}</div>
            <div class="hero-v2-card-sub" title={name.clone()}>{name}</div>
        </div>
    }
}

fn render_scale_card(stagger: usize, scale: Option<&MaxScale>) -> Html {
    let Some(scale) = scale else {
        return html! {};
    };
    let total = scale.total_actors();
    let breakdown = if scale.producers > 0 && scale.consumers > 0 {
        format!(
            "{} producers × {} consumers",
            scale.producers, scale.consumers
        )
    } else if scale.producers > 0 {
        format!("{} producers", scale.producers)
    } else {
        format!("{} consumers", scale.consumers)
    };
    html! {
        <div class="hero-v2-card" style={format!("--stagger: {stagger}")}>
            <div class="hero-v2-card-value-row">
                <span class="hero-v2-card-value">{total}</span>
                <span class="hero-v2-card-unit">{"actors"}</span>
            </div>
            <div class="hero-v2-card-label">{"Max scale tested"}</div>
            <div class="hero-v2-card-sub" title={scale.pretty_name.clone()}>{breakdown}</div>
        </div>
    }
}

fn render_showcase_card(stagger: usize, showcase: Option<&BenchmarkReportLight>) -> Html {
    let Some(benchmark) = showcase else {
        return html! {};
    };
    let Some(summary) = benchmark.group_metrics.first().map(|m| &m.summary) else {
        return html! {};
    };
    let p99 = summary.average_p99_latency_ms;
    let name = benchmark.params.pretty_name.clone();

    html! {
        <div class="hero-v2-card hero-v2-card-accent" style={format!("--stagger: {stagger}")}>
            <div class="hero-v2-card-value-row">
                <span class="hero-v2-card-value">{format_ms(p99)}</span>
                <span class="hero-v2-card-unit">{"ms"}</span>
            </div>
            <div class="hero-v2-card-label">{"P99 at peak throughput"}</div>
            <div class="hero-v2-card-sub" title={name.clone()}>{name}</div>
        </div>
    }
}

fn throughput_mb(benchmark: &BenchmarkReportLight) -> f64 {
    benchmark
        .group_metrics
        .first()
        .map(|metrics| metrics.summary.total_throughput_megabytes_per_second)
        .unwrap_or(0.0)
}

fn render_summary_card(stagger: usize, total: usize, latest_ts: Option<&str>) -> Html {
    let sub = match latest_ts {
        Some(ts) => format!("Latest: {}", format_date(ts)),
        None => String::new(),
    };
    html! {
        <div class="hero-v2-card" style={format!("--stagger: {stagger}")}>
            <div class="hero-v2-card-value-row">
                <span class="hero-v2-card-value">{total}</span>
                <span class="hero-v2-card-unit">{"runs"}</span>
            </div>
            <div class="hero-v2-card-label">{"Benchmarks loaded"}</div>
            <div class="hero-v2-card-sub">{sub}</div>
        </div>
    }
}

fn format_throughput_bytes(mb_per_s: f64) -> (String, &'static str) {
    if mb_per_s >= 1_000_000.0 {
        (format_significant(mb_per_s / 1_000_000.0), "TB/s")
    } else if mb_per_s >= 1_000.0 {
        (format_significant(mb_per_s / 1_000.0), "GB/s")
    } else {
        (format_significant(mb_per_s), "MB/s")
    }
}

fn format_msg_rate(rate: f64) -> (String, &'static str) {
    if rate >= 1_000_000_000.0 {
        (format_significant(rate / 1_000_000_000.0), "B msg/s")
    } else if rate >= 1_000_000.0 {
        (format_significant(rate / 1_000_000.0), "M msg/s")
    } else if rate >= 1_000.0 {
        (format_significant(rate / 1_000.0), "k msg/s")
    } else {
        (format!("{rate:.0}"), "msg/s")
    }
}

fn format_significant(v: f64) -> String {
    if v >= 100.0 {
        format!("{v:.0}")
    } else if v >= 10.0 {
        format!("{v:.1}")
    } else {
        format!("{v:.2}")
    }
}

fn format_date(timestamp_str: &str) -> String {
    match DateTime::parse_from_rfc3339(timestamp_str) {
        Ok(t) => t.format("%Y-%m-%d").to_string(),
        Err(_) => "unknown".to_string(),
    }
}
