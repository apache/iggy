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
use crate::format::format_ms;
use crate::router::AppRoute;
use crate::state::benchmark::{latest_sweep, pick_best_from_recent_batch};
use bench_dashboard_shared::BenchmarkReportLight;
use gloo::console::log;
use gloo::timers::callback::Timeout;
use std::cell::Cell;
use std::rc::Rc;
use yew::platform::spawn_local;
use yew::prelude::*;
use yew_router::prelude::{Navigator, use_navigator};

#[derive(Properties, PartialEq, Default)]
pub struct HeroProps;

#[function_component(Hero)]
pub fn hero(_props: &HeroProps) -> Html {
    let navigator = use_navigator();
    let (is_dark, _) = use_context::<(bool, Callback<()>)>().expect("Theme context not found");
    let recent = use_state(Vec::<BenchmarkReportLight>::new);
    let is_loading = use_state(|| true);
    let is_slow = use_state(|| false);

    {
        let recent = recent.clone();
        let is_loading = is_loading.clone();
        let cancelled = Rc::new(Cell::new(false));
        let cancelled_async = cancelled.clone();
        use_effect_with((), move |_| {
            spawn_local(async move {
                match api::fetch_recent_benchmarks(Some(10_000)).await {
                    Ok(data) => {
                        if !cancelled_async.get() {
                            recent.set(data);
                            is_loading.set(false);
                        }
                    }
                    Err(error) => {
                        log!(format!("Hero: fetch_recent_benchmarks failed: {}", error));
                        if !cancelled_async.get() {
                            is_loading.set(false);
                        }
                    }
                }
            });
            move || cancelled.set(true)
        });
    }

    {
        let is_slow = is_slow.clone();
        let is_loading_value = *is_loading;
        use_effect_with(is_loading_value, move |loading| {
            if !*loading {
                is_slow.set(false);
                return Box::new(|| ()) as Box<dyn FnOnce()>;
            }
            let timeout = Timeout::new(2_000, move || is_slow.set(true));
            Box::new(move || drop(timeout)) as Box<dyn FnOnce()>
        });
    }

    if *is_loading {
        return render_hero_loading(is_dark, *is_slow);
    }

    let recent_vec = (*recent).clone();
    let source: Vec<&BenchmarkReportLight> = recent_vec.iter().collect();
    let sweep = latest_sweep(&source);
    let mut stats = compute_stats(sweep.iter().copied());
    stats.showcase = pick_best_from_recent_batch(&source);

    if stats.total == 0 {
        return render_hero_loading(is_dark, true);
    }

    let display = stats
        .showcase
        .as_ref()
        .map(showcase_display)
        .unwrap_or_default();

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
        let navigator = navigator.clone();
        Callback::from(move |_: MouseEvent| {
            let navigator = navigator.clone();
            spawn_local(async move {
                if let Some(uuid) = fetch_latest_uuid().await {
                    navigate_to_benchmark(&navigator, uuid);
                }
            });
        })
    };

    html! {
        <div class="hero-v2">
            { render_background_grid() }
            <div class="hero-v2-inner">
                { render_headline(&display, &on_browse_click) }
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

fn render_headline(display: &ShowcaseDisplay, on_browse_click: &Callback<MouseEvent>) -> Html {
    html! {
        <div class="hero-v2-headline">
            <div class="hero-v2-eyebrow">{"Peak sustained throughput"}</div>
            <h1 class="hero-v2-title">
                <span class="hero-v2-big">{display.formatted_value.clone()}</span>
                <span class="hero-v2-unit">{display.unit}</span>
            </h1>
            <p class="hero-v2-sub">
                { render_hero_sub(&display.pretty_name, &display.cpu_name, display.gitref.as_deref()) }
            </p>
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

fn render_hero_sub(subject: &str, hardware: &str, gitref: Option<&str>) -> Html {
    let prefix = match (subject.is_empty(), hardware.is_empty()) {
        (true, true) => String::new(),
        (true, false) => hardware.to_string(),
        (false, true) => subject.to_string(),
        (false, false) => format!("{subject} · {hardware}"),
    };
    let has_prefix = !prefix.is_empty();
    let gitref = gitref.map(str::to_string);
    let gitref_owned = gitref.clone();

    html! {
        <>
            if has_prefix {
                <span>{prefix}</span>
            }
            if let Some(gitref) = gitref_owned {
                if has_prefix {
                    <span class="hero-v2-sub-sep">{" @ "}</span>
                }
                <a
                    class="hero-v2-sub-gitref"
                    href={iggy_gitref_url(&gitref)}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={format!("Browse apache/iggy at {gitref}")}
                >
                    {gitref}
                    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24"
                         fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                        <polyline points="15 3 21 3 21 9" />
                        <line x1="10" y1="14" x2="21" y2="3" />
                    </svg>
                </a>
            }
        </>
    }
}

fn iggy_gitref_url(gitref: &str) -> String {
    if crate::version::parse_semver_recency(gitref).is_some() {
        format!("https://github.com/apache/iggy/tree/server-{gitref}")
    } else {
        format!("https://github.com/apache/iggy/tree/{gitref}")
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
            { render_volume_card(3, stats.showcase.as_ref()) }
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

fn render_volume_card(stagger: usize, showcase: Option<&BenchmarkReportLight>) -> Html {
    let Some(benchmark) = showcase else {
        return html! {};
    };
    let total_bytes = benchmark.total_bytes();
    if total_bytes == 0 {
        return html! {};
    }
    let (value, unit) = format_volume(total_bytes);
    let messages = benchmark.total_messages_sent() + benchmark.total_messages_received();
    let sub = if messages > 0 {
        format!("{} messages moved", format_count(messages))
    } else {
        String::new()
    };
    html! {
        <div class="hero-v2-card" style={format!("--stagger: {stagger}")}>
            <div class="hero-v2-card-value-row">
                <span class="hero-v2-card-value">{value}</span>
                <span class="hero-v2-card-unit">{unit}</span>
            </div>
            <div class="hero-v2-card-label">{"Volume pushed in run"}</div>
            <div class="hero-v2-card-sub">{sub}</div>
        </div>
    }
}

fn format_volume(bytes: u64) -> (String, &'static str) {
    let bytes = bytes as f64;
    if bytes >= 1_000_000_000_000.0 {
        (format_significant(bytes / 1_000_000_000_000.0), "TB")
    } else if bytes >= 1_000_000_000.0 {
        (format_significant(bytes / 1_000_000_000.0), "GB")
    } else if bytes >= 1_000_000.0 {
        (format_significant(bytes / 1_000_000.0), "MB")
    } else {
        (format_significant(bytes / 1_000.0), "kB")
    }
}

fn format_count(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.2}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
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

fn render_hero_loading(is_dark: bool, is_slow: bool) -> Html {
    let logo_src = if is_dark {
        "/assets/iggy-light.svg"
    } else {
        "/assets/iggy-dark.svg"
    };
    html! {
        <div class="hero-v2 hero-v2-loading" aria-busy="true" aria-live="polite">
            { render_background_grid() }
            <div class="hero-v2-loading-inner">
                <img
                    class="hero-v2-loading-mark"
                    src={logo_src}
                    alt=""
                    aria-hidden="true"
                />
                <div class="hero-v2-loading-sub">{"Benchmarks"}</div>
                if is_slow {
                    <p class="hero-v2-loading-slow">
                        {"Fetching the latest benchmark run. This can take a moment on a cold cache."}
                    </p>
                }
                <span class="visually-hidden">{"Loading benchmarks"}</span>
            </div>
        </div>
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct ShowcaseDisplay {
    pub formatted_value: String,
    pub unit: &'static str,
    pub pretty_name: String,
    pub cpu_name: String,
    pub gitref: Option<String>,
}

pub fn showcase_display(showcase: &BenchmarkReportLight) -> ShowcaseDisplay {
    let throughput = showcase
        .group_metrics
        .first()
        .map(|metrics| metrics.summary.total_throughput_megabytes_per_second);
    let (formatted_value, unit) = match throughput {
        Some(value) => {
            let (formatted, unit) = format_throughput_bytes(value);
            (formatted, unit)
        }
        None => ("-".to_string(), "MB/s"),
    };
    let cpu_name = showcase.hardware.cpu_name.clone();
    let gitref = showcase
        .params
        .gitref
        .clone()
        .filter(|gitref| !gitref.is_empty());
    ShowcaseDisplay {
        formatted_value,
        unit,
        pretty_name: showcase.params.pretty_name.clone(),
        cpu_name,
        gitref,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::BenchmarkGroupMetricsLight;
    use bench_report::group_metrics_kind::GroupMetricsKind;
    use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;

    #[test]
    fn given_showcase_benchmark_when_building_display_should_copy_fields_verbatim() {
        let showcase = showcase_fixture(
            "pinned-producer 64P-1000B",
            "AMD Ryzen 9 7950X3D",
            Some("0.7.0-edge.3"),
            1_200.0,
        );

        let display = showcase_display(&showcase);

        assert_eq!(display.pretty_name, "pinned-producer 64P-1000B");
        assert_eq!(display.cpu_name, "AMD Ryzen 9 7950X3D");
        assert_eq!(display.gitref.as_deref(), Some("0.7.0-edge.3"));
        assert_eq!(display.formatted_value, "1.20");
        assert_eq!(display.unit, "GB/s");
    }

    #[test]
    fn given_showcase_benchmark_when_rendering_should_never_mix_fields_from_other_benchmarks() {
        let peak_throughput =
            showcase_fixture("peak-throughput-run", "Decoy CPU", Some("0.6.0"), 9_999.0);
        let lowest_p99 =
            showcase_fixture("lowest-p99-run", "Real CPU", Some("0.7.0-edge.1"), 800.0);

        // Hero always reads from THE SHOWCASE (lowest_p99), never from a
        // sibling like peak_throughput. Changing peak_throughput must not
        // affect the output of showcase_display(&lowest_p99).
        let _ignored = showcase_display(&peak_throughput);
        let display = showcase_display(&lowest_p99);

        assert_eq!(display.pretty_name, "lowest-p99-run");
        assert_eq!(display.cpu_name, "Real CPU");
        assert_eq!(display.gitref.as_deref(), Some("0.7.0-edge.1"));
        assert_ne!(display.pretty_name, "peak-throughput-run");
        assert_ne!(display.cpu_name, "Decoy CPU");
        assert_ne!(display.gitref.as_deref(), Some("0.6.0"));
    }

    #[test]
    fn given_showcase_without_gitref_when_building_display_should_drop_gitref() {
        let showcase = showcase_fixture("no-tag-run", "CPU", None, 100.0);
        let display = showcase_display(&showcase);
        assert_eq!(display.gitref, None);
    }

    #[test]
    fn given_showcase_without_metrics_when_building_display_should_return_dash_placeholder() {
        let mut showcase = showcase_fixture("empty", "CPU", Some("0.7.0"), 0.0);
        showcase.group_metrics.clear();
        let display = showcase_display(&showcase);
        assert_eq!(display.formatted_value, "-");
        assert_eq!(display.unit, "MB/s");
    }

    fn showcase_fixture(
        pretty_name: &str,
        cpu_name: &str,
        gitref: Option<&str>,
        throughput_mb_s: f64,
    ) -> BenchmarkReportLight {
        let mut report = BenchmarkReportLight::default();
        report.params.pretty_name = pretty_name.to_string();
        report.params.gitref = gitref.map(str::to_string);
        report.hardware.cpu_name = cpu_name.to_string();
        report.group_metrics.push(BenchmarkGroupMetricsLight {
            summary: BenchmarkGroupMetricsSummary {
                kind: GroupMetricsKind::Producers,
                total_throughput_megabytes_per_second: throughput_mb_s,
                total_throughput_messages_per_second: 0.0,
                average_throughput_megabytes_per_second: 0.0,
                average_throughput_messages_per_second: 0.0,
                average_p50_latency_ms: 0.0,
                average_p90_latency_ms: 0.0,
                average_p95_latency_ms: 0.0,
                average_p99_latency_ms: 0.0,
                average_p999_latency_ms: 0.0,
                average_p9999_latency_ms: 0.0,
                average_latency_ms: 0.0,
                average_median_latency_ms: 0.0,
                min_latency_ms: 0.0,
                max_latency_ms: 0.0,
                std_dev_latency_ms: 0.0,
            },
            latency_distribution: None,
        });
        report
    }
}
