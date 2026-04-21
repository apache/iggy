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

use crate::components::chart::single_chart::SingleChart;
use crate::components::chart::tail_chart::TailChart;
use crate::components::layout::benchmark_meta::BenchmarkMeta;
use crate::components::layout::sweep_view::SweepView;
use crate::components::loader::{IggyLoader, LoaderSize};
use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::router::AppRoute;
use crate::state::benchmark::use_benchmark;
use crate::state::ui::{UiAction, use_ui};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::benchmark_kind::BenchmarkKind;
use std::collections::BTreeMap;
use yew::prelude::*;
use yew_router::prelude::use_navigator;

#[derive(Properties, PartialEq, Default)]
pub struct MainContentProps;

#[function_component(MainContent)]
pub fn main_content(_props: &MainContentProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui = use_ui();
    let selected = benchmark_ctx.state.selected_benchmark.clone();
    let pinned = ui.compare_pin.clone();
    let entries = benchmark_ctx.state.entries.clone();
    let (is_dark, _) = use_context::<(bool, Callback<()>)>().expect("Theme context not found");

    let navigator = use_navigator();
    let on_unpin = {
        let navigator = navigator.clone();
        let selected_clone = selected.clone();
        let ui = ui.clone();
        Callback::from(move |_: MouseEvent| {
            if let (Some(selected_benchmark), Some(nav)) =
                (selected_clone.as_ref(), navigator.as_ref())
            {
                nav.push(&AppRoute::Benchmark {
                    uuid: selected_benchmark.uuid.to_string(),
                });
            } else {
                ui.dispatch(UiAction::SetComparePin(Box::new(None)));
            }
        })
    };

    let on_swap = {
        let navigator = navigator.clone();
        let selected_clone = selected.clone();
        let pinned_clone = pinned.clone();
        Callback::from(move |_: MouseEvent| {
            if let (Some(selected_benchmark), Some(pinned_benchmark), Some(nav)) = (
                selected_clone.as_ref(),
                pinned_clone.as_ref(),
                navigator.as_ref(),
            ) {
                nav.push(&AppRoute::Compare {
                    left: pinned_benchmark.uuid.to_string(),
                    right: selected_benchmark.uuid.to_string(),
                });
            }
        })
    };

    let content = match (selected.as_ref(), pinned.as_ref()) {
        (Some(selected_benchmark), Some(pinned_benchmark))
            if selected_benchmark.uuid != pinned_benchmark.uuid =>
        {
            render_compare(
                selected_benchmark,
                pinned_benchmark,
                ui.selected_measurement.clone(),
                is_dark,
                on_unpin,
                on_swap,
            )
        }
        (Some(selected_benchmark), _) => render_single(
            selected_benchmark,
            ui.selected_measurement.clone(),
            is_dark,
            &entries,
        ),
        (None, _) => render_loading(),
    };

    html! { <main class="main-content">{content}</main> }
}

fn render_single(
    benchmark: &BenchmarkReportLight,
    measurement: MeasurementType,
    is_dark: bool,
    entries: &BTreeMap<BenchmarkKind, Vec<BenchmarkReportLight>>,
) -> Html {
    html! {
        <div class="content-wrapper">
            <div class="chart-title">
                <div class="chart-title-primary">
                    { benchmark.title(&measurement.to_string()) }
                </div>
                <div class="chart-title-identifier">
                    { render_benchmark_identifier(benchmark) }
                </div>
            </div>
            <BenchmarkMeta benchmark={benchmark.clone()} />
            <div class="single-view">
                { render_measurement_chart(benchmark, measurement, is_dark) }
            </div>
            <SweepView benchmark={benchmark.clone()} entries={entries.clone()} is_dark={is_dark} />
        </div>
    }
}

fn render_measurement_chart(
    benchmark: &BenchmarkReportLight,
    measurement: MeasurementType,
    is_dark: bool,
) -> Html {
    if measurement == MeasurementType::Tail {
        return html! { <TailChart benchmark={benchmark.clone()} /> };
    }
    html! {
        <SingleChart
            benchmark_uuid={benchmark.uuid}
            measurement_type={measurement}
            is_dark={is_dark}
        />
    }
}

fn render_compare(
    selected_benchmark: &BenchmarkReportLight,
    pinned_benchmark: &BenchmarkReportLight,
    measurement: MeasurementType,
    is_dark: bool,
    on_unpin: Callback<MouseEvent>,
    on_swap: Callback<MouseEvent>,
) -> Html {
    html! {
        <div class="content-wrapper compare-wrapper">
            <div class="compare-banner">
                <span class="compare-banner-badge">{"Compare"}</span>
                <div class="compare-banner-names">
                    <span class="compare-banner-name a">
                        <span class="compare-banner-letter">{"A"}</span>
                        {short_name(selected_benchmark)}
                    </span>
                    <button type="button" class="compare-banner-swap" onclick={on_swap}
                            title="Swap A and B">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14"
                             viewBox="0 0 24 24" fill="none" stroke="currentColor"
                             stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <polyline points="17 1 21 5 17 9" />
                            <path d="M3 11V9a4 4 0 0 1 4-4h14" />
                            <polyline points="7 23 3 19 7 15" />
                            <path d="M21 13v2a4 4 0 0 1-4 4H3" />
                        </svg>
                    </button>
                    <span class="compare-banner-name b">
                        <span class="compare-banner-letter">{"B"}</span>
                        {short_name(pinned_benchmark)}
                    </span>
                </div>
                <button type="button" class="compare-banner-unpin" onclick={on_unpin}
                        title="Exit compare mode">
                    {"Exit"}
                </button>
            </div>
            <div class="compare-grid">
                { render_compare_pane("A", selected_benchmark, measurement.clone(), is_dark) }
                { render_compare_pane("B", pinned_benchmark, measurement, is_dark) }
            </div>
        </div>
    }
}

fn short_name(benchmark: &BenchmarkReportLight) -> String {
    let full = &benchmark.params.pretty_name;
    full.split('(').next().unwrap_or(full).trim().to_string()
}

fn render_compare_pane(
    label: &str,
    benchmark: &BenchmarkReportLight,
    measurement: MeasurementType,
    is_dark: bool,
) -> Html {
    html! {
        <div class="compare-pane">
            <div class="compare-pane-label">{label}</div>
            <div class="chart-title">
                <div class="chart-title-primary">
                    { benchmark.title(&measurement.to_string()) }
                </div>
                <div class="chart-title-identifier">
                    { render_benchmark_identifier(benchmark) }
                </div>
            </div>
            <BenchmarkMeta benchmark={benchmark.clone()} />
            <div class="single-view">
                { render_measurement_chart(benchmark, measurement, is_dark) }
            </div>
        </div>
    }
}

fn render_loading() -> Html {
    html! {
        <div class="content-wrapper">
            <div class="empty-state">
                <IggyLoader
                    size={LoaderSize::Large}
                    label={AttrValue::from("Loading the benchmark...")}
                />
            </div>
        </div>
    }
}

fn iggy_gitref_url(gitref: &str) -> String {
    if crate::version::parse_semver_recency(gitref).is_some() {
        format!("https://github.com/apache/iggy/tree/server-{gitref}")
    } else {
        format!("https://github.com/apache/iggy/tree/{gitref}")
    }
}

fn render_benchmark_identifier(benchmark: &BenchmarkReportLight) -> Html {
    let cpu = benchmark.hardware.cpu_name.as_str();
    let gitref = benchmark
        .params
        .gitref
        .as_deref()
        .filter(|gitref| !gitref.is_empty());

    html! {
        <>
            <span>{cpu.to_string()}</span>
            if let Some(gitref) = gitref {
                <span class="chart-title-sep">{"·"}</span>
                <a
                    class="chart-title-gitref"
                    href={iggy_gitref_url(gitref)}
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
