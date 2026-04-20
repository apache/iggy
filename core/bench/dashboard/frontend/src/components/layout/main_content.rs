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
use crate::components::layout::hero::Hero;
use crate::components::layout::percentile_card::PercentileCard;
use crate::components::layout::sweep_view::SweepView;
use crate::components::layout::topbar::TopBar;
use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::state::benchmark::use_benchmark;
use crate::state::ui::{UiAction, ViewMode, use_ui};
use bench_dashboard_shared::BenchmarkReportLight;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct MainContentProps {
    pub selected_gitref: String,
    pub is_dark: bool,
    pub on_theme_toggle: Callback<bool>,
    pub view_mode: ViewMode,
}

#[function_component(MainContent)]
pub fn main_content(props: &MainContentProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui = use_ui();
    let is_recent_view = matches!(props.view_mode, ViewMode::RecentBenchmarks);
    let selected = benchmark_ctx.state.selected_benchmark.clone();
    let pinned = ui.compare_pin.as_deref().cloned();
    let entries = benchmark_ctx.state.entries.clone();

    let on_unpin = {
        let ui = ui.clone();
        Callback::from(move |_: MouseEvent| ui.dispatch(UiAction::SetComparePin(None)))
    };

    let show_hero = ui.is_landing && !is_recent_view;
    let content = if show_hero {
        html! {
            <div class="content-wrapper">
                <Hero selected_gitref={props.selected_gitref.clone()} />
            </div>
        }
    } else {
        match (selected.as_ref(), pinned.as_ref()) {
            (Some(selected_benchmark), Some(pinned_benchmark))
                if selected_benchmark.uuid != pinned_benchmark.uuid =>
            {
                render_compare(
                    selected_benchmark,
                    pinned_benchmark,
                    ui.selected_measurement.clone(),
                    props.is_dark,
                    on_unpin,
                )
            }
            (Some(selected_benchmark), _) => render_single(
                selected_benchmark,
                ui.selected_measurement.clone(),
                props.is_dark,
                &entries,
            ),
            (None, _) if is_recent_view => render_empty_recent(),
            (None, _) => html! {
                <div class="content-wrapper">
                    <Hero selected_gitref={props.selected_gitref.clone()} />
                </div>
            },
        }
    };

    html! {
        <div class="content">
            if !show_hero {
                <TopBar
                    is_dark={props.is_dark}
                    selected_gitref={props.selected_gitref.clone()}
                    on_theme_toggle={props.on_theme_toggle.clone()}
                />
            }
            {content}
        </div>
    }
}

fn render_single(
    benchmark: &BenchmarkReportLight,
    measurement: MeasurementType,
    is_dark: bool,
    entries: &std::collections::BTreeMap<
        bench_report::benchmark_kind::BenchmarkKind,
        Vec<BenchmarkReportLight>,
    >,
) -> Html {
    html! {
        <div class="content-wrapper">
            <div class="chart-title">
                <div class="chart-title-primary">
                    { benchmark.title(&measurement.to_string()) }
                </div>
                <div class="chart-title-sub">{ benchmark.subtext() }</div>
                <div class="chart-title-identifier">
                    { benchmark.identifier_with_cpu_and_version() }
                </div>
            </div>
            <PercentileCard benchmark={benchmark.clone()} />
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
) -> Html {
    html! {
        <div class="content-wrapper compare-wrapper">
            <div class="compare-banner">
                <span class="compare-banner-label">{"Compare mode"}</span>
                <button type="button" class="compare-banner-unpin" onclick={on_unpin}>
                    {"Unpin"}
                </button>
            </div>
            <div class="compare-grid">
                { render_compare_pane("A - selected", selected_benchmark, measurement.clone(), is_dark) }
                { render_compare_pane("B - pinned", pinned_benchmark, measurement, is_dark) }
            </div>
        </div>
    }
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
                <div class="chart-title-sub">{ benchmark.subtext() }</div>
                <div class="chart-title-identifier">
                    { benchmark.identifier_with_cpu_and_version() }
                </div>
            </div>
            <PercentileCard benchmark={benchmark.clone()} />
            <div class="single-view">
                { render_measurement_chart(benchmark, measurement, is_dark) }
            </div>
        </div>
    }
}

fn render_empty_recent() -> Html {
    html! {
        <div class="content-wrapper">
            <div class="empty-state">
                <div class="empty-state-content">
                    <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
                        <polyline points="13 2 13 9 20 9"/>
                        <line x1="16" y1="13" x2="8" y2="13"/>
                        <line x1="16" y1="17" x2="8" y2="17"/>
                    </svg>
                    <h2>{"Select a recent benchmark"}</h2>
                    <p>{"Choose a benchmark from the sidebar to display performance data."}</p>
                </div>
            </div>
        </div>
    }
}
