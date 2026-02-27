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
use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::components::theme::theme_toggle::ThemeToggle;
use crate::components::tooltips::benchmark_info_toggle::BenchmarkInfoToggle;
use crate::components::tooltips::benchmark_info_tooltip::BenchmarkInfoTooltip;
use crate::components::tooltips::server_stats_toggle::ServerStatsToggle;
use crate::components::tooltips::server_stats_tooltip::ServerStatsTooltip;
use crate::state::benchmark::use_benchmark;
use crate::state::ui::{UiAction, use_ui};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct TopBarProps {
    pub is_dark: bool,
    pub selected_gitref: String,
    pub on_theme_toggle: Callback<bool>,
}

#[function_component(TopBar)]
pub fn topbar(props: &TopBarProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let selected_measurement = ui_state.selected_measurement.clone();
    let is_benchmark_tooltip_visible = ui_state.is_benchmark_tooltip_visible;
    let is_server_stats_tooltip_visible = ui_state.is_server_stats_tooltip_visible;
    let has_distribution = benchmark_ctx
        .state
        .selected_benchmark
        .as_ref()
        .is_some_and(|b| b.has_latency_distribution());

    {
        let ui_state = ui_state.clone();
        let selected_measurement = selected_measurement.clone();
        use_effect_with(
            (has_distribution, selected_measurement),
            move |(has_dist, measurement)| {
                if !has_dist && *measurement == MeasurementType::Distribution {
                    ui_state.dispatch(UiAction::SetMeasurementType(MeasurementType::Latency));
                }
            },
        );
    }

    let on_download_artifacts = {
        let benchmark_ctx = benchmark_ctx.clone();
        Callback::from(move |_| {
            if let Some(benchmark) = &benchmark_ctx.state.selected_benchmark {
                api::download_test_artifacts(&benchmark.uuid);
            }
        })
    };

    let on_server_stats_toggle = {
        let ui_state = ui_state.clone();
        Callback::from(move |_| {
            ui_state.dispatch(UiAction::ToggleServerStatsTooltip);
        })
    };

    let on_measurement_select = {
        let ui_state = ui_state.clone();
        Callback::from(move |mt: MeasurementType| {
            ui_state.dispatch(UiAction::SetMeasurementType(mt));
        })
    };

    let on_benchmark_tooltip_toggle = {
        let ui_state = ui_state.clone();
        Callback::from(move |_| {
            ui_state.dispatch(UiAction::ToggleBenchmarkTooltip);
        })
    };

    html! {
        <div class="top-buttons">
            <div class="controls">
                <ThemeToggle
                    is_dark={props.is_dark}
                    on_toggle={props.on_theme_toggle.clone()}
                />
                {
                    if !props.selected_gitref.is_empty() {
                        html! {
                            <>
                                <button
                                    class="download-button"
                                    onclick={on_download_artifacts.clone()}
                                    title="Download Test Artifacts"
                                    disabled={benchmark_ctx.state.selected_benchmark.is_none()}
                                >
                                    <svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                                        <polyline points="7 10 12 15 17 10"/>
                                        <line x1="12" y1="15" x2="12" y2="3"/>
                                    </svg>
                                </button>
                                <div class="info-container">
                                    <ServerStatsToggle
                                        is_visible={is_server_stats_tooltip_visible}
                                        on_toggle={on_server_stats_toggle.clone()}
                                    />
                                    {
                                        if is_server_stats_tooltip_visible {
                                            html! {
                                                <ServerStatsTooltip
                                                    benchmark_report={benchmark_ctx.state.selected_benchmark.clone()}
                                                    visible={true}
                                                    view_mode={ui_state.view_mode.clone()}
                                                />
                                            }
                                        } else {
                                            html! {}
                                        }
                                    }
                                </div>
                                <div class="info-container">
                                    <BenchmarkInfoToggle
                                        is_visible={is_benchmark_tooltip_visible}
                                        on_toggle={on_benchmark_tooltip_toggle.clone()}
                                    />
                                    {
                                        if is_benchmark_tooltip_visible && benchmark_ctx.state.selected_benchmark.is_some() {
                                            html! {
                                                <BenchmarkInfoTooltip
                                                    benchmark_report={benchmark_ctx.state.selected_benchmark.clone().unwrap()}
                                                    visible={true}
                                                    view_mode={ui_state.view_mode.clone()}
                                                />
                                            }
                                        } else {
                                            html! {}
                                        }
                                    }
                                </div>
                                <div class="measurement-buttons">
                                    <button
                                        class={classes!(
                                            "measurement-button",
                                            (selected_measurement == MeasurementType::Latency).then_some("active")
                                        )}
                                        onclick={on_measurement_select.reform(|_| MeasurementType::Latency)}
                                    >
                                        { "Latency" }
                                    </button>
                                    {
                                        if has_distribution {
                                            html! {
                                                <button
                                                    class={classes!(
                                                        "measurement-button",
                                                        (selected_measurement == MeasurementType::Distribution).then_some("active")
                                                    )}
                                                    onclick={on_measurement_select.reform(|_| MeasurementType::Distribution)}
                                                >
                                                    { "Distribution" }
                                                </button>
                                            }
                                        } else {
                                            html! {}
                                        }
                                    }
                                    <button
                                        class={classes!(
                                            "measurement-button",
                                            (selected_measurement == MeasurementType::Throughput).then_some("active")
                                        )}
                                        onclick={on_measurement_select.reform(|_| MeasurementType::Throughput)}
                                    >
                                        { "Throughput" }
                                    </button>
                                </div>
                            </>
                        }
                    } else {
                        html! {}
                    }
                }
            </div>
        </div>
    }
}
