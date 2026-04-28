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
use crate::components::embed_modal::EmbedModal;
use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::components::tooltips::benchmark_info_toggle::BenchmarkInfoToggle;
use crate::components::tooltips::benchmark_info_tooltip::BenchmarkInfoTooltip;
use crate::components::tooltips::server_stats_toggle::ServerStatsToggle;
use crate::components::tooltips::server_stats_tooltip::ServerStatsTooltip;
use crate::router::AppRoute;
use crate::state::benchmark::{BenchmarkAction, use_benchmark};
use crate::state::ui::{TopBarPopup, UiAction, use_ui};
use gloo::console::log;
use gloo::timers::callback::Timeout;
use yew::platform::spawn_local;
use yew::prelude::*;
use yew_router::prelude::{Navigator, use_navigator};

#[derive(Properties, PartialEq)]
pub struct TopAppBarProps {
    pub show_sidebar_toggle: bool,
    pub show_detail_actions: bool,
}

#[function_component(TopAppBar)]
pub fn top_app_bar(props: &TopAppBarProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let ui = use_ui();
    let navigator = use_navigator();
    let (is_dark, toggle_theme) =
        use_context::<(bool, Callback<()>)>().expect("Theme context not found");

    let selected_benchmark = benchmark_ctx.state.selected_benchmark.clone();
    let selected_measurement = ui.selected_measurement.clone();
    let has_distribution = selected_benchmark
        .as_ref()
        .is_some_and(|benchmark| benchmark.has_latency_distribution());

    {
        let ui_handle = ui.clone();
        let current_measurement = selected_measurement.clone();
        use_effect_with(
            (has_distribution, current_measurement),
            move |(has_dist, measurement)| {
                if !has_dist && *measurement == MeasurementType::Distribution {
                    ui_handle.dispatch(UiAction::SetMeasurementType(MeasurementType::Latency));
                }
            },
        );
    }

    {
        let ui_handle = ui.clone();
        let selected_uuid = selected_benchmark.as_ref().map(|benchmark| benchmark.uuid);
        use_effect_with(selected_uuid, move |_| {
            ui_handle.dispatch(UiAction::CloseAllPopups);
        });
    }

    let on_home = {
        let benchmark_ctx = benchmark_ctx.clone();
        let ui = ui.clone();
        let navigator = navigator.clone();
        Callback::from(move |_| {
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmark(Box::new(None)));
            ui.dispatch(UiAction::SetComparePin(Box::new(None)));
            if let Some(nav) = navigator.as_ref() {
                nav.push(&AppRoute::Home);
            }
        })
    };

    let on_sidebar_toggle = {
        let ui = ui.clone();
        Callback::from(move |_| ui.dispatch(UiAction::ToggleSidebar))
    };

    let on_theme_click = {
        let toggle_theme = toggle_theme.clone();
        Callback::from(move |_| toggle_theme.emit(()))
    };

    let on_measurement_select = {
        let ui = ui.clone();
        Callback::from(move |measurement: MeasurementType| {
            ui.dispatch(UiAction::SetMeasurementType(measurement));
        })
    };

    let on_download_artifacts = {
        let selected_benchmark = selected_benchmark.clone();
        Callback::from(move |_| {
            if let Some(benchmark) = &selected_benchmark {
                api::download_test_artifacts(&benchmark.uuid);
            }
        })
    };

    let share_copied = use_state(|| false);
    let on_share = {
        let share_copied = share_copied.clone();
        Callback::from(move |_| {
            let Some(window) = web_sys::window() else {
                return;
            };
            let url = window.location().href().unwrap_or_else(|_| String::new());
            if url.is_empty() {
                return;
            }
            let clipboard = window.navigator().clipboard();
            let _ = clipboard.write_text(&url);
            share_copied.set(true);
            let share_copied_for_timer = share_copied.clone();
            let timeout = Timeout::new(1_400, move || share_copied_for_timer.set(false));
            timeout.forget();
        })
    };

    let on_embed_toggle = {
        let ui = ui.clone();
        Callback::from(move |_| ui.dispatch(UiAction::TogglePopup(TopBarPopup::Embed)))
    };

    let on_server_stats_toggle = {
        let ui = ui.clone();
        Callback::from(move |_| ui.dispatch(UiAction::TogglePopup(TopBarPopup::ServerStats)))
    };

    let on_benchmark_tooltip_toggle = {
        let ui = ui.clone();
        Callback::from(move |_| ui.dispatch(UiAction::TogglePopup(TopBarPopup::BenchmarkInfo)))
    };

    let on_browse_click = {
        let navigator = navigator.clone();
        Callback::from(move |_| {
            let navigator = navigator.clone();
            spawn_local(async move {
                if let Some(uuid) = fetch_latest_uuid().await {
                    navigate_to_benchmark(&navigator, uuid);
                }
            });
        })
    };

    let is_collapsed = ui.is_sidebar_collapsed;
    let sidebar_toggle_title = if is_collapsed {
        "Show sidebar"
    } else {
        "Hide sidebar"
    };
    let theme_title = if is_dark {
        "Switch to light theme"
    } else {
        "Switch to dark theme"
    };
    let logo_src = if is_dark {
        "/assets/iggy-light.svg"
    } else {
        "/assets/iggy-dark.svg"
    };

    html! {
        <header class="app-bar">
            <div class="app-bar-left">
                if props.show_sidebar_toggle {
                    <button
                        type="button"
                        class={classes!("app-bar-icon-btn", is_collapsed.then_some("active"))}
                        onclick={on_sidebar_toggle}
                        title={sidebar_toggle_title}
                        aria-label={sidebar_toggle_title}
                    >
                        { render_chevron_icon(is_collapsed) }
                    </button>
                }
                <button
                    type="button"
                    class="app-bar-brand"
                    onclick={on_home}
                    title="Back to overview"
                >
                    <img src={logo_src} alt="Apache Iggy" />
                    <span class="app-bar-brand-text">{"Iggy Benchmarks"}</span>
                </button>
            </div>

            <div class="app-bar-center">
                if props.show_detail_actions && selected_benchmark.is_some() {
                    { render_measurement_tabs(&selected_measurement, has_distribution, &on_measurement_select) }
                }
            </div>

            <div class="app-bar-right">
                if !props.show_detail_actions {
                    <button
                        type="button"
                        class="app-bar-text-btn"
                        onclick={on_browse_click}
                        title="Browse the most recent benchmark"
                    >
                        {"Browse"}
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
                             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <line x1="5" y1="12" x2="19" y2="12" />
                            <polyline points="12 5 19 12 12 19" />
                        </svg>
                    </button>
                }
                if props.show_detail_actions && selected_benchmark.is_some() {
                    <div class="app-bar-icon-wrap">
                        <button
                            type="button"
                            class={classes!("app-bar-icon-btn", (*share_copied).then_some("active"))}
                            onclick={on_share}
                            title={if *share_copied { "Link copied" } else { "Copy share link" }}
                            aria-label="Copy share link"
                        >
                            { render_share_icon(*share_copied) }
                        </button>
                        if *share_copied {
                            <span class="app-bar-toast" role="status">{"Link copied"}</span>
                        }
                    </div>
                    <button
                        type="button"
                        class="app-bar-icon-btn mobile-hide"
                        onclick={on_download_artifacts}
                        title="Download test artifacts"
                        aria-label="Download test artifacts"
                    >
                        { render_download_icon() }
                    </button>
                    <div class="app-bar-icon-wrap mobile-hide">
                        <button
                            type="button"
                            class={classes!("app-bar-icon-btn", ui.is_embed_modal_visible.then_some("active"))}
                            onclick={on_embed_toggle}
                            title="Embed chart"
                            aria-label="Embed chart"
                        >
                            { render_embed_icon() }
                        </button>
                        if ui.is_embed_modal_visible
                            && let Some(benchmark) = selected_benchmark.as_ref()
                        {
                            <EmbedModal
                                uuid={benchmark.uuid.to_string()}
                                measurement_type={selected_measurement.clone()}
                                is_dark={is_dark}
                            />
                        }
                    </div>
                    <div class="app-bar-icon-wrap mobile-hide">
                        <ServerStatsToggle
                            is_visible={ui.is_server_stats_tooltip_visible}
                            on_toggle={on_server_stats_toggle}
                        />
                        if ui.is_server_stats_tooltip_visible
                            && selected_benchmark.is_some()
                        {
                            <ServerStatsTooltip
                                benchmark_report={selected_benchmark.clone()}
                                visible={true}
                            />
                        }
                    </div>
                    <div class="app-bar-icon-wrap mobile-hide">
                        <BenchmarkInfoToggle
                            is_visible={ui.is_benchmark_tooltip_visible}
                            on_toggle={on_benchmark_tooltip_toggle}
                        />
                        if ui.is_benchmark_tooltip_visible
                            && let Some(benchmark) = selected_benchmark.as_ref()
                        {
                            <BenchmarkInfoTooltip
                                benchmark_report={benchmark.clone()}
                                visible={true}
                            />
                        }
                    </div>
                }
                <button
                    type="button"
                    class="app-bar-icon-btn"
                    onclick={on_theme_click}
                    title={theme_title}
                    aria-label={theme_title}
                >
                    { render_theme_icon(is_dark) }
                </button>
            </div>
        </header>
    }
}

fn render_measurement_tabs(
    selected: &MeasurementType,
    has_distribution: bool,
    on_select: &Callback<MeasurementType>,
) -> Html {
    let entries: Vec<(MeasurementType, &'static str)> = {
        let mut list = vec![
            (MeasurementType::Latency, "Latency"),
            (MeasurementType::Throughput, "Throughput"),
        ];
        if has_distribution {
            list.push((MeasurementType::Distribution, "Distribution"));
        }
        list.push((MeasurementType::Tail, "Tail"));
        list
    };

    html! {
        <div class="app-bar-tabs" role="tablist">
            { for entries.into_iter().map(|(measurement, label)| {
                let is_active = &measurement == selected;
                let on_click = {
                    let on_select = on_select.clone();
                    let measurement = measurement.clone();
                    Callback::from(move |_| on_select.emit(measurement.clone()))
                };
                html! {
                    <button
                        type="button"
                        role="tab"
                        aria-selected={is_active.to_string()}
                        class={classes!("app-bar-tab", is_active.then_some("active"))}
                        onclick={on_click}
                    >
                        {label}
                    </button>
                }
            })}
        </div>
    }
}

fn render_chevron_icon(is_collapsed: bool) -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            {
                if is_collapsed {
                    html! { <polyline points="9 18 15 12 9 6" /> }
                } else {
                    html! { <polyline points="15 18 9 12 15 6" /> }
                }
            }
        </svg>
    }
}

fn render_download_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <polyline points="7 10 12 15 17 10" />
            <line x1="12" y1="15" x2="12" y2="3" />
        </svg>
    }
}

fn render_share_icon(copied: bool) -> Html {
    if copied {
        return html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">
                <polyline points="20 6 9 17 4 12" />
            </svg>
        };
    }
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="18" cy="5" r="3" />
            <circle cx="6" cy="12" r="3" />
            <circle cx="18" cy="19" r="3" />
            <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
            <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
        </svg>
    }
}

fn render_embed_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="16 18 22 12 16 6" />
            <polyline points="8 6 2 12 8 18" />
        </svg>
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

fn render_theme_icon(is_dark: bool) -> Html {
    if is_dark {
        html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
            </svg>
        }
    } else {
        html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="5" />
                <line x1="12" y1="1" x2="12" y2="3" />
                <line x1="12" y1="21" x2="12" y2="23" />
                <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
                <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
                <line x1="1" y1="12" x2="3" y2="12" />
                <line x1="21" y1="12" x2="23" y2="12" />
                <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
                <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
            </svg>
        }
    }
}
