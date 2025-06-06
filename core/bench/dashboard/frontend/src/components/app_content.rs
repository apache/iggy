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

use crate::{
    api,
    components::layout::{main_content::MainContent, sidebar::Sidebar},
    router::AppRoute,
    state::{
        benchmark::{BenchmarkAction, BenchmarkContext, use_benchmark},
        gitref::{GitrefAction, GitrefContext, use_gitref},
        hardware::{HardwareAction, HardwareContext, use_hardware},
        ui::use_ui,
    },
};
use bench_report::hardware::BenchmarkHardware;
use gloo::console::log;
use yew::prelude::*;
use yew_router::hooks::use_route;
use yew_router::prelude::use_navigator;

// Props definitions
#[derive(Properties, PartialEq)]
pub struct AppContentProps {}

#[hook]
fn use_init_hardware(
    hardware_ctx: HardwareContext,
    route: Option<AppRoute>,
    is_loading_from_url: bool,
) {
    use gloo::console::log;

    use_effect_with(
        (route, is_loading_from_url),
        move |(route, is_loading_from_url)| {
            let dispatch = hardware_ctx.dispatch.clone();
            let already_selected = hardware_ctx.state.selected_hardware.clone();
            let current_route = route.clone();
            let loading_from_url = *is_loading_from_url;

            yew::platform::spawn_local(async move {
                match api::fetch_hardware_configurations().await {
                    Ok(mut hw_list) => {
                        if !hw_list.is_empty() {
                            hw_list.sort_by(|a, b| a.identifier.cmp(&b.identifier));
                            dispatch.emit(HardwareAction::SetHardwareList(hw_list.clone()));

                            if already_selected.is_none() && !loading_from_url {
                                if let Some(AppRoute::Benchmark { .. }) = current_route {
                                    // Do nothing, app_content will handle selection for this route
                                } else if let Some(preferred) =
                                    preferred_hardware_identifier(&hw_list)
                                {
                                    dispatch.emit(HardwareAction::SelectHardware(Some(preferred)));
                                } else {
                                    log!("No preferred hardware found");
                                }
                            }
                        }
                    }
                    Err(e) => log!(format!("Error fetching hardware: {}", e)),
                }
            });
            || ()
        },
    );
}

fn preferred_hardware_identifier(hw_list: &[BenchmarkHardware]) -> Option<String> {
    hw_list
        .iter()
        .filter_map(|hw| {
            hw.identifier
                .clone()
                .filter(|identifier| identifier == "spetz-amd-rkyv")
        })
        .next()
}

#[hook]
fn use_load_gitrefs(
    gitref_ctx: GitrefContext,
    hardware: Option<String>,
    route: Option<AppRoute>,
    is_loading_from_url: bool,
) {
    use gloo::console::log;

    use_effect_with(
        (hardware.clone(), route.clone(), is_loading_from_url),
        move |(hardware_dep, route_dep, is_loading_dep)| {
            let gitref_ctx_effect = gitref_ctx.clone();
            let hardware_val = hardware_dep.clone();
            let current_route_val = route_dep.clone();
            let loading_from_url_val = *is_loading_dep;

            if let Some(hw) = hardware_val {
                yew::platform::spawn_local(async move {
                    match api::fetch_gitrefs_for_hardware(&hw).await {
                        Ok(vers) => {
                            gitref_ctx_effect
                                .dispatch
                                .emit(GitrefAction::SetGitrefs(vers.clone()));
                            if !vers.is_empty() {
                                let current_selected_gitref_val =
                                    gitref_ctx_effect.state.selected_gitref.clone();

                                log!(format!(
                                    "use_load_gitrefs: Pre-auto-selection check. loading_from_url: {}, current_route: {:?}, has_versions: {}, current_selected_gitref: {:?}",
                                    loading_from_url_val,
                                    current_route_val,
                                    !vers.is_empty(),
                                    current_selected_gitref_val
                                ));

                                if !loading_from_url_val {
                                    if let Some(AppRoute::Benchmark { .. }) = current_route_val {
                                        log!(
                                            "use_load_gitrefs: On Benchmark route, skipping auto gitref selection."
                                        );
                                    } else {
                                        log!(
                                            "use_load_gitrefs: Not on Benchmark route, proceeding with auto gitref selection logic."
                                        );
                                        let final_gitref = match current_selected_gitref_val {
                                            Some(ref existing) if vers.contains(existing) => {
                                                log!(format!(
                                                    "use_load_gitrefs: Retaining existing gitref: {}",
                                                    existing
                                                ));
                                                existing.clone()
                                            }
                                            _ => {
                                                log!(format!(
                                                    "use_load_gitrefs: Selecting first available gitref: {}",
                                                    vers[0]
                                                ));
                                                vers[0].clone()
                                            }
                                        };

                                        if Some(final_gitref.clone())
                                            != gitref_ctx_effect.state.selected_gitref
                                        {
                                            log!(format!(
                                                "use_load_gitrefs: Dispatching SetSelectedGitref with: {}",
                                                final_gitref
                                            ));
                                            gitref_ctx_effect.dispatch.emit(
                                                GitrefAction::SetSelectedGitref(Some(final_gitref)),
                                            );
                                        } else {
                                            log!(format!(
                                                "use_load_gitrefs: Gitref {} already selected or matches current state.",
                                                final_gitref
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => log!(format!("Error fetching gitrefs: {}", e)),
                    }
                });
            }
            || ()
        },
    );
}

#[hook]
fn use_load_benchmarks(
    benchmark_ctx: BenchmarkContext,
    hardware: Option<String>,
    gitref: Option<String>,
) {
    use gloo::console::log;
    use_effect_with(
        (hardware.clone(), gitref.clone()),
        move |(hardware_dep, gitref_dep)| {
            let benchmark_ctx_effect = benchmark_ctx.clone();
            if let (Some(hw), Some(gr)) = (hardware_dep.clone(), gitref_dep.clone()) {
                log!(format!(
                    "use_load_benchmarks: Triggered with HW: {:?}, GitRef: {:?}",
                    hw, gr
                ));
                let hw_clone = hw.clone(); // Clone for async move
                let gr_clone = gr.clone(); // Clone for async move
                yew::platform::spawn_local(async move {
                    match api::fetch_benchmarks_for_hardware_and_gitref(&hw_clone, &gr_clone).await
                    {
                        Ok(benchmarks) => {
                            log!(format!(
                                "use_load_benchmarks: Fetched {} benchmarks for {} & {}",
                                benchmarks.len(),
                                hw_clone,
                                gr_clone
                            ));
                            benchmark_ctx_effect.dispatch.emit(
                                BenchmarkAction::SetBenchmarksForGitref(
                                    benchmarks, hw_clone, gr_clone,
                                ),
                            );
                        }
                        Err(e) => log!(format!(
                            "use_load_benchmarks: Error fetching benchmarks: {}",
                            e
                        )),
                    }
                });
            } else {
                log!(format!(
                    "use_load_benchmarks: Not fetching. HW: {:?}, GitRef: {:?}",
                    hardware_dep, gitref_dep
                ));
                // Do nothing if hardware or gitref is None, allowing state to persist
                // until both are available. This avoids clearing entries transiently.
            }
            || ()
        },
    );
}

#[function_component(AppContent)]
pub fn app_content() -> Html {
    let ui_state = use_ui();
    let benchmark_ctx = use_benchmark();
    let gitref_ctx = use_gitref();
    let hardware_ctx = use_hardware();
    let route = use_route::<AppRoute>();
    let navigator = use_navigator();

    // Get theme context from the provider
    let (is_dark, theme_toggle) =
        use_context::<(bool, Callback<()>)>().expect("Theme context not found");

    let is_loading_from_url = use_state(|| false);

    use_init_hardware(hardware_ctx.clone(), route.clone(), *is_loading_from_url);

    {
        let route_clone = route.clone();
        let hardware_ctx_cloned = hardware_ctx.clone();
        let gitref_ctx_cloned = gitref_ctx.clone();
        let benchmark_ctx_cloned = benchmark_ctx.clone();
        let is_loading_from_url_handle = is_loading_from_url.clone();

        use_effect_with(route_clone, move |current_route| {
            if let Some(AppRoute::Benchmark { uuid }) = current_route {
                let uuid_val = uuid.clone();
                log!(format!(
                    "AppContent: Processing benchmark UUID from URL: {}",
                    uuid_val
                ));
                is_loading_from_url_handle.set(true);

                let hardware_ctx_effect = hardware_ctx_cloned.clone();
                let gitref_ctx_effect = gitref_ctx_cloned.clone();
                let benchmark_ctx_effect = benchmark_ctx_cloned.clone();
                let is_loading_setter_effect = is_loading_from_url_handle.clone();

                yew::platform::spawn_local(async move {
                    match api::fetch_benchmark_by_uuid(&uuid_val).await {
                        Ok(target_bm_light) => {
                            log!(format!(
                                "AppContent: URL Benchmark Details Fetched: UUID={}, Hardware={:?}, GitRef={:?}",
                                uuid_val,
                                target_bm_light.hardware.identifier,
                                target_bm_light.params.gitref
                            ));

                            if hardware_ctx_effect.state.selected_hardware.as_ref()
                                != target_bm_light.hardware.identifier.as_ref()
                            {
                                hardware_ctx_effect
                                    .dispatch
                                    .emit(HardwareAction::SelectHardware(
                                        target_bm_light.hardware.identifier.clone(),
                                    ));
                            }

                            if gitref_ctx_effect.state.selected_gitref.as_ref()
                                != target_bm_light.params.gitref.as_ref()
                            {
                                gitref_ctx_effect
                                    .dispatch
                                    .emit(GitrefAction::SetSelectedGitref(
                                        target_bm_light.params.gitref.clone(),
                                    ));
                            }

                            benchmark_ctx_effect
                                .dispatch
                                .emit(BenchmarkAction::SelectBenchmark(Box::new(Some(
                                    target_bm_light,
                                ))));

                            is_loading_setter_effect.set(false);
                        }
                        Err(e) => {
                            log!(format!(
                                "AppContent: Error fetching benchmark {} for URL init: {}",
                                uuid_val, e
                            ));
                            is_loading_setter_effect.set(false);
                        }
                    }
                });
            } else if *is_loading_from_url_handle {
                is_loading_from_url_handle.set(false);
            }
            || ()
        });
    }

    use_load_gitrefs(
        gitref_ctx.clone(),
        hardware_ctx.state.selected_hardware.clone(),
        route.clone(),
        *is_loading_from_url,
    );

    use_load_benchmarks(
        benchmark_ctx.clone(),
        hardware_ctx.state.selected_hardware.clone(),
        gitref_ctx.state.selected_gitref.clone(),
    );

    html! {
        <div class="container">
            <Sidebar
                on_gitref_select={Callback::from(move |gitref: String| {
                    // When gitref is selected, we might also want to navigate to Home with new query params
                    // For now, let existing logic handle it. If issues persist, revisit.
                    gitref_ctx.dispatch.emit(GitrefAction::SetSelectedGitref(Some(gitref)));
                })}
                on_hardware_select={Callback::from(move |hw: String| {
                    let navigator_clone = navigator.clone();
                    hardware_ctx.dispatch.emit(HardwareAction::SelectHardware(Some(hw.clone())));
                    // Navigate to Home to reset the route context from a specific benchmark URL
                    // This allows use_load_gitrefs to correctly auto-select a gitref
                    if let Some(nav) = navigator_clone {
                        log!(format!("Navigating to Home due to manual hardware selection: {}", hw));
                        nav.push(&AppRoute::Home);
                    } else {
                        log!("Navigator not available for hardware selection navigation");
                    }
                })}
            />
            <MainContent
                selected_gitref={gitref_ctx.state.selected_gitref.clone().unwrap_or_default()}
                is_dark={is_dark}
                on_theme_toggle={Callback::from(move |_: bool| {
                    theme_toggle.emit(());
                })}
                view_mode={ui_state.view_mode.clone()}
            />
        </div>
    }
}
