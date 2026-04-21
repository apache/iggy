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
    components::layout::{
        hero::Hero, main_content::MainContent, sidebar::Sidebar, top_app_bar::TopAppBar,
    },
    router::AppRoute,
    state::{
        benchmark::{BenchmarkAction, BenchmarkContext, use_benchmark},
        gitref::{GitrefAction, GitrefContext, use_gitref},
        hardware::{HardwareAction, HardwareContext, use_hardware},
        ui::{UiAction, use_ui},
    },
};
use bench_report::hardware::BenchmarkHardware;
use gloo::console::log;
use std::cell::Cell;
use std::rc::Rc;
use yew::prelude::*;
use yew_router::hooks::use_route;
use yew_router::prelude::use_navigator;

const MOBILE_BREAKPOINT_PX: f64 = 768.0;

fn is_mobile_viewport() -> bool {
    web_sys::window()
        .and_then(|window| window.inner_width().ok())
        .and_then(|value| value.as_f64())
        .is_some_and(|width| width < MOBILE_BREAKPOINT_PX)
}

async fn apply_benchmark(
    uuid: &str,
    hardware_ctx: &HardwareContext,
    gitref_ctx: &GitrefContext,
    benchmark_ctx: &BenchmarkContext,
) {
    match api::fetch_benchmark_by_uuid(uuid).await {
        Ok(target_benchmark) => {
            if hardware_ctx.state.selected_hardware.as_ref()
                != target_benchmark.hardware.identifier.as_ref()
            {
                hardware_ctx.dispatch.emit(HardwareAction::SelectHardware(
                    target_benchmark.hardware.identifier.clone(),
                ));
            }
            if gitref_ctx.state.selected_gitref.as_ref() != target_benchmark.params.gitref.as_ref()
            {
                gitref_ctx.dispatch.emit(GitrefAction::SetSelectedGitref(
                    target_benchmark.params.gitref.clone(),
                ));
            }
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmark(Box::new(Some(
                    target_benchmark,
                ))));
        }
        Err(error) => log!(format!(
            "AppContent: fetch benchmark {} failed: {}",
            uuid, error
        )),
    }
}

#[derive(Properties, PartialEq)]
#[allow(dead_code)]
pub struct AppContentProps {}

#[function_component(AppContent)]
pub fn app_content() -> Html {
    let ui_state = use_ui();
    let benchmark_ctx = use_benchmark();
    let gitref_ctx = use_gitref();
    let hardware_ctx = use_hardware();
    let route = use_route::<AppRoute>();
    let navigator = use_navigator();

    let is_loading_from_url = use_state(|| false);
    use_init_hardware(hardware_ctx.clone(), route.clone(), *is_loading_from_url);
    {
        let ui = ui_state.clone();
        use_effect_with((), move |_| {
            if is_mobile_viewport() && !ui.is_sidebar_collapsed {
                ui.dispatch(UiAction::ToggleSidebar);
            }

            let last_mobile = Rc::new(Cell::new(is_mobile_viewport()));
            let ui_inner = ui.clone();
            let last_mobile_handler = last_mobile.clone();
            let listener = gloo::events::EventListener::new(
                &web_sys::window().expect("window"),
                "resize",
                move |_| {
                    let now_mobile = is_mobile_viewport();
                    if now_mobile == last_mobile_handler.get() {
                        return;
                    }
                    last_mobile_handler.set(now_mobile);
                    let should_toggle = now_mobile != ui_inner.is_sidebar_collapsed;
                    if should_toggle {
                        ui_inner.dispatch(UiAction::ToggleSidebar);
                    }
                },
            );
            move || drop(listener)
        });
    }

    let route_generation = use_mut_ref(|| 0u64);
    {
        let route_clone = route.clone();
        let hardware_ctx_clone = hardware_ctx.clone();
        let gitref_ctx_clone = gitref_ctx.clone();
        let benchmark_ctx_clone = benchmark_ctx.clone();
        let is_loading_handle = is_loading_from_url.clone();
        let ui_state_clone = ui_state.clone();
        let route_generation = route_generation.clone();

        use_effect_with(route_clone, move |current_route| {
            let generation = {
                let mut counter = route_generation.borrow_mut();
                *counter = counter.wrapping_add(1);
                *counter
            };
            let generation_source = route_generation.clone();
            let is_current = move || *generation_source.borrow() == generation;

            match current_route {
                Some(AppRoute::Benchmark { uuid }) => {
                    is_loading_handle.set(true);
                    let uuid = uuid.clone();
                    let hardware_ctx = hardware_ctx_clone.clone();
                    let gitref_ctx = gitref_ctx_clone.clone();
                    let benchmark_ctx = benchmark_ctx_clone.clone();
                    let ui = ui_state_clone.clone();
                    let is_loading_handle = is_loading_handle.clone();
                    let is_current = is_current.clone();
                    yew::platform::spawn_local(async move {
                        apply_benchmark(&uuid, &hardware_ctx, &gitref_ctx, &benchmark_ctx).await;
                        if !is_current() {
                            return;
                        }
                        ui.dispatch(UiAction::SetComparePin(Box::new(None)));
                        is_loading_handle.set(false);
                    });
                }
                Some(AppRoute::Compare { left, right }) => {
                    is_loading_handle.set(true);
                    let left_uuid = left.clone();
                    let right_uuid = right.clone();
                    let hardware_ctx = hardware_ctx_clone.clone();
                    let gitref_ctx = gitref_ctx_clone.clone();
                    let benchmark_ctx = benchmark_ctx_clone.clone();
                    let ui = ui_state_clone.clone();
                    let is_loading_handle = is_loading_handle.clone();
                    let is_current = is_current.clone();
                    yew::platform::spawn_local(async move {
                        apply_benchmark(&left_uuid, &hardware_ctx, &gitref_ctx, &benchmark_ctx)
                            .await;
                        if !is_current() {
                            return;
                        }
                        match api::fetch_benchmark_by_uuid(&right_uuid).await {
                            Ok(right_benchmark) => {
                                if is_current() {
                                    ui.dispatch(UiAction::SetComparePin(Box::new(Some(
                                        right_benchmark,
                                    ))));
                                }
                            }
                            Err(error) => log!(format!(
                                "AppContent: fetch right benchmark {} failed: {}",
                                right_uuid, error
                            )),
                        }
                        if is_current() {
                            is_loading_handle.set(false);
                        }
                    });
                }
                _ => {
                    if *is_loading_handle {
                        is_loading_handle.set(false);
                    }
                }
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

    {
        let selected_uuid = benchmark_ctx
            .state
            .selected_benchmark
            .as_ref()
            .map(|benchmark| benchmark.uuid.to_string());
        let route_uuid = match route.as_ref() {
            Some(AppRoute::Benchmark { uuid }) => Some(uuid.clone()),
            _ => None,
        };
        let navigator = navigator.clone();
        let is_loading_handle = is_loading_from_url.clone();
        use_effect_with(
            (selected_uuid, route_uuid),
            move |(selected_uuid, route_uuid)| {
                if *is_loading_handle {
                    return;
                }
                if let (Some(selected), Some(current)) = (selected_uuid, route_uuid)
                    && selected != current
                    && let Some(nav) = navigator.as_ref()
                {
                    nav.push(&AppRoute::Benchmark {
                        uuid: selected.clone(),
                    });
                }
            },
        );
    }

    let show_detail = matches!(
        route,
        Some(AppRoute::Benchmark { .. }) | Some(AppRoute::Compare { .. })
    );

    html! {
        <div class={classes!(
            "app-shell",
            show_detail.then_some("detail-layout"),
            (!show_detail).then_some("landing-layout"),
            (show_detail && ui_state.is_sidebar_collapsed).then_some("sidebar-collapsed"),
        )}>
            <TopAppBar show_sidebar_toggle={show_detail} show_detail_actions={show_detail} />
            if show_detail {
                <Sidebar />
                <MainContent />
            } else {
                <Hero />
            }
        </div>
    }
}

#[hook]
fn use_init_hardware(
    hardware_ctx: HardwareContext,
    route: Option<AppRoute>,
    is_loading_from_url: bool,
) {
    use_effect_with(
        (route, is_loading_from_url),
        move |(route, is_loading_from_url)| {
            let dispatch = hardware_ctx.dispatch.clone();
            let already_selected = hardware_ctx.state.selected_hardware.clone();
            let current_route = route.clone();
            let loading_from_url = *is_loading_from_url;

            yew::platform::spawn_local(async move {
                match api::fetch_hardware_configurations().await {
                    Ok(mut hardware_list) => {
                        if !hardware_list.is_empty() {
                            hardware_list
                                .sort_by(|left, right| left.identifier.cmp(&right.identifier));
                            dispatch.emit(HardwareAction::SetHardwareList(hardware_list.clone()));

                            if already_selected.is_none() && !loading_from_url {
                                if matches!(
                                    current_route,
                                    Some(AppRoute::Benchmark { .. })
                                        | Some(AppRoute::Compare { .. })
                                ) {
                                    // URL-driven routes: apply_benchmark picks hardware from the benchmark.
                                } else if let Some(preferred) =
                                    preferred_hardware_identifier(&hardware_list)
                                {
                                    dispatch.emit(HardwareAction::SelectHardware(Some(preferred)));
                                }
                            }
                        }
                    }
                    Err(error) => log!(format!("Error fetching hardware: {}", error)),
                }
            });
            || ()
        },
    );
}

fn preferred_hardware_identifier(hardware_list: &[BenchmarkHardware]) -> Option<String> {
    const PREFERRED: &str = "spetz-amd-rkyv";
    let preferred = hardware_list
        .iter()
        .filter_map(|hardware| hardware.identifier.clone())
        .find(|identifier| identifier == PREFERRED);
    preferred.or_else(|| {
        hardware_list
            .iter()
            .find_map(|hardware| hardware.identifier.clone())
    })
}

#[hook]
fn use_load_gitrefs(
    gitref_ctx: GitrefContext,
    hardware: Option<String>,
    route: Option<AppRoute>,
    is_loading_from_url: bool,
) {
    use_effect_with(
        (hardware, route, is_loading_from_url),
        move |(hardware_dep, route_dep, loading_dep)| {
            let gitref_ctx = gitref_ctx.clone();
            let hardware_val = hardware_dep.clone();
            let route_val = route_dep.clone();
            let loading = *loading_dep;

            if let Some(hardware_id) = hardware_val {
                yew::platform::spawn_local(async move {
                    match api::fetch_gitrefs_for_hardware(&hardware_id).await {
                        Ok(gitrefs) => {
                            gitref_ctx
                                .dispatch
                                .emit(GitrefAction::SetGitrefs(gitrefs.clone()));
                            if gitrefs.is_empty() || loading {
                                return;
                            }
                            if matches!(
                                route_val,
                                Some(AppRoute::Benchmark { .. }) | Some(AppRoute::Compare { .. })
                            ) {
                                return;
                            }
                            let current = gitref_ctx.state.selected_gitref.clone();
                            let final_gitref = match current {
                                Some(existing) if gitrefs.contains(&existing) => existing,
                                _ => gitrefs[0].clone(),
                            };
                            if Some(final_gitref.clone()) != gitref_ctx.state.selected_gitref {
                                gitref_ctx
                                    .dispatch
                                    .emit(GitrefAction::SetSelectedGitref(Some(final_gitref)));
                            }
                        }
                        Err(error) => log!(format!("Error fetching gitrefs: {}", error)),
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
    use_effect_with((hardware, gitref), move |(hardware_dep, gitref_dep)| {
        let benchmark_ctx = benchmark_ctx.clone();
        if let (Some(hardware_id), Some(gitref_id)) = (hardware_dep.clone(), gitref_dep.clone()) {
            yew::platform::spawn_local(async move {
                match api::fetch_benchmarks_for_hardware_and_gitref(&hardware_id, &gitref_id).await
                {
                    Ok(benchmarks) => {
                        benchmark_ctx
                            .dispatch
                            .emit(BenchmarkAction::SetBenchmarksForGitref(
                                benchmarks,
                                hardware_id,
                                gitref_id,
                            ));
                    }
                    Err(error) => {
                        log!(format!("Error fetching benchmarks: {}", error));
                    }
                }
            });
        }
        || ()
    });
}
