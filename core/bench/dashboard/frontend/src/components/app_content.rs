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
    state::{
        benchmark::{BenchmarkAction, BenchmarkContext, use_benchmark},
        gitref::{GitrefAction, GitrefContext, use_gitref},
        hardware::{HardwareAction, HardwareContext, use_hardware},
        ui::{UiAction, use_ui},
    },
};
use gloo::console::log;
use urlencoding::decode;
use yew::prelude::*;
use yew_router::hooks::use_location;

// Props definitions
#[derive(Properties, PartialEq)]
pub struct AppContentProps {}

// Hardware initialization hook
#[hook]
fn use_init_hardware(hardware_ctx: HardwareContext) {
    use gloo::console::log;

    use_effect_with((), move |_| {
        let dispatch = hardware_ctx.dispatch.clone();
        let already_selected = hardware_ctx.state.selected_hardware.clone();
        yew::platform::spawn_local(async move {
            match api::fetch_hardware_configurations().await {
                Ok(mut hw_list) => {
                    if !hw_list.is_empty() {
                        hw_list.sort_by(|a, b| a.identifier.cmp(&b.identifier));
                        dispatch.emit(HardwareAction::SetHardwareList(hw_list.clone()));

                        if already_selected.is_none() {
                            if let Some(first_hw) = hw_list.first() {
                                dispatch.emit(HardwareAction::SelectHardware(Some(
                                    first_hw.identifier.clone().unwrap(),
                                )));
                            }
                        }
                    }
                }
                Err(e) => log!(format!("Error fetching hardware: {}", e)),
            }
        });
        || ()
    });
}
// Gitref loading hook
#[hook]
fn use_load_gitrefs(
    gitref_ctx: GitrefContext,
    benchmark_ctx: BenchmarkContext,
    hardware: Option<String>,
) {
    use gloo::console::log;

    use_effect_with(hardware.clone(), move |hardware| {
        let gitref_ctx = gitref_ctx.clone();
        let benchmark_ctx = benchmark_ctx.clone();
        let hardware = hardware.clone();

        if let Some(hardware) = hardware {
            yew::platform::spawn_local(async move {
                match api::fetch_gitrefs_for_hardware(&hardware).await {
                    Ok(vers) => {
                        gitref_ctx
                            .dispatch
                            .emit(GitrefAction::SetGitrefs(vers.clone()));
                        if !vers.is_empty() {
                            let current_selected = gitref_ctx.state.selected_gitref.clone();

                            let final_gitref = match current_selected {
                                Some(ref existing) if vers.contains(existing) => existing.clone(),
                                _ => {
                                    log!("Using first available version for new hardware");
                                    vers[0].clone()
                                }
                            };

                            if Some(final_gitref.clone()) != gitref_ctx.state.selected_gitref {
                                gitref_ctx
                                    .dispatch
                                    .emit(GitrefAction::SetSelectedGitref(Some(
                                        final_gitref.clone(),
                                    )));
                            }

                            match api::fetch_benchmarks_for_hardware_and_gitref(
                                &hardware,
                                &final_gitref,
                            )
                            .await
                            {
                                Ok(benchmarks) => {
                                    benchmark_ctx.dispatch.emit(
                                        BenchmarkAction::SetBenchmarksForGitref(
                                            benchmarks, hardware,
                                        ),
                                    );
                                }
                                Err(e) => log!(format!("Error fetching benchmarks: {}", e)),
                            }
                        }
                    }
                    Err(e) => log!(format!("Error fetching git refs: {}", e)),
                }
            });
        }
        || ()
    });
}

#[hook]
fn use_load_benchmarks(
    benchmark_ctx: BenchmarkContext,
    hardware: Option<String>,
    gitref: Option<String>,
) {
    use_effect_with(
        (hardware.clone(), gitref.clone()),
        move |(hardware, gitref)| {
            let benchmark_ctx = benchmark_ctx.clone();
            let hardware = hardware.clone();
            let gitref = gitref.clone();

            if let (Some(hardware), Some(gitref)) = (hardware, gitref) {
                yew::platform::spawn_local(async move {
                    match api::fetch_benchmarks_for_hardware_and_gitref(&hardware, &gitref).await {
                        Ok(benchmarks) => {
                            benchmark_ctx
                                .dispatch
                                .emit(BenchmarkAction::SetBenchmarksForGitref(
                                    benchmarks, hardware,
                                ));
                        }
                        Err(e) => log!(format!("Error fetching benchmarks: {}", e)),
                    }
                });
            }
            || ()
        },
    );
}

#[function_component(AppContent)]
pub fn app_content() -> Html {
    let hardware_ctx = use_hardware();
    let gitref_ctx = use_gitref();
    let benchmark_ctx = use_benchmark();
    let ui_state = use_ui();
    let theme_ctx = use_context::<(bool, Callback<()>)>().expect("Theme context not found");
    let is_dark = theme_ctx.0;
    let theme_toggle = theme_ctx.1;
    let location = use_location().expect("Should have <BrowserRouter> in the tree");

    use_init_hardware(hardware_ctx.clone());

    {
        let hardware_ctx = hardware_ctx.clone();
        let gitref_ctx = gitref_ctx.clone();
        let benchmark_ctx = benchmark_ctx.clone();
        let ui_state = ui_state.clone();
        let query_str = location.query_str().to_string();

        use_effect_with((), move |_| {
            let search = query_str.clone();
            let (hw_opt, gitref_opt, meas_opt, pid_opt) = parse_query_params_from_str(&search);
            if hw_opt.is_none() && gitref_opt.is_none() && meas_opt.is_none() && pid_opt.is_none() {
                log!("No query params, selecting first available");
            } else {
                if let Some(hw) = hw_opt {
                    log!("Selected hardware: {}", &hw);
                    let already = hardware_ctx.state.selected_hardware.as_ref();
                    if already != Some(&hw) {
                        hardware_ctx
                            .dispatch
                            .emit(HardwareAction::SelectHardware(Some(hw)));
                    }
                }

                if let Some(gr) = gitref_opt {
                    log!("Selected gitref: {}", &gr);
                    let already = &gitref_ctx.state.selected_gitref;
                    if already.as_ref() != Some(&gr) {
                        gitref_ctx
                            .dispatch
                            .emit(GitrefAction::SetSelectedGitref(Some(gr)));
                    }
                }

                if let Some(meas_str) = meas_opt {
                    log!("Selected measurement: {}", &meas_str);
                    if let Ok(meas) = meas_str.parse() {
                        if meas != ui_state.selected_measurement {
                            ui_state.dispatch(UiAction::SetMeasurementType(meas));
                        }
                    }
                }

                if let Some(pid) = pid_opt {
                    log!("Selected params identifier: {}", &pid);
                    let already_same_pid = benchmark_ctx
                        .state
                        .selected_benchmark
                        .as_ref()
                        .map(|b| b.params.params_identifier == pid)
                        .unwrap_or(false);
                    if !already_same_pid {
                        benchmark_ctx
                            .dispatch
                            .emit(BenchmarkAction::SelectBenchmarkByParamsIdentifier(pid));
                    }
                }
            }

            || ()
        });
    }

    use_load_gitrefs(
        gitref_ctx.clone(),
        benchmark_ctx.clone(),
        hardware_ctx.state.selected_hardware.clone(),
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
                    gitref_ctx.dispatch.emit(GitrefAction::SetSelectedGitref(Some(gitref)));
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

fn parse_query_params_from_str(
    search: &str,
) -> (
    Option<String>, // hardware
    Option<String>, // gitref
    Option<String>, // measurement
    Option<String>, // params_identifier
) {
    let search = search.trim_start_matches('?');
    let mut hardware = None;
    let mut gitref = None;
    let mut measurement = None;
    let mut params_identifier = None;

    for pair in search.split('&') {
        let mut parts = pair.split('=');
        if let Some(key) = parts.next() {
            if let Some(value) = parts.next() {
                let decoded = decode(value).unwrap_or_else(|_| value.into()).into_owned();

                match key {
                    "hardware" => hardware = Some(decoded),
                    "gitref" => gitref = Some(decoded),
                    "measurement" => measurement = Some(decoded),
                    "params_identifier" => params_identifier = Some(decoded),
                    _ => {}
                }
            }
        }
    }

    (hardware, gitref, measurement, params_identifier)
}
