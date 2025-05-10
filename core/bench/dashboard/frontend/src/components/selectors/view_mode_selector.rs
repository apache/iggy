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

use crate::state::benchmark::{BenchmarkAction, use_benchmark};
use crate::state::ui::{UiAction, ViewMode, use_ui};
use yew::prelude::*;

#[function_component(ViewModeSelector)]
pub fn view_mode_toggle() -> Html {
    let ui_state = use_ui();
    let benchmark_ctx = use_benchmark();
    let current_mode = &ui_state.view_mode;

    let single_onclick: Callback<MouseEvent> =
        {
            let ui_state = ui_state.clone();
            let benchmark_ctx = benchmark_ctx.clone();
            Callback::from(move |_| {
                if let Some(selected_benchmark) = &benchmark_ctx.state.selected_benchmark {
                    let kind = selected_benchmark.params.benchmark_kind;

                    ui_state.dispatch(UiAction::SetViewMode(ViewMode::SingleGitref));

                    if benchmark_ctx.state.selected_kind != kind {
                        benchmark_ctx
                            .dispatch
                            .emit(BenchmarkAction::SelectBenchmarkKind(kind));
                        gloo::console::log!(format!(
                            "Selected benchmark kind {:?} when switching to Single view",
                            kind
                        ));
                    }

                    let params_identifier = selected_benchmark.params.params_identifier.clone();
                    benchmark_ctx.dispatch.emit(
                        BenchmarkAction::SelectBenchmarkByParamsIdentifier(params_identifier),
                    );
                    gloo::console::log!(format!(
                        "Maintaining selection of benchmark with params_identifier: {}",
                        selected_benchmark.params.params_identifier
                    ));
                } else {
                    ui_state.dispatch(UiAction::SetViewMode(ViewMode::SingleGitref));
                }
            })
        };

    // TODO(hubcio): Trend is not used anywhere yet, because we don't have a regression ongoing.
    // let trend_onclick = {
    //     let ui_state = ui_state.clone();
    //     Callback::from(move |_| {
    //         ui_state.dispatch(UiAction::SetViewMode(ViewMode::GitrefTrend));
    //     })
    // };

    let recent_onclick = {
        let ui_state = ui_state.clone();
        Callback::from(move |_| {
            ui_state.dispatch(UiAction::SetViewMode(ViewMode::RecentBenchmarks));
        })
    };

    html! {
        <div class="view-mode-container">
            <h3>{"View Mode"}</h3>
            <div class="segmented-control">
                <button
                    class={if matches!(current_mode, ViewMode::SingleGitref) { "segment active" } else { "segment" }}
                    onclick={single_onclick}
                >
                    {"Single"}
                </button>
                // TODO(hubcio): Trend is not used anywhere yet, because we don't have a regression ongoing.
                // <button
                //     class={if matches!(current_mode, ViewMode::GitrefTrend) { "segment active" } else { "segment" }}
                //     onclick={trend_onclick}
                // >
                //     {"Trend"}
                // </button>
                <button
                    class={if matches!(current_mode, ViewMode::RecentBenchmarks) { "segment active" } else { "segment" }}
                    onclick={recent_onclick}
                >
                    {"Recent"}
                </button>
            </div>
        </div>
    }
}
