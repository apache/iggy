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

use crate::state::ui::{UiAction, ViewMode, use_ui};
use yew::prelude::*;

#[function_component(ViewModeSelector)]
pub fn view_mode_toggle() -> Html {
    let ui_state = use_ui();
    let is_trend_view = matches!(ui_state.view_mode, ViewMode::GitrefTrend);

    let onclick = {
        let ui_state = ui_state.clone();
        Callback::from(move |_| {
            ui_state.dispatch(UiAction::SetViewMode(if is_trend_view {
                ViewMode::SingleGitref
            } else {
                ViewMode::GitrefTrend
            }));
        })
    };

    html! {
        <div class="view-mode-container">
            <h3>{"View Mode"}</h3>
            <div class="segmented-control">
                <button
                    class={if !is_trend_view { "segment active" } else { "segment" }}
                    onclick={onclick.clone()}
                >
                    {"Single"}
                </button>
                <button
                    class={if is_trend_view { "segment active" } else { "segment" }}
                    {onclick}
                >
                    {"Trend"}
                </button>
            </div>
        </div>
    }
}
