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

use crate::router::AppRoute;
use crate::state::benchmark::{BenchmarkAction, use_benchmark};
use crate::state::ui::{UiAction, ViewMode, use_ui};
use yew::prelude::*;
use yew_router::prelude::use_navigator;

#[function_component(Logo)]
pub fn logo() -> Html {
    let (is_dark, _) = use_context::<(bool, Callback<()>)>().expect("Theme context not found");
    let ui = use_ui();
    let benchmark_ctx = use_benchmark();
    let navigator = use_navigator();
    let logo_src = if is_dark {
        "/assets/iggy-light.png"
    } else {
        "/assets/iggy-dark.png"
    };

    let on_click = {
        let ui = ui.clone();
        let benchmark_ctx = benchmark_ctx.clone();
        let navigator = navigator.clone();
        Callback::from(move |_| {
            benchmark_ctx
                .dispatch
                .emit(BenchmarkAction::SelectBenchmark(Box::new(None)));
            ui.dispatch(UiAction::SetComparePin(None));
            ui.dispatch(UiAction::SetViewMode(ViewMode::SingleGitref));
            ui.dispatch(UiAction::SetLanding(true));
            if let Some(nav) = navigator.as_ref() {
                nav.push(&AppRoute::Home);
            }
        })
    };

    html! {
        <button type="button" class="logo logo-home-button" onclick={on_click}
                title="Back to overview">
            <img src={logo_src} alt="Apache Iggy Logo" />
            <h1>{"Apache Iggy Benchmarks"}</h1>
        </button>
    }
}
