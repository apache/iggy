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

use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BenchmarkInfoToggleProps {
    pub on_toggle: Callback<()>,
    pub is_visible: bool,
}

#[function_component(BenchmarkInfoToggle)]
pub fn benchmark_info_toggle(props: &BenchmarkInfoToggleProps) -> Html {
    let onclick = {
        let on_toggle = props.on_toggle.clone();
        Callback::from(move |_| {
            on_toggle.emit(());
        })
    };

    html! {
        <button
            class={classes!("icon-button", props.is_visible.then_some("active"))}
            {onclick}
            title="Toggle Benchmark Info"
        >
            <svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                <line x1="12" y1="8" x2="12" y2="12"/>
                <line x1="12" y1="16" x2="12" y2="16"/>
            </svg>
        </button>
    }
}
