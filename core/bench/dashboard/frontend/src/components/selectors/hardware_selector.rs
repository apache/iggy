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

use crate::state::hardware::use_hardware;
use gloo::console::log;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct HardwareSelectorProps {
    pub on_hardware_select: Callback<String>,
}

#[function_component(HardwareSelector)]
pub fn hardware_selector(props: &HardwareSelectorProps) -> Html {
    let hardware_ctx = use_hardware();

    let on_hardware_change = create_on_hardware_change_callback(props.on_hardware_select.clone());

    html! {
        <div class="hardware-selector">
            <h3>{"Hardware"}</h3>
            <select
                onchange={on_hardware_change.clone()}
            >
                { for hardware_ctx.state.hardware_list.iter().map(|hardware| html! {
                    <option
                        value={hardware.identifier.clone().unwrap_or_else(|| "Unknown".to_string())}
                        selected={hardware_ctx.state.selected_hardware == Some(hardware.identifier.clone().unwrap_or_else(|| "Unknown".to_string()))}
                    >
                        {format!("{} @ {}", hardware.identifier.clone().unwrap_or_else(|| "Unknown".to_string()), &hardware.cpu_name)}
                    </option>
                }) }
            </select>
        </div>
    }
}

fn create_on_hardware_change_callback(on_hardware_select: Callback<String>) -> Callback<Event> {
    Callback::from(move |e: Event| {
        let target = e.target_dyn_into::<web_sys::HtmlSelectElement>();
        if let Some(select) = target {
            let value = select.value();
            log!(format!("Hardware selected via dropdown: {}", value));
            on_hardware_select.emit(value);
        }
    })
}
