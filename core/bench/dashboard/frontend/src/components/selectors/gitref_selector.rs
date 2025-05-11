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

use wasm_bindgen::JsCast;
use web_sys::HtmlSelectElement;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct GitrefSelectorProps {
    pub gitrefs: Vec<String>,
    pub selected_gitref: String,
    pub on_gitref_select: Callback<String>,
}

#[function_component(GitrefSelector)]
pub fn gitref_selector(props: &GitrefSelectorProps) -> Html {
    let onchange = {
        let on_gitref_select = props.on_gitref_select.clone();
        Callback::from(move |e: Event| {
            if let Some(select) = e
                .target()
                .and_then(|t| t.dyn_into::<HtmlSelectElement>().ok())
            {
                let gitref = select.value();
                on_gitref_select.emit(gitref);
            }
        })
    };

    html! {
        <div class="gitref-select">
            <h3>{"Version"}</h3>
            <select {onchange} value={props.selected_gitref.clone()}>
                {
                    props.gitrefs.iter().map(|gitref| {
                        html! {
                            <option
                                value={gitref.clone()}
                                selected={gitref == &props.selected_gitref}
                            >
                                {gitref}
                            </option>
                        }
                    }).collect::<Html>()
                }
            </select>
        </div>
    }
}
