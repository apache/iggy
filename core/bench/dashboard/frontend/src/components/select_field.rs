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

//! A labeled select whose choice lives in the page URL.

use web_sys::HtmlSelectElement;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SelectFieldProps {
    pub label: &'static str,
    /// The value and the text of each option.
    pub options: Vec<(String, String)>,
    pub selected: String,
    pub onchange: Callback<String>,
    /// Shows the choice in its own style instead of the select's text. The select stays on top,
    /// invisible, so it still opens on a click and takes key presses.
    #[prop_or_default]
    pub face: Option<Html>,
}

/// The select sets its value after every render: once the user picks an option, the browser
/// no longer follows the `selected` attribute, so Back would show a stale choice.
#[function_component(SelectField)]
pub fn select_field(props: &SelectFieldProps) -> Html {
    let select_ref = use_node_ref();
    {
        let select_ref = select_ref.clone();
        let selected = props.selected.clone();
        use_effect(move || {
            if let Some(select) = select_ref.cast::<HtmlSelectElement>() {
                select.set_value(&selected);
            }
        });
    }
    let onchange = {
        let onchange = props.onchange.clone();
        Callback::from(move |event: Event| {
            if let Some(select) = event.target_dyn_into::<HtmlSelectElement>() {
                onchange.emit(select.value());
            }
        })
    };
    html! {
        <label class={classes!("explore-facet", props.face.is_some().then_some("explore-facet-face"))}>
            <span class="explore-facet-label">{ props.label }</span>
            if let Some(face) = &props.face {
                { face.clone() }
            }
            <select ref={select_ref} class="explore-select" {onchange}>
                { for props.options.iter().map(|(value, text)| html! {
                    <option key={value.clone()} value={value.clone()}>{ text.as_str() }</option>
                }) }
            </select>
        </label>
    }
}
