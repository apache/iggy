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
    /// Shows the choice in its own style instead of the text of its option.
    #[prop_or_default]
    pub face: Option<Html>,
}

/// The field shows its label and the choice. The select covers the whole field, invisible, so a
/// click anywhere on the field opens it, and the field still takes key presses.
///
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
    let face = props
        .face
        .clone()
        .unwrap_or_else(|| html! { chosen_text(&props.options, &props.selected).to_owned() });
    html! {
        <label class="explore-facet">
            <span class="explore-facet-text">
                <span class="explore-facet-label">{ props.label }</span>
                // The select gives the choice to screen readers.
                <span class="explore-facet-value" aria-hidden="true">{ face }</span>
            </span>
            <select ref={select_ref} class="explore-select" {onchange}>
                { for props.options.iter().map(|(value, text)| html! {
                    <option key={value.clone()} value={value.clone()}>{ text.as_str() }</option>
                }) }
            </select>
        </label>
    }
}

/// The text of the chosen option. A value with no option shows nothing, like the select.
fn chosen_text<'a>(options: &'a [(String, String)], selected: &str) -> &'a str {
    options
        .iter()
        .find(|(value, _)| value == selected)
        .map_or("", |(_, text)| text.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> Vec<(String, String)> {
        [("", "Any machine (3)"), ("mxpc", "mxpc (2)")]
            .map(|(value, text)| (value.to_owned(), text.to_owned()))
            .to_vec()
    }

    #[test]
    fn given_chosen_value_when_shown_should_show_text_of_its_option() {
        assert_eq!(chosen_text(&options(), "mxpc"), "mxpc (2)");
        assert_eq!(chosen_text(&options(), ""), "Any machine (3)");
    }

    #[test]
    fn given_value_without_option_when_shown_should_show_nothing() {
        assert_eq!(chosen_text(&options(), "atlas"), "");
    }
}
