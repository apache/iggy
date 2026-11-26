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

use gloo::{
    storage::{LocalStorage, Storage},
    utils::document,
};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ThemeProviderProps {
    pub children: Html,
}

#[function_component(ThemeProvider)]
pub fn theme_provider(props: &ThemeProviderProps) -> Html {
    // `None` until the stored theme is read, which happens in an effect: the server has no storage,
    // and the first render in the browser must give the HTML of the server. Until then the page is
    // light, unless the script at the start of the body of `index.html` set the dark class.
    let stored = use_state(|| None::<bool>);
    {
        let stored = stored.clone();
        use_effect_with((), move |()| {
            stored.set(Some(
                LocalStorage::get("theme")
                    .map(|theme: String| theme == "dark")
                    .unwrap_or(false),
            ));
        });
    }
    let is_dark = stored.unwrap_or(false);

    // Updates the body class and the storage when the theme changes, once the stored theme is known.
    use_effect_with(*stored, move |stored| {
        if let Some(is_dark) = *stored {
            let body = document().body().unwrap();
            body.set_class_name(if is_dark { "dark" } else { "" });
            let _ = LocalStorage::set("theme", if is_dark { "dark" } else { "light" });
        }
        || ()
    });

    let toggle_theme = {
        let stored = stored.clone();
        Callback::from(move |_| stored.set(Some(!is_dark)))
    };

    html! {
        <ContextProvider<(bool, Callback<()>)> context={(is_dark, toggle_theme)}>
            {props.children.clone()}
        </ContextProvider<(bool, Callback<()>)>>
    }
}
