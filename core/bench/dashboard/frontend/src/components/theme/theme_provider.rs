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
    let is_dark = use_state(|| {
        LocalStorage::get("theme")
            .map(|theme: String| theme == "dark")
            .unwrap_or(false)
    });

    // Effect to update body class and local storage when theme changes
    {
        let is_dark = is_dark.clone();
        use_effect_with(*is_dark, move |is_dark| {
            let body = document().body().unwrap();
            if *is_dark {
                body.set_class_name("dark");
            } else {
                body.set_class_name("");
            }

            let _ = LocalStorage::set("theme", if *is_dark { "dark" } else { "light" });

            || ()
        });
    }

    let toggle_theme = {
        let is_dark = is_dark.clone();
        Callback::from(move |_| {
            is_dark.set(!*is_dark);
        })
    };

    html! {
        <ContextProvider<(bool, Callback<()>)> context={(*is_dark, toggle_theme)}>
            {props.children.clone()}
        </ContextProvider<(bool, Callback<()>)>>
    }
}
