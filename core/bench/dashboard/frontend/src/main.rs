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

mod api;
mod components;
mod config;
mod error;
mod format;
mod hooks;
mod pages;
mod router;
mod state;
#[cfg(test)]
mod test_support;

use components::theme::theme_provider::ThemeProvider;
use pages::compare::releases::ReleasesPage;
use pages::compare::{ComparePage, RunList};
use pages::explore::Explore;
use pages::methodology::Methodology;
use pages::not_found::NotFound;
use pages::overview::Overview;
use pages::run::RunPage;
use router::AppRoute;
use state::{data::DataProvider, selection::SelectionProvider};
use yew::prelude::*;
use yew_router::{BrowserRouter, Switch};

#[function_component(App)]
pub fn app() -> Html {
    html! {
        <BrowserRouter>
            <ThemeProvider>
                <DataProvider>
                    <SelectionProvider>
                        <Switch<AppRoute> render={switch} />
                    </SelectionProvider>
                </DataProvider>
            </ThemeProvider>
        </BrowserRouter>
    }
}

fn switch(routes: AppRoute) -> Html {
    match routes {
        AppRoute::Home => html! { <Overview /> },
        AppRoute::Compare => html! { <ComparePage /> },
        AppRoute::CompareReleases => html! { <ReleasesPage /> },
        AppRoute::ComparePair { left, right } => html! {
            <ComparePage alias={RunList::from_iter([left.as_str(), right.as_str()])} />
        },
        // Keyed by the uuid, so another run's page starts with fresh state.
        AppRoute::Benchmark { uuid } => {
            let key = uuid.clone();
            html! { <RunPage {key} {uuid} /> }
        }
        AppRoute::Explore => html! { <Explore /> },
        AppRoute::Methodology => html! { <Methodology /> },
        AppRoute::NotFound => html! { <NotFound /> },
    }
}

fn main() {
    if let Some(document) = web_sys::window().and_then(|w| w.document())
        && let Ok(Some(splash)) = document.query_selector(".boot-splash")
    {
        splash.remove();
    }
    yew::Renderer::<App>::new().render();
}
