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
use pages::omb::compare::OmbComparePage;
use pages::omb::list::OmbRunsPage;
use pages::omb::run::OmbRunPage;
use pages::overview::Overview;
use pages::run::RunPage;
use router::{AppRoute, Moved};
use state::data::DataProvider;
use state::github::GithubStarsProvider;
use state::omb::OmbDataProvider;
use state::selection::{OmbSelectionProvider, SelectionProvider};
use uuid::Uuid;
use yew::prelude::*;
use yew_router::{BrowserRouter, Switch};

#[function_component(App)]
pub fn app() -> Html {
    html! {
        <BrowserRouter>
            <ThemeProvider>
                <GithubStarsProvider>
                    <DataProvider>
                        <OmbDataProvider>
                            <SelectionProvider>
                                <OmbSelectionProvider>
                                    <Switch<AppRoute> render={switch} />
                                </OmbSelectionProvider>
                            </SelectionProvider>
                        </OmbDataProvider>
                    </DataProvider>
                </GithubStarsProvider>
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
            <ComparePage alias={RunList::<Uuid>::from_iter([left.as_str(), right.as_str()])} />
        },
        // Keyed by the uuid, so another run's page starts with fresh state.
        AppRoute::Run { uuid } => {
            let key = uuid.clone();
            html! { <RunPage {key} {uuid} /> }
        }
        AppRoute::OldRun { uuid } => html! { <Moved to={AppRoute::Run { uuid }} /> },
        AppRoute::Explore => html! { <Explore /> },
        AppRoute::OmbRuns => html! { <OmbRunsPage /> },
        // Keyed by the id, so another run's page starts with fresh state.
        AppRoute::OmbRun { id } => {
            let key = id.clone();
            html! { <OmbRunPage {key} {id} /> }
        }
        AppRoute::OmbCompare => html! { <OmbComparePage /> },
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
