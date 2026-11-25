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

//! The dashboard app. The browser binary renders it, or hydrates the HTML that the dashboard
//! server rendered. With the `ssr` feature, the server renders it to HTML through [`ssr`].

mod api;
mod components;
mod config;
mod error;
mod hooks;
mod pages;
mod router;
#[cfg(feature = "ssr")]
pub mod ssr;
mod state;
#[cfg(test)]
mod test_support;

pub use state::start::StartData;

use components::document_head::DocumentHead;
use components::theme::theme_provider::ThemeProvider;
use gloo::console::error;
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
use std::rc::Rc;
use uuid::Uuid;
use web_sys::{Document, Element};
use yew::prelude::*;
use yew_router::{BrowserRouter, Switch};

/// The element that holds the app. The server writes the HTML of the app into it.
const ROOT_ID: &str = "app";
/// The `<script type="application/json">` element with the [`StartData`] of a page that the server
/// rendered.
pub const START_DATA_ID: &str = "start-data";

#[derive(Properties, PartialEq)]
pub struct AppProps {
    /// The data that the server rendered the page with. `None` for a page that the browser renders
    /// alone.
    #[prop_or_default]
    pub start: Option<Rc<StartData>>,
}

#[function_component(App)]
pub fn app(props: &AppProps) -> Html {
    html! {
        <BrowserRouter>
            { shell(props.start.clone()) }
        </BrowserRouter>
    }
}

/// Renders the app into its root. When the server rendered the page, the app hydrates the HTML that
/// is there instead, which needs the same data that the server used.
pub fn start() {
    let document = gloo::utils::document();
    let Some(root) = document.get_element_by_id(ROOT_ID) else {
        error!("The page has no element with the id", ROOT_ID);
        return;
    };
    mount(root, start_data(&document).map(Rc::new));
}

#[cfg(feature = "hydration")]
fn mount(root: Element, start: Option<Rc<StartData>>) {
    let hydrate = start.is_some();
    let renderer = yew::Renderer::<App>::with_root_and_props(root, AppProps { start });
    if hydrate {
        renderer.hydrate();
    } else {
        renderer.render();
    }
}

/// Without hydration, the app replaces the HTML of the server, so a build without the feature still
/// works on a rendered page.
#[cfg(not(feature = "hydration"))]
fn mount(root: Element, start: Option<Rc<StartData>>) {
    yew::Renderer::<App>::with_root_and_props(root, AppProps { start }).render();
}

/// `None` for a page that the server did not render, and for start data that does not parse.
fn start_data(document: &Document) -> Option<StartData> {
    let text = document.get_element_by_id(START_DATA_ID)?.text_content()?;
    serde_json::from_str(&text)
        .inspect_err(|e| error!("The start data of the page does not parse:", e.to_string()))
        .ok()
}

/// Everything below the router. The server renders the same tree, so the browser can hydrate it.
fn shell(start: Option<Rc<StartData>>) -> Html {
    html! {
        <ThemeProvider>
            <GithubStarsProvider>
                <DataProvider start={start.clone()}>
                    <OmbDataProvider {start}>
                        <SelectionProvider>
                            <OmbSelectionProvider>
                                <DocumentHead />
                                <Switch<AppRoute> render={switch} />
                            </OmbSelectionProvider>
                        </SelectionProvider>
                    </OmbDataProvider>
                </DataProvider>
            </GithubStarsProvider>
        </ThemeProvider>
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
