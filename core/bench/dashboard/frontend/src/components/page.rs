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

//! The frame, headings and data states that the pages share. They only use the app-wide tokens.
//! They're plain functions because they hold no state.

use crate::components::footer::Footer;
use crate::components::layout::top_bar::TopBar;
use crate::router::AppRoute;
use crate::state::data::{DashboardData, Loadable};
use yew::prelude::*;
use yew_router::prelude::Link;

type CrumbQuery = Vec<(&'static str, String)>;

/// The top bar, then the content and the footer in one scroll container. `scroll_ref` lands on
/// that container, for parts that follow the scroll position.
pub fn page_shell(scroll_ref: &NodeRef, content: Html) -> Html {
    html! {
        <div class="page-frame">
            <TopBar />
            <div class="page-scroll" ref={scroll_ref.clone()}>
                <main class="page">{ content }</main>
                <Footer />
            </div>
        </div>
    }
}

/// `above` comes before the eyebrow, for example breadcrumbs. `aside` sits next to the text on
/// a wide screen and below it on a narrow one.
pub fn page_head(above: Html, eyebrow: Option<&str>, title: &str, lead: &str, aside: Html) -> Html {
    html! {
        <header class="page-head">
            <div class="page-container page-head-layout">
                <div class="page-head-text">
                    { above }
                    if let Some(eyebrow) = eyebrow {
                        <p class="eyebrow">{ eyebrow.to_owned() }</p>
                    }
                    <h1 class="page-title">{ title.to_owned() }</h1>
                    <p class="page-lead">{ lead.to_owned() }</p>
                </div>
                { aside }
            </div>
        </header>
    }
}

pub fn section_head(eyebrow: &'static str, title: &'static str, note: &'static str) -> Html {
    html! {
        <div class="section-head">
            <p class="eyebrow">{ eyebrow }</p>
            <h2 class="section-title">{ title }</h2>
            <p class="section-note">{ note }</p>
        </div>
    }
}

/// One link in a breadcrumb trail, with the query of the view it opens.
#[derive(Debug, Clone, PartialEq)]
pub struct Crumb {
    pub label: String,
    pub route: AppRoute,
    pub query: Vec<(&'static str, String)>,
}

impl Crumb {
    pub fn new(label: impl Into<String>, route: AppRoute) -> Self {
        Self {
            label: label.into(),
            route,
            query: Vec::new(),
        }
    }

    pub fn overview() -> Self {
        Self::new("Benchmarks", AppRoute::Home)
    }
}

/// The links of `trail`, then the name of the current page.
pub fn breadcrumbs(trail: &[Crumb], current: &str) -> Html {
    html! {
        <nav class="breadcrumbs" aria-label="Breadcrumb">
            <ol>
                { for trail.iter().map(|crumb| html! {
                    <li>
                        <Link<AppRoute, CrumbQuery>
                            to={crumb.route.clone()}
                            query={(!crumb.query.is_empty()).then(|| crumb.query.clone())}
                        >
                            { crumb.label.clone() }
                        </Link<AppRoute, CrumbQuery>>
                    </li>
                }) }
                <li aria-current="page">{ current.to_owned() }</li>
            </ol>
        </nav>
    }
}

/// The loading or failed state of `value`, else `ready` with the value. The failed state offers
/// `reload`, which fetches the failed data again.
pub fn loadable<T>(
    value: &Loadable<T>,
    what: &str,
    reload: &Callback<()>,
    ready: impl FnOnce(&T) -> Html,
) -> Html {
    match value {
        Loadable::Loading => html! {
            <p class="data-status data-loading">{ format!("Loading {what}...") }</p>
        },
        Loadable::Failed(error) => html! {
            <p class="data-status data-failed" role="alert">
                { format!("Couldn't load {what}: {error}") }
                <button type="button" class="data-retry" onclick={reload.reform(|_| ())}>
                    {"Try again"}
                </button>
            </p>
        },
        Loadable::Ready(value) => ready(value),
    }
}

/// The loading or failed state of the runs, then of the configuration. Empty once both have loaded.
pub fn data_status(data: &DashboardData) -> Html {
    loadable(&data.summaries, "the runs", &data.reload, |_| {
        loadable(
            &data.config,
            "the dashboard configuration",
            &data.reload,
            |_| Html::default(),
        )
    })
}
