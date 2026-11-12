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

use crate::components::footer::render_github_icon;
use crate::router::AppRoute;
use yew::prelude::*;
use yew_router::prelude::{Link, use_route};

const GITHUB_URL: &str = "https://github.com/apache/iggy";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NavTab {
    Overview,
    Explore,
    Methodology,
}

/// The bar on top of every page: brand, main navigation, theme and GitHub.
#[function_component(TopBar)]
pub fn top_bar() -> Html {
    let route = use_route::<AppRoute>();
    let (is_dark, toggle_theme) =
        use_context::<(bool, Callback<()>)>().expect("Theme context not found");

    let active = route.as_ref().and_then(NavTab::of);
    let tab_class =
        |tab: NavTab| classes!("top-bar-tab", (active == Some(tab)).then_some("active"));

    let on_theme_click = Callback::from(move |_| toggle_theme.emit(()));

    let theme_title = if is_dark {
        "Switch to light theme"
    } else {
        "Switch to dark theme"
    };
    let logo_src = if is_dark {
        "/assets/iggy-light.svg"
    } else {
        "/assets/iggy-dark.svg"
    };

    html! {
        <header class="top-bar">
            <div class="top-bar-inner">
                <Link<AppRoute> to={AppRoute::Home} classes={classes!("top-bar-brand")}>
                    <img src={logo_src} alt="Apache Iggy" />
                    <span class="top-bar-divider" aria-hidden="true"></span>
                    <span class="top-bar-wordmark">{"Benchmarks"}</span>
                </Link<AppRoute>>
                <nav class="top-bar-nav" aria-label="Main">
                    <Link<AppRoute> to={AppRoute::Home} classes={tab_class(NavTab::Overview)}>
                        {"Overview"}
                    </Link<AppRoute>>
                    <Link<AppRoute> to={AppRoute::Explore} classes={tab_class(NavTab::Explore)}>
                        {"Explore"}
                    </Link<AppRoute>>
                    <Link<AppRoute> to={AppRoute::Methodology} classes={tab_class(NavTab::Methodology)}>
                        {"Methodology"}
                    </Link<AppRoute>>
                </nav>
                <div class="top-bar-actions">
                    <button
                        type="button"
                        class="top-bar-icon"
                        onclick={on_theme_click}
                        title={theme_title}
                        aria-label={theme_title}
                    >
                        { render_theme_icon(is_dark) }
                    </button>
                    <a
                        class="top-bar-github"
                        href={GITHUB_URL}
                        target="_blank"
                        rel="noopener noreferrer"
                        aria-label="Apache Iggy on GitHub"
                    >
                        { render_github_icon() }
                        <span class="top-bar-github-label">{"GitHub"}</span>
                    </a>
                </div>
            </div>
        </header>
    }
}

impl NavTab {
    /// The run and compare views belong to Explore. The releases compare opens from the
    /// Overview, so it belongs there.
    const fn of(route: &AppRoute) -> Option<Self> {
        match route {
            AppRoute::Home | AppRoute::CompareReleases => Some(Self::Overview),
            AppRoute::Explore
            | AppRoute::Benchmark { .. }
            | AppRoute::Compare
            | AppRoute::ComparePair { .. } => Some(Self::Explore),
            AppRoute::Methodology => Some(Self::Methodology),
            AppRoute::NotFound => None,
        }
    }
}

fn render_theme_icon(is_dark: bool) -> Html {
    if is_dark {
        html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
            </svg>
        }
    } else {
        html! {
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
                 fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="5" />
                <line x1="12" y1="1" x2="12" y2="3" />
                <line x1="12" y1="21" x2="12" y2="23" />
                <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
                <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
                <line x1="1" y1="12" x2="3" y2="12" />
                <line x1="21" y1="12" x2="23" y2="12" />
                <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
                <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
            </svg>
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_explore_run_or_compare_route_when_tab_picked_should_choose_explore() {
        let run = AppRoute::Benchmark {
            uuid: "run".to_owned(),
        };
        let old_compare_link = AppRoute::ComparePair {
            left: "left".to_owned(),
            right: "right".to_owned(),
        };

        assert_eq!(NavTab::of(&AppRoute::Explore), Some(NavTab::Explore));
        assert_eq!(NavTab::of(&run), Some(NavTab::Explore));
        assert_eq!(NavTab::of(&AppRoute::Compare), Some(NavTab::Explore));
        assert_eq!(NavTab::of(&old_compare_link), Some(NavTab::Explore));
    }

    #[test]
    fn given_page_routes_when_tab_picked_should_choose_their_own_tab() {
        assert_eq!(NavTab::of(&AppRoute::Home), Some(NavTab::Overview));
        assert_eq!(
            NavTab::of(&AppRoute::CompareReleases),
            Some(NavTab::Overview)
        );
        assert_eq!(
            NavTab::of(&AppRoute::Methodology),
            Some(NavTab::Methodology)
        );
        assert_eq!(NavTab::of(&AppRoute::NotFound), None);
    }
}
