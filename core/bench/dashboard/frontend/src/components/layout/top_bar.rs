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
use crate::pages::compare::runs::opens_elsewhere;
use crate::router::AppRoute;
use bench_dashboard_shared::github::GithubStars;
use yew::prelude::*;
use yew_router::Routable;
use yew_router::prelude::{Link, use_navigator, use_route};

const GITHUB_URL: &str = "https://github.com/apache/iggy";
const GITHUB_LABEL: &str = "Apache Iggy on GitHub";
const STAR_POINTS: &str = "12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 \
                           2 9.27 8.91 8.26 12 2";
const OMB_TAB_LABEL: &str = "OMB";
const OMB_TAB_TITLE: &str = "OpenMessaging Benchmark";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NavTab {
    Overview,
    Explore,
    Omb,
    Methodology,
}

/// The bar on top of every page: brand, main navigation, GitHub with the star count, and theme.
#[function_component(TopBar)]
pub fn top_bar() -> Html {
    let route = use_route::<AppRoute>();
    let (is_dark, toggle_theme) =
        use_context::<(bool, Callback<()>)>().expect("Theme context not found");

    let active = route.as_ref().and_then(NavTab::of);
    let tab_class =
        |tab: NavTab| classes!("top-bar-tab", (active == Some(tab)).then_some("active"));

    let on_theme_click = Callback::from(move |_| toggle_theme.emit(()));
    // A `Link` takes no title, and the short tab label needs the full name on hover.
    let navigator = use_navigator();
    let open_omb = Callback::from(move |event: MouseEvent| {
        if let Some(navigator) = navigator.as_ref().filter(|_| !opens_elsewhere(&event)) {
            event.prevent_default();
            navigator.push(&AppRoute::OmbRuns);
        }
    });

    let stars = use_context::<GithubStars>()
        .and_then(|github| github.stars)
        .map(star_count_text);
    let github_label = stars.as_ref().map_or_else(
        || GITHUB_LABEL.to_owned(),
        |stars| format!("{GITHUB_LABEL}, {stars} stars"),
    );

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
                    <a
                        href={AppRoute::OmbRuns.to_path()}
                        class={tab_class(NavTab::Omb)}
                        title={OMB_TAB_TITLE}
                        onclick={open_omb}
                    >
                        { OMB_TAB_LABEL }
                    </a>
                    <Link<AppRoute> to={AppRoute::Methodology} classes={tab_class(NavTab::Methodology)}>
                        {"Methodology"}
                    </Link<AppRoute>>
                </nav>
                <div class="top-bar-actions">
                    <a
                        class="top-bar-github"
                        href={GITHUB_URL}
                        target="_blank"
                        rel="noopener noreferrer"
                        aria-label={github_label}
                    >
                        { render_github_icon() }
                        if let Some(stars) = stars {
                            <span class="top-bar-github-stars">
                                { render_star_icon() }
                                { stars }
                            </span>
                        } else {
                            <span class="top-bar-github-label">{"GitHub"}</span>
                        }
                    </a>
                    <button
                        type="button"
                        class="top-bar-icon"
                        onclick={on_theme_click}
                        title={theme_title}
                        aria-label={theme_title}
                    >
                        { render_theme_icon(is_dark) }
                    </button>
                </div>
            </div>
        </header>
    }
}

impl NavTab {
    /// The run and compare views belong to Explore. The releases compare opens from the
    /// Overview, so it belongs there. Every OMB page belongs to the OMB tab, so the two
    /// frameworks never share a tab.
    const fn of(route: &AppRoute) -> Option<Self> {
        match route {
            AppRoute::Home | AppRoute::CompareReleases => Some(Self::Overview),
            AppRoute::Explore
            | AppRoute::Run { .. }
            | AppRoute::OldRun { .. }
            | AppRoute::Compare
            | AppRoute::ComparePair { .. } => Some(Self::Explore),
            AppRoute::OmbRuns | AppRoute::OmbRun { .. } | AppRoute::OmbCompare => Some(Self::Omb),
            AppRoute::Methodology => Some(Self::Methodology),
            AppRoute::NotFound => None,
        }
    }
}

/// The count as GitHub writes it: thousands with one decimal, for example 5.1K.
fn star_count_text(stars: u32) -> String {
    if stars >= 1000 {
        format!("{:.1}K", f64::from(stars) / 1000.0)
    } else {
        stars.to_string()
    }
}

fn render_star_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
             fill="currentColor" aria-hidden="true">
            <polygon points={STAR_POINTS} />
        </svg>
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
        let run = AppRoute::Run {
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

    #[test]
    fn given_star_counts_when_written_should_shorten_thousands() {
        let texts: Vec<_> = [7, 999, 1000, 5123, 5150, 12_480]
            .into_iter()
            .map(star_count_text)
            .collect();

        assert_eq!(texts, ["7", "999", "1.0K", "5.1K", "5.2K", "12.5K"]);
    }

    #[test]
    fn given_omb_routes_when_tab_picked_should_choose_omb() {
        let run = AppRoute::OmbRun {
            id: "3f2a9c1b0eb36c51".to_owned(),
        };

        assert_eq!(NavTab::of(&AppRoute::OmbRuns), Some(NavTab::Omb));
        assert_eq!(NavTab::of(&run), Some(NavTab::Omb));
        assert_eq!(NavTab::of(&AppRoute::OmbCompare), Some(NavTab::Omb));
    }
}
