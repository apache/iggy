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

//! The document title of the current page. The server writes the head of the first page, and this
//! component sets the title when the reader moves to another page. Both take it from the same
//! [`PageHead`], so the title of a page doesn't depend on how the reader got there.

use bench_dashboard_shared::head::PageHead;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::pages::ExploreScope;
use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;
use yew::prelude::*;
use yew_router::prelude::{use_location, use_route};

use crate::router::AppRoute;
use crate::state::data::{DashboardData, Loadable, use_dashboard_data};
use crate::state::omb::OmbData;

/// Sets the title once the data of the page has loaded. Until then the title of the previous page
/// stays, which on the first page is the title that the server wrote.
#[function_component(PageTitle)]
pub fn page_title() -> Html {
    let route = use_route::<AppRoute>();
    let scope = use_location()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .map(ExploreScope::of_query)
        .unwrap_or_default();
    let data = use_dashboard_data();
    // The context, not `use_omb_data`, which would fetch the OMB runs on every page.
    let omb = use_context::<OmbData>();
    let title = route
        .and_then(|route| page_head(&route, scope, &data, omb.as_ref()))
        .map(|head| head.title);
    use_effect_with(title, |title| {
        if let Some(title) = title {
            gloo::utils::document().set_title(title);
        }
    });
    Html::default()
}

/// The head of the page at `route`, as the server builds it for the same path. `scope` is the
/// Explore scope of the query, which only the Explore page reads. `None` while the data that the
/// head needs is loading, and for the old run link, which moves to the run page.
pub fn page_head(
    route: &AppRoute,
    scope: ExploreScope,
    data: &DashboardData,
    omb: Option<&OmbData>,
) -> Option<PageHead> {
    let omb_runs = || match omb.map(|omb| &omb.runs) {
        Some(Loadable::Ready(list)) => Some(list),
        _ => None,
    };
    match route {
        AppRoute::Home => match (&data.summaries, &data.config) {
            (Loadable::Ready(summaries), Loadable::Ready(config)) => {
                Some(PageHead::overview(summaries, config))
            }
            _ => None,
        },
        AppRoute::Explore => match (&data.summaries, &data.config) {
            (Loadable::Ready(summaries), Loadable::Ready(config)) => {
                Some(PageHead::explore(summaries, config, scope))
            }
            _ => None,
        },
        AppRoute::Run { uuid } => {
            let summaries = summaries(data)?;
            let run = Uuid::parse_str(uuid)
                .ok()
                .and_then(|uuid| summaries.iter().find(|run| run.uuid == uuid));
            Some(run.map_or_else(PageHead::not_found, PageHead::run))
        }
        AppRoute::OldRun { .. } => None,
        AppRoute::Compare | AppRoute::ComparePair { .. } => Some(PageHead::compare_runs()),
        AppRoute::CompareReleases => Some(PageHead::compare_releases()),
        AppRoute::OmbRuns => omb_runs().map(|list| PageHead::omb_runs(list)),
        AppRoute::OmbRun { id } => {
            let list = omb_runs()?;
            let run = id
                .parse::<OmbRunId>()
                .ok()
                .and_then(|id| list.runs.iter().find(|run| run.id == id));
            Some(run.map_or_else(PageHead::not_found, PageHead::omb_run))
        }
        AppRoute::OmbCompare => Some(PageHead::omb_compare()),
        AppRoute::Methodology => Some(PageHead::methodology()),
        AppRoute::NotFound => Some(PageHead::not_found()),
    }
}

fn summaries(data: &DashboardData) -> Option<&[BenchmarkSummary]> {
    match &data.summaries {
        Loadable::Ready(summaries) => Some(summaries),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        MACHINE, OMB_RC1_DEFAULT, RELEASE, THRESHOLDS, omb_run_list, producer, run,
    };
    use bench_dashboard_shared::config::DashboardConfig;
    use bench_dashboard_shared::omb::summary::OmbRunList;
    use std::rc::Rc;

    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: Vec::new(),
            headlines: Vec::new(),
            thresholds: THRESHOLDS,
        }
    }

    fn loaded(summaries: Vec<BenchmarkSummary>) -> DashboardData {
        DashboardData {
            summaries: Loadable::Ready(Rc::new(summaries)),
            config: Loadable::Ready(Rc::new(config())),
            reload: Callback::noop(),
        }
    }

    fn loading() -> DashboardData {
        DashboardData {
            summaries: Loadable::Loading,
            config: Loadable::Loading,
            reload: Callback::noop(),
        }
    }

    fn omb(runs: Loadable<OmbRunList>) -> OmbData {
        OmbData {
            runs,
            reload: Callback::noop(),
        }
    }

    #[test]
    fn given_loaded_data_when_building_head_of_each_route_should_match_server_head() {
        let summaries = vec![run(1, producer(), MACHINE, RELEASE)];
        let data = loaded(summaries.clone());
        let list = omb_run_list();
        let omb_run = OMB_RC1_DEFAULT.summary();
        let omb = omb(Loadable::Ready(Rc::new(list.clone())));

        for (route, expected) in [
            (AppRoute::Home, PageHead::overview(&summaries, &config())),
            (
                AppRoute::Explore,
                PageHead::explore(&summaries, &config(), ExploreScope::Official),
            ),
            (
                AppRoute::Run {
                    uuid: Uuid::from_u128(1).to_string(),
                },
                PageHead::run(&summaries[0]),
            ),
            (AppRoute::Compare, PageHead::compare_runs()),
            (
                AppRoute::ComparePair {
                    left: "a".to_owned(),
                    right: "b".to_owned(),
                },
                PageHead::compare_runs(),
            ),
            (AppRoute::CompareReleases, PageHead::compare_releases()),
            (AppRoute::OmbRuns, PageHead::omb_runs(&list)),
            (
                AppRoute::OmbRun {
                    id: omb_run.id.to_string(),
                },
                PageHead::omb_run(&omb_run),
            ),
            (AppRoute::OmbCompare, PageHead::omb_compare()),
            (AppRoute::Methodology, PageHead::methodology()),
            (AppRoute::NotFound, PageHead::not_found()),
        ] {
            assert_eq!(
                page_head(&route, ExploreScope::Official, &data, Some(&omb)),
                Some(expected),
                "{route:?}"
            );
        }
    }

    #[test]
    fn given_scope_in_query_when_building_explore_head_should_build_head_of_that_scope() {
        let summaries = vec![run(1, producer(), MACHINE, RELEASE)];
        let data = loaded(summaries.clone());

        for scope in ExploreScope::ALL {
            assert_eq!(
                page_head(&AppRoute::Explore, scope, &data, None),
                Some(PageHead::explore(&summaries, &config(), scope)),
                "{scope}"
            );
        }
        assert_eq!(
            page_head(
                &AppRoute::Run {
                    uuid: Uuid::from_u128(1).to_string()
                },
                ExploreScope::All,
                &data,
                None
            ),
            Some(PageHead::run(&summaries[0])),
            "only the Explore page reads the scope"
        );
    }

    #[test]
    fn given_unknown_run_when_building_head_should_build_not_found_head() {
        let data = loaded(vec![run(1, producer(), MACHINE, RELEASE)]);
        let omb = omb(Loadable::Ready(Rc::new(omb_run_list())));

        for route in [
            AppRoute::Run {
                uuid: Uuid::from_u128(2).to_string(),
            },
            AppRoute::Run {
                uuid: "not-a-uuid".to_owned(),
            },
            AppRoute::OmbRun {
                id: "0000000000000000".to_owned(),
            },
        ] {
            assert_eq!(
                page_head(&route, ExploreScope::Official, &data, Some(&omb)),
                Some(PageHead::not_found()),
                "{route:?}"
            );
        }
    }

    #[test]
    fn given_data_still_loading_when_building_head_should_keep_previous_title() {
        let omb = omb(Loadable::Loading);

        for route in [
            AppRoute::Home,
            AppRoute::Explore,
            AppRoute::Run {
                uuid: Uuid::from_u128(1).to_string(),
            },
            AppRoute::OmbRuns,
            AppRoute::OmbRun {
                id: OMB_RC1_DEFAULT.id().to_string(),
            },
            AppRoute::OldRun {
                uuid: Uuid::from_u128(1).to_string(),
            },
        ] {
            assert_eq!(
                page_head(&route, ExploreScope::Official, &loading(), Some(&omb)),
                None,
                "{route:?}"
            );
        }
        assert_eq!(
            page_head(
                &AppRoute::Methodology,
                ExploreScope::Official,
                &loading(),
                None
            ),
            Some(PageHead::methodology())
        );
    }
}
