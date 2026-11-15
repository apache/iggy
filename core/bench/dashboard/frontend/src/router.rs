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

use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Debug, Clone, PartialEq, Routable)]
pub enum AppRoute {
    #[at("/")]
    Home,
    #[at("/explore")]
    Explore,
    #[at("/runs/:uuid")]
    Run { uuid: String },
    /// The run link from before `/runs`. Embed snippets on other pages still point here, so it
    /// moves to the run's page.
    #[at("/benchmarks/:uuid")]
    OldRun { uuid: String },
    /// The runs are in the query: `?runs=a,b,c`, the baseline first.
    #[at("/compare")]
    Compare,
    /// The suite and the two versions are in the query: `?suite=...&base=...&head=...`.
    #[at("/compare/releases")]
    CompareReleases,
    /// The link form from before the query. It stays an alias of `?runs=left,right`, so shared
    /// links keep working.
    #[at("/compare/:left/:right")]
    ComparePair { left: String, right: String },
    /// The OpenMessaging Benchmark runs, a part of the site of its own.
    #[at("/omb")]
    OmbRuns,
    #[at("/omb/runs/:id")]
    OmbRun { id: String },
    /// The OMB runs are in the query: `?runs=a,b,c`, the baseline first.
    #[at("/omb/compare")]
    OmbCompare,
    #[at("/methodology")]
    Methodology,
    #[not_found]
    #[at("/404")]
    NotFound,
}

#[derive(Properties, PartialEq)]
pub struct MovedProps {
    pub to: AppRoute,
}

/// Sends an old link to its page. It replaces the history entry, so Back skips the old link
/// instead of landing on it and moving forward again.
#[function_component(Moved)]
pub fn moved(props: &MovedProps) -> Html {
    let navigator = use_navigator();
    let to = props.to.clone();
    use_effect_with(to, move |to| {
        if let Some(navigator) = navigator {
            navigator.replace(to);
        }
    });
    Html::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_releases_path_when_recognized_should_open_releases_compare_not_run_pair() {
        assert_eq!(
            AppRoute::recognize("/compare/releases"),
            Some(AppRoute::CompareReleases)
        );
        assert_eq!(
            AppRoute::recognize("/compare/releases/"),
            Some(AppRoute::CompareReleases)
        );
        assert_eq!(AppRoute::CompareReleases.to_path(), "/compare/releases");
    }

    #[test]
    fn given_old_pair_link_when_recognized_should_keep_alias() {
        assert_eq!(
            AppRoute::recognize("/compare/3f2a9c1b/0eb36c51"),
            Some(AppRoute::ComparePair {
                left: "3f2a9c1b".to_owned(),
                right: "0eb36c51".to_owned(),
            })
        );
        assert_eq!(AppRoute::recognize("/compare"), Some(AppRoute::Compare));
    }

    #[test]
    fn given_omb_paths_when_recognized_should_open_omb_pages() {
        assert_eq!(AppRoute::recognize("/omb"), Some(AppRoute::OmbRuns));
        assert_eq!(AppRoute::recognize("/omb/"), Some(AppRoute::OmbRuns));
        assert_eq!(
            AppRoute::recognize("/omb/runs/3f2a9c1b0eb36c51"),
            Some(AppRoute::OmbRun {
                id: "3f2a9c1b0eb36c51".to_owned(),
            })
        );
        assert_eq!(
            AppRoute::recognize("/omb/compare"),
            Some(AppRoute::OmbCompare)
        );
    }

    #[test]
    fn given_omb_compare_path_when_recognized_should_not_open_run_page() {
        assert_ne!(
            AppRoute::recognize("/omb/compare"),
            Some(AppRoute::OmbRun {
                id: "compare".to_owned(),
            })
        );
        assert_eq!(
            AppRoute::recognize("/omb/compare/"),
            Some(AppRoute::OmbCompare)
        );
    }

    #[test]
    fn given_omb_routes_when_written_should_stay_under_omb() {
        let run = AppRoute::OmbRun {
            id: "3f2a9c1b0eb36c51".to_owned(),
        };

        assert_eq!(AppRoute::OmbRuns.to_path(), "/omb");
        assert_eq!(run.to_path(), "/omb/runs/3f2a9c1b0eb36c51");
        assert_eq!(AppRoute::OmbCompare.to_path(), "/omb/compare");
    }

    #[test]
    fn given_iggy_bench_paths_when_recognized_should_open_explore_and_run_pages() {
        assert_eq!(AppRoute::recognize("/explore"), Some(AppRoute::Explore));
        assert_eq!(AppRoute::recognize("/explore/"), Some(AppRoute::Explore));
        assert_eq!(
            AppRoute::recognize("/runs/0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f"),
            Some(AppRoute::Run {
                uuid: "0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f".to_owned(),
            })
        );
        assert_eq!(AppRoute::Explore.to_path(), "/explore");
        assert_eq!(
            AppRoute::Run {
                uuid: "0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f".to_owned(),
            }
            .to_path(),
            "/runs/0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f"
        );
    }

    #[test]
    fn given_old_run_link_when_recognized_should_keep_alias() {
        assert_eq!(
            AppRoute::recognize("/benchmarks/0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f"),
            Some(AppRoute::OldRun {
                uuid: "0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f".to_owned(),
            })
        );
    }

    #[test]
    fn given_omb_id_under_iggy_bench_path_when_recognized_should_open_iggy_bench_run_page() {
        assert_eq!(
            AppRoute::recognize("/runs/3f2a9c1b0eb36c51"),
            Some(AppRoute::Run {
                uuid: "3f2a9c1b0eb36c51".to_owned(),
            })
        );
    }
}
