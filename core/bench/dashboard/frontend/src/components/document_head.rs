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

//! The head of the current page. The server writes the head of the first page, and this component
//! writes the head when the reader moves to another page. Both build it from the same
//! [`PageHead`], so the head of a page does not depend on how the reader got there.

use bench_dashboard_shared::head::{HEAD_END, HEAD_START, PUBLIC_URL_META, PageHead};
use bench_dashboard_shared::html::escape;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::pages::ExploreScope;
use gloo::console::error;
use std::rc::Rc;
use uuid::Uuid;
use wasm_bindgen::JsValue;
use web_sys::{Document, Node};
use yew::prelude::*;
use yew_router::prelude::{use_location, use_route};

use crate::router::AppRoute;
use crate::state::data::{DashboardData, Loadable};
use crate::state::omb::OmbData;

/// Writes the head of the page once its data has loaded. On the first page, the head that the
/// server wrote stays, and the component sets only the title, which a frontend build without the
/// server lacks. After the reader moves to another page, the component writes every tag of the
/// head. Until the data of that page has loaded, the head keeps only the title of the previous
/// page, so no link, image or robots rule of the previous page stays.
#[function_component(DocumentHead)]
pub fn document_head() -> Html {
    let route = use_route::<AppRoute>();
    let location = use_location();
    let scope = location
        .as_ref()
        .and_then(|location| location.query::<Vec<(String, String)>>().ok())
        .map(ExploreScope::of_query)
        .unwrap_or_default();
    // The path and the query. A fragment alone does not make another page.
    let address = location
        .map(|location| format!("{}{}", location.path(), location.query_str()))
        .unwrap_or_default();
    let data = use_context::<DashboardData>().expect("DataProvider wraps the router");
    // The contexts, not `use_dashboard_data` and `use_omb_data`, which would fetch the runs on
    // every page.
    let omb = use_context::<OmbData>();
    let head = route.and_then(|route| page_head(&route, scope, &data, omb.as_ref()));
    let first_address = use_memo((), |()| address.clone());
    let moved = use_mut_ref(|| false);
    use_effect_with((head, address), move |(head, address)| {
        let document = gloo::utils::document();
        if !*moved.borrow() && *address == *first_address {
            if let Some(head) = head {
                document.set_title(&head.title);
            }
            return;
        }
        *moved.borrow_mut() = true;
        let tags = match head {
            Some(head) => head.tags(public_url(&document).as_deref()).to_string(),
            None => format!("<title>{}</title>", escape(&document.title())),
        };
        if let Err(e) = replace_head_tags(&document, &tags) {
            error!("Can't write the head of the page:", e);
        }
    });
    Html::default()
}

/// The public address of the dashboard, which the server writes into the page. `None` when the
/// server has none, so the head has no absolute links, as the server writes it.
fn public_url(document: &Document) -> Option<String> {
    document
        .query_selector(&format!(r#"meta[name="{PUBLIC_URL_META}"]"#))
        .ok()
        .flatten()
        .and_then(|meta| meta.get_attribute("content"))
}

/// Replaces the nodes between the comments [`HEAD_START`] and [`HEAD_END`] with `tags`. A frontend
/// build without the comments keeps its head.
fn replace_head_tags(document: &Document, tags: &str) -> Result<(), JsValue> {
    let Some(head) = document.head() else {
        return Ok(());
    };
    let is_comment = |node: &Node, comment: &str| {
        node.node_type() == Node::COMMENT_NODE
            && node
                .node_value()
                .is_some_and(|text| format!("<!--{text}-->") == comment)
    };
    let mut between = Vec::new();
    let mut start = false;
    let mut node = head.first_child();
    let end = loop {
        let Some(current) = node else {
            return Ok(());
        };
        if start && is_comment(&current, HEAD_END) {
            break current;
        }
        if start {
            between.push(current.clone());
        }
        start |= is_comment(&current, HEAD_START);
        node = current.next_sibling();
    };
    for old in between {
        head.remove_child(&old)?;
    }
    // Tags parsed in a detached element: a script among them, the structured data, never runs.
    let parsed = document.create_element("div")?;
    parsed.set_inner_html(tags);
    while let Some(tag) = parsed.first_child() {
        head.insert_before(&tag, Some(&end))?;
    }
    Ok(())
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
        Some(Loadable::Ready(list)) => Some(Rc::clone(list)),
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
            let id = Uuid::parse_str(uuid).ok();
            let Loadable::Ready(summaries) = data.runs_of(id) else {
                return None;
            };
            let run = id.and_then(|id| summaries.iter().find(|run| run.uuid == id));
            Some(run.map_or_else(PageHead::not_found, PageHead::run))
        }
        AppRoute::OldRun { .. } => None,
        AppRoute::Compare | AppRoute::ComparePair { .. } => Some(PageHead::compare_runs()),
        AppRoute::CompareReleases => Some(PageHead::compare_releases()),
        AppRoute::OmbRuns => omb_runs().map(|list| PageHead::omb_runs(&list)),
        AppRoute::OmbRun { id } => {
            let id = id.parse::<OmbRunId>().ok();
            let Loadable::Ready(list) = omb?.runs_of(id) else {
                return None;
            };
            let run = id.and_then(|id| list.runs.iter().find(|run| run.id == id));
            Some(run.map_or_else(PageHead::not_found, PageHead::omb_run))
        }
        AppRoute::OmbCompare => Some(PageHead::omb_compare()),
        AppRoute::Methodology => Some(PageHead::methodology()),
        AppRoute::NotFound => Some(PageHead::not_found()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::start::PageRuns;
    use crate::test_support::{
        MACHINE, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED, RELEASE, THRESHOLDS, omb_run_list, producer,
        run,
    };
    use bench_dashboard_shared::config::DashboardConfig;
    use bench_dashboard_shared::omb::summary::OmbRunList;
    use bench_dashboard_shared::summary::BenchmarkSummary;

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
            page_runs: None,
            reload: Callback::noop(),
        }
    }

    fn loading() -> DashboardData {
        DashboardData {
            summaries: Loadable::Loading,
            config: Loadable::Loading,
            page_runs: None,
            reload: Callback::noop(),
        }
    }

    fn omb(runs: Loadable<OmbRunList>) -> OmbData {
        OmbData {
            runs,
            page_runs: None,
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
    fn given_only_runs_of_one_run_page_when_building_heads_should_build_that_page_alone() {
        let run_a = run(1, producer(), MACHINE, RELEASE);
        let run_b = run(2, producer(), MACHINE, RELEASE);
        let data = DashboardData {
            page_runs: Some(PageRuns {
                id: run_a.uuid,
                runs: Rc::new(vec![run_a.clone(), run_b.clone()]),
            }),
            config: Loadable::Ready(Rc::new(config())),
            ..loading()
        };
        let omb_a = OMB_RC1_DEFAULT.summary();
        let omb_b = OMB_RC1_PERSISTED.summary();
        let omb = OmbData {
            page_runs: Some(PageRuns {
                id: omb_a.id,
                runs: Rc::new(OmbRunList {
                    enabled: true,
                    runs: vec![omb_a.clone(), omb_b.clone()],
                }),
            }),
            ..omb(Loadable::Loading)
        };
        let head = |route: AppRoute| page_head(&route, ExploreScope::Official, &data, Some(&omb));

        assert_eq!(
            head(AppRoute::Run {
                uuid: run_a.uuid.to_string()
            }),
            Some(PageHead::run(&run_a))
        );
        assert_eq!(
            head(AppRoute::OmbRun {
                id: omb_a.id.to_string()
            }),
            Some(PageHead::omb_run(&omb_a))
        );
        for route in [
            AppRoute::Run {
                uuid: run_b.uuid.to_string(),
            },
            AppRoute::Home,
            AppRoute::Explore,
            AppRoute::OmbRun {
                id: omb_b.id.to_string(),
            },
            AppRoute::OmbRuns,
        ] {
            assert_eq!(head(route.clone()), None, "{route:?}");
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
