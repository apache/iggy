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

//! The compare page's runs as named in its link, `?runs=a,b,c` with the baseline first, and the
//! links that other pages build to it. The iggy-bench and the OMB compare pages share the link
//! form and its rules, and each takes only the ids of its own framework.

use std::fmt::Display;
use std::iter;
use std::str::FromStr;

use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::summary::BenchmarkSummary;
use uuid::Uuid;
use yew::prelude::*;
use yew_router::Routable;
use yew_router::prelude::use_navigator;
use yew_router::query::Raw;

use crate::router::AppRoute;
use crate::state::selection::MAX_COMPARED_RUNS;

pub const RUNS_PARAM: &str = "runs";
const RUN_SEPARATOR: &str = ",";

/// The id of a run of one framework. Each framework has its own id type and its own compare page,
/// so a link or a selection of one framework can't name a run of the other.
pub trait RunId: Copy + PartialEq + Display + FromStr + 'static {
    /// The compare page that takes runs with this id.
    const COMPARE_ROUTE: AppRoute;
}

impl RunId for Uuid {
    const COMPARE_ROUTE: AppRoute = AppRoute::Compare;
}

impl RunId for OmbRunId {
    const COMPARE_ROUTE: AppRoute = AppRoute::OmbCompare;
}

/// One entry in a link's run list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEntry<Id> {
    Id(Id),
    /// Text that isn't an id of the page's framework, so it doesn't name a run.
    Invalid(String),
}

/// The runs that a link names, in link order and without repeats. Empty parts, as in `a,,b` or
/// after a trailing comma, don't name a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunList<Id> {
    /// At most [`MAX_COMPARED_RUNS`].
    pub entries: Vec<RunEntry<Id>>,
    /// The entries past the limit, which the page leaves out.
    pub dropped: usize,
}

#[derive(Properties, PartialEq)]
pub struct CompareLinkProps<Id: PartialEq> {
    /// The baseline first.
    pub runs: Vec<Id>,
    #[prop_or_default]
    pub classes: Classes,
    #[prop_or_default]
    pub title: Option<AttrValue>,
    pub children: Html,
}

/// A link to the iggy-bench compare page.
pub type CompareLink = CompareLinkOf<Uuid>;

impl<Id> Default for RunList<Id> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            dropped: 0,
        }
    }
}

impl<'a, Id: FromStr + PartialEq> FromIterator<&'a str> for RunList<Id> {
    fn from_iter<T: IntoIterator<Item = &'a str>>(parts: T) -> Self {
        let mut entries: Vec<RunEntry<Id>> = Vec::new();
        for part in parts
            .into_iter()
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            let entry = part
                .parse()
                .map_or_else(|_| RunEntry::Invalid(part.to_owned()), RunEntry::Id);
            if !entries.contains(&entry) {
                entries.push(entry);
            }
        }
        let dropped = entries.len().saturating_sub(MAX_COMPARED_RUNS);
        entries.truncate(MAX_COMPARED_RUNS);
        Self { entries, dropped }
    }
}

impl<Id: FromStr + PartialEq> From<&str> for RunList<Id> {
    /// The value of the `runs` parameter.
    fn from(runs: &str) -> Self {
        runs.split(RUN_SEPARATOR).collect()
    }
}

/// A link to the compare page of the framework of `runs`. The router would escape the query's
/// commas as `%2C`, and an id needs no escaping, so the link writes the query itself.
#[function_component(CompareLinkOf)]
pub fn compare_link_of<Id: RunId>(props: &CompareLinkProps<Id>) -> Html {
    let navigator = use_navigator();
    let href = compare_href(&props.runs);
    let query = compare_query(&props.runs);
    let onclick = Callback::from(move |event: MouseEvent| {
        if opens_elsewhere(&event) {
            return;
        }
        if let Some(navigator) = &navigator {
            event.prevent_default();
            let Ok(()) = navigator.push_with_query(&Id::COMPARE_ROUTE, Raw(query.as_str()));
        }
    });
    html! {
        <a {href} class={props.classes.clone()} title={props.title.clone()} {onclick}>
            { props.children.clone() }
        </a>
    }
}

/// With a modifier key, the browser opens the link's href in another tab or window, and this tab's
/// router must leave that click alone.
pub fn opens_elsewhere(event: &MouseEvent) -> bool {
    event.meta_key() || event.ctrl_key() || event.shift_key() || event.alt_key()
}

pub fn compare_query<Id: Display>(runs: &[Id]) -> String {
    let ids: Vec<String> = runs.iter().map(Id::to_string).collect();
    format!("{RUNS_PARAM}={}", ids.join(RUN_SEPARATOR))
}

/// The path and query of the compare page for `runs`, the baseline first.
pub fn compare_href<Id: RunId>(runs: &[Id]) -> String {
    format!("{}?{}", Id::COMPARE_ROUTE.to_path(), compare_query(runs))
}

/// `runs` with `baseline` first and the other runs in their order.
pub fn with_baseline<Id: Copy + PartialEq>(runs: &[Id], baseline: Id) -> Vec<Id> {
    iter::once(baseline)
        .chain(runs.iter().copied().filter(|run| *run != baseline))
        .collect()
}

pub fn without<Id: Copy + PartialEq>(runs: &[Id], removed: Id) -> Vec<Id> {
    runs.iter().copied().filter(|run| *run != removed).collect()
}

/// The shared crate's version order, then the id, so runs that started at the same moment still
/// have one order.
pub fn oldest_first<'a>(
    runs: impl IntoIterator<Item = &'a BenchmarkSummary>,
) -> Vec<&'a BenchmarkSummary> {
    let mut runs: Vec<_> = runs.into_iter().collect();
    runs.sort_by(|left, right| {
        (&left.version_order, left.uuid).cmp(&(&right.version_order, right.uuid))
    });
    runs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, OLD_RELEASE, OMB_ALL, OMB_EDGE8_DEFAULT, OMB_RC1_DEFAULT, OMB_RC1_PERSISTED,
        RELEASE, limited,
    };

    type UuidList = RunList<Uuid>;
    type OmbList = RunList<OmbRunId>;

    fn id(id: u128) -> Uuid {
        Uuid::from_u128(id)
    }

    fn ids(runs: &[&BenchmarkSummary]) -> Vec<Uuid> {
        runs.iter().map(|run| run.uuid).collect()
    }

    #[test]
    fn given_empty_or_blank_list_when_parsed_should_name_no_run() {
        for text in ["", " ", ",", " , ,"] {
            assert_eq!(UuidList::from(text), UuidList::default(), "{text:?}");
        }
    }

    #[test]
    fn given_one_or_three_runs_when_parsed_should_keep_link_order() {
        let one = UuidList::from(id(7).to_string().as_str());
        let three = UuidList::from(format!("{},{} , {},", id(3), id(1), id(2)).as_str());

        assert_eq!(one.entries, [RunEntry::Id(id(7))]);
        assert_eq!(
            three.entries,
            [
                RunEntry::Id(id(3)),
                RunEntry::Id(id(1)),
                RunEntry::Id(id(2))
            ]
        );
        assert_eq!((one.dropped, three.dropped), (0, 0));
    }

    #[test]
    fn given_more_than_three_runs_when_parsed_should_keep_first_three_and_count_rest() {
        let text = [1, 2, 3, 4, 5].map(|run| id(run).to_string()).join(",");

        let list = UuidList::from(text.as_str());

        assert_eq!(
            list.entries,
            [
                RunEntry::Id(id(1)),
                RunEntry::Id(id(2)),
                RunEntry::Id(id(3))
            ]
        );
        assert_eq!(list.dropped, 2);
    }

    #[test]
    fn given_repeated_run_when_parsed_should_keep_first_place_in_any_spelling() {
        let upper = id(0xab).to_string().to_uppercase();
        let text = format!("{},{},{upper},{}", id(0xab), id(2), id(2));

        let list = UuidList::from(text.as_str());

        assert_eq!(list.entries, [RunEntry::Id(id(0xab)), RunEntry::Id(id(2))]);
        assert_eq!(list.dropped, 0);
    }

    #[test]
    fn given_text_that_is_no_uuid_when_parsed_should_keep_it_as_invalid_entry() {
        let text = format!("{},not-a-run,{}", id(1), id(2));

        assert_eq!(
            UuidList::from(text.as_str()).entries,
            [
                RunEntry::Id(id(1)),
                RunEntry::Invalid("not-a-run".to_owned()),
                RunEntry::Id(id(2)),
            ]
        );
    }

    #[test]
    fn given_old_link_pair_when_listed_should_match_query_form() {
        let (left, right) = (id(1).to_string(), id(2).to_string());

        assert_eq!(
            UuidList::from_iter([left.as_str(), right.as_str()]),
            UuidList::from(format!("{left},{right}").as_str())
        );
    }

    #[test]
    fn given_runs_when_baseline_moved_or_run_removed_should_keep_order_of_others() {
        let runs = [id(1), id(2), id(3)];

        assert_eq!(with_baseline(&runs, id(3)), [id(3), id(1), id(2)]);
        assert_eq!(with_baseline(&runs, id(1)), runs);
        assert_eq!(without(&runs, id(2)), [id(1), id(3)]);
        assert_eq!(without(&without(&runs, id(1)), id(3)), [id(2)]);
    }

    #[test]
    fn given_runs_when_linked_should_write_readable_query_that_parses_back() {
        let runs = [id(2), id(1)];

        let query = compare_query(&runs);

        assert_eq!(query, format!("runs={},{}", id(2), id(1)));
        let value = query.trim_start_matches("runs=");
        assert_eq!(
            UuidList::from(value).entries,
            runs.map(RunEntry::Id).to_vec()
        );
    }

    #[test]
    fn given_runs_of_several_versions_when_ordered_should_put_oldest_version_first() {
        let candidate = limited(1, CANDIDATE);
        let release = limited(2, RELEASE);
        let old_release = limited(3, OLD_RELEASE);
        let rerun = limited(4, RELEASE);

        assert_eq!(
            ids(&oldest_first([&rerun, &candidate, &release, &old_release])),
            [id(3), id(2), id(4), id(1)]
        );
    }

    #[test]
    fn given_omb_fixture_ids_when_parsed_should_keep_link_order() {
        let runs = [
            OMB_RC1_PERSISTED.id(),
            OMB_EDGE8_DEFAULT.id(),
            OMB_RC1_DEFAULT.id(),
        ];
        let text = format!("{}, {},{},", runs[0], runs[1], runs[2]);

        let list = OmbList::from(text.as_str());

        assert_eq!(list.entries, runs.map(RunEntry::Id));
        assert_eq!(list.dropped, 0);
    }

    #[test]
    fn given_every_omb_fixture_when_parsed_should_keep_first_three_and_count_rest() {
        let ids: Vec<OmbRunId> = OMB_ALL.iter().map(|fixture| fixture.id()).collect();
        let text = ids
            .iter()
            .map(OmbRunId::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let list = OmbList::from(text.as_str());

        assert_eq!(
            list.entries,
            ids[..MAX_COMPARED_RUNS]
                .iter()
                .copied()
                .map(RunEntry::Id)
                .collect::<Vec<_>>()
        );
        assert_eq!(list.dropped, OMB_ALL.len() - MAX_COMPARED_RUNS);
    }

    #[test]
    fn given_id_of_other_framework_when_parsed_should_keep_it_as_invalid_entry() {
        let omb = OMB_RC1_DEFAULT.id().to_string();
        let uuid = id(1).to_string();

        assert_eq!(
            OmbList::from(format!("{omb},{uuid}").as_str()).entries,
            [
                RunEntry::Id(OMB_RC1_DEFAULT.id()),
                RunEntry::Invalid(uuid.clone())
            ]
        );
        assert_eq!(
            UuidList::from(format!("{uuid},{omb}").as_str()).entries,
            [RunEntry::Id(id(1)), RunEntry::Invalid(omb)]
        );
    }

    #[test]
    fn given_omb_runs_when_linked_should_open_omb_compare_with_query_that_parses_back() {
        let runs = [OMB_RC1_DEFAULT.id(), OMB_RC1_PERSISTED.id()];

        let href = compare_href(&runs);

        assert_eq!(href, format!("/omb/compare?runs={},{}", runs[0], runs[1]));
        let value = href.trim_start_matches("/omb/compare?runs=");
        assert_eq!(
            OmbList::from(value).entries,
            runs.map(RunEntry::Id).to_vec()
        );
        assert_eq!(compare_href(&[id(1)]), format!("/compare?runs={}", id(1)));
    }

    #[test]
    fn given_omb_runs_when_baseline_moved_or_run_removed_should_keep_order_of_others() {
        let runs = [
            OMB_RC1_DEFAULT.id(),
            OMB_RC1_PERSISTED.id(),
            OMB_EDGE8_DEFAULT.id(),
        ];

        assert_eq!(with_baseline(&runs, runs[2]), [runs[2], runs[0], runs[1]]);
        assert_eq!(without(&runs, runs[1]), [runs[0], runs[2]]);
    }
}
