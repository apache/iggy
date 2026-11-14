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

//! The Explore page's view as the URL holds it: the scope, the search text, one value per facet and
//! the sort. A link is never an error, because the page ignores what it can't use.

use serde::{Deserialize, Serialize};

use crate::api::{api_name, from_api_name};

const SCOPE_PARAM: &str = "scope";
const SEARCH_PARAM: &str = "q";
const SORT_PARAM: &str = "sort";

/// Which runs the page lists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// The runs whose suite the dashboard configuration names.
    #[default]
    Official,
    All,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sort {
    #[default]
    Recent,
    /// Highest first.
    Throughput,
    /// Lowest first.
    P99,
}

/// A filter that takes one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Facet {
    Machine,
    Version,
    Kind,
    Topology,
    Durability,
    MessageSize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExploreQuery {
    pub scope: Scope,
    /// As typed, so the search box keeps the spaces between words.
    pub search: String,
    /// One value per facet, as written in the URL. Never empty, because [`Self::set_facet`] keeps
    /// empty values out.
    pub(super) facets: [Option<String>; Facet::COUNT],
    pub sort: Sort,
}

impl Scope {
    pub const ALL: [Self; 2] = [Self::Official, Self::All];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Official => "Official",
            Self::All => "All runs",
        }
    }

    pub const fn includes(self, official: bool) -> bool {
        official || matches!(self, Self::All)
    }
}

impl Sort {
    pub const ALL: [Self; 3] = [Self::Recent, Self::Throughput, Self::P99];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Recent => "Most recent",
            Self::Throughput => "Highest throughput",
            Self::P99 => "Lowest P99",
        }
    }
}

impl Facet {
    pub const ALL: [Self; 6] = [
        Self::Machine,
        Self::Version,
        Self::Kind,
        Self::Topology,
        Self::Durability,
        Self::MessageSize,
    ];
    pub const COUNT: usize = Self::ALL.len();

    /// The position of the facet in [`Self::ALL`], and in every array indexed by facet.
    pub const fn index(self) -> usize {
        self as usize
    }

    const fn param(self) -> &'static str {
        match self {
            Self::Machine => "machine",
            Self::Version => "version",
            Self::Kind => "kind",
            Self::Topology => "nodes",
            Self::Durability => "durability",
            Self::MessageSize => "size",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Machine => "Machine",
            Self::Version => "Version",
            Self::Kind => "Kind",
            Self::Topology => "Topology",
            Self::Durability => "Durability",
            Self::MessageSize => "Message size",
        }
    }

    /// The label of the option that clears the facet.
    pub const fn any_label(self) -> &'static str {
        match self {
            Self::Machine => "All machines",
            Self::Version => "All versions",
            Self::Kind => "All kinds",
            Self::Topology => "Any topology",
            Self::Durability => "Any durability",
            Self::MessageSize => "Any size",
        }
    }
}

impl ExploreQuery {
    /// Reads the decoded pairs of a query string. An unknown parameter, a value that doesn't parse
    /// and an empty facet value are ignored. When a parameter repeats, the last valid one wins.
    /// Whether a loaded run has a facet value is for the catalog to decide.
    pub fn from_pairs<K: AsRef<str>, V: AsRef<str>>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        let mut query = Self::default();
        for (key, value) in pairs {
            let (key, value) = (key.as_ref(), value.as_ref());
            match key {
                SCOPE_PARAM => query.scope = from_api_name(value).unwrap_or(query.scope),
                SEARCH_PARAM => query.search = value.to_owned(),
                SORT_PARAM => query.sort = from_api_name(value).unwrap_or(query.sort),
                _ => {
                    if let Some(facet) = Facet::ALL.into_iter().find(|facet| facet.param() == key)
                        && !value.is_empty()
                    {
                        query.set_facet(facet, Some(value.to_owned()));
                    }
                }
            }
        }
        query
    }

    /// The pairs of the query string. Default values stay out, so the default view is the bare
    /// page URL.
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if self.scope != Scope::default() {
            pairs.extend(api_name(self.scope).map(|scope| (SCOPE_PARAM, scope)));
        }
        if !self.search.is_empty() {
            pairs.push((SEARCH_PARAM, self.search.clone()));
        }
        for facet in Facet::ALL {
            if let Some(value) = self.facet(facet) {
                pairs.push((facet.param(), value.to_owned()));
            }
        }
        if self.sort != Sort::default() {
            pairs.extend(api_name(self.sort).map(|sort| (SORT_PARAM, sort)));
        }
        pairs
    }

    pub fn facet(&self, facet: Facet) -> Option<&str> {
        self.facets[facet.index()].as_deref()
    }

    /// An empty value clears the facet, because the URL can't tell it from no value.
    pub fn set_facet(&mut self, facet: Facet, value: Option<String>) {
        self.facets[facet.index()] = value.filter(|value| !value.is_empty());
    }

    /// True when the search or a facet narrows the list beyond the scope.
    pub fn has_filters(&self) -> bool {
        !self.search.trim().is_empty() || self.facets.iter().any(Option::is_some)
    }

    /// The same scope and sort, without the search and the facets.
    pub fn without_filters(&self) -> Self {
        Self {
            scope: self.scope,
            sort: self.sort,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_query() -> ExploreQuery {
        let mut query = ExploreQuery {
            scope: Scope::All,
            search: "pinned  0.9.0 ".to_owned(),
            sort: Sort::P99,
            ..ExploreQuery::default()
        };
        let values = [
            "aws_i4i_4xlarge",
            "0.10.0 at 91e020636",
            "end_to_end",
            "3",
            "persisted",
            "100..1000",
        ];
        for (facet, value) in Facet::ALL.into_iter().zip(values) {
            query.set_facet(facet, Some(value.to_owned()));
        }
        query
    }

    #[test]
    fn given_full_view_when_written_and_read_back_should_round_trip() {
        let query = full_query();

        let pairs = query.to_pairs();

        assert_eq!(
            pairs,
            [
                ("scope", "all".to_owned()),
                ("q", "pinned  0.9.0 ".to_owned()),
                ("machine", "aws_i4i_4xlarge".to_owned()),
                ("version", "0.10.0 at 91e020636".to_owned()),
                ("kind", "end_to_end".to_owned()),
                ("nodes", "3".to_owned()),
                ("durability", "persisted".to_owned()),
                ("size", "100..1000".to_owned()),
                ("sort", "p99".to_owned()),
            ]
        );
        assert_eq!(ExploreQuery::from_pairs(pairs), query);
    }

    #[test]
    fn given_default_view_when_written_should_leave_url_bare() {
        assert_eq!(ExploreQuery::default().to_pairs(), []);
        assert_eq!(
            ExploreQuery::from_pairs::<&str, &str>([]),
            ExploreQuery::default()
        );
    }

    #[test]
    fn given_unknown_parameters_and_values_when_read_should_ignore_them() {
        let query = ExploreQuery::from_pairs([
            ("scope", "everything"),
            ("sort", "fastest"),
            ("utm_source", "chat"),
            ("machine", ""),
            ("Kind", "producer"),
        ]);

        assert_eq!(query, ExploreQuery::default());
    }

    #[test]
    fn given_repeated_parameters_when_read_should_keep_last_valid_value() {
        let query = ExploreQuery::from_pairs([
            ("sort", "throughput"),
            ("sort", "fastest"),
            ("machine", "atlas"),
            ("machine", "iggy-ci"),
            ("scope", "all"),
            ("scope", "official"),
        ]);

        assert_eq!(query.sort, Sort::Throughput);
        assert_eq!(query.facet(Facet::Machine), Some("iggy-ci"));
        assert_eq!(query.scope, Scope::Official);
    }

    #[test]
    fn given_filters_when_cleared_should_keep_scope_and_sort() {
        let query = full_query();

        let cleared = query.without_filters();

        assert!(query.has_filters());
        assert!(!cleared.has_filters());
        assert_eq!(cleared.scope, Scope::All);
        assert_eq!(cleared.sort, Sort::P99);
        assert!(
            !ExploreQuery {
                search: "  ".to_owned(),
                ..ExploreQuery::default()
            }
            .has_filters()
        );
    }
}
