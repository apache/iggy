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

//! The OMB run list's view as the URL holds it: the search text, one value per facet and the sort.
//! A link is never an error, because the page ignores what it can't use.

use serde::{Deserialize, Serialize};

use crate::api::{api_name, from_api_name};

const SEARCH_PARAM: &str = "q";
const SORT_PARAM: &str = "sort";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sort {
    /// By run time.
    #[default]
    Newest,
    /// Highest publish MiB/s first.
    PublishThroughput,
    /// Lowest first.
    PublishP99,
    /// Lowest first. The runs without end-to-end numbers come last.
    EndToEndP99,
}

/// A filter that takes one value. The order is the order on the page: the workload and the setup,
/// as the columns of the list, then the parts of a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Facet {
    /// The `name` from the workload file.
    Workload,
    Driver,
    /// The environment of the setup sidecar.
    Environment,
    /// The Iggy version of the setup sidecar.
    Version,
    /// The suite of the setup sidecar.
    Suite,
    MessageSize,
    /// Topics times partitions per topic.
    Topology,
    /// Producers and consumers per topic.
    Actors,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OmbQuery {
    /// As typed, so the search box keeps the spaces between words.
    pub search: String,
    /// One value per facet, as written in the URL. Never empty, because [`Self::set_facet`] keeps
    /// empty values out.
    pub(super) facets: [Option<String>; Facet::COUNT],
    pub sort: Sort,
}

impl Sort {
    pub const ALL: [Self; 4] = [
        Self::Newest,
        Self::PublishThroughput,
        Self::PublishP99,
        Self::EndToEndP99,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Newest => "Newest",
            Self::PublishThroughput => "Highest publish throughput",
            Self::PublishP99 => "Lowest publish P99",
            Self::EndToEndP99 => "Lowest end-to-end P99",
        }
    }
}

impl Facet {
    pub const ALL: [Self; 8] = [
        Self::Workload,
        Self::Driver,
        Self::Environment,
        Self::Version,
        Self::Suite,
        Self::MessageSize,
        Self::Topology,
        Self::Actors,
    ];
    pub const COUNT: usize = Self::ALL.len();

    /// The position of the facet in [`Self::ALL`], and in every array indexed by facet.
    pub const fn index(self) -> usize {
        self as usize
    }

    const fn param(self) -> &'static str {
        match self {
            Self::Driver => "driver",
            Self::Workload => "workload",
            Self::MessageSize => "size",
            Self::Topology => "topology",
            Self::Actors => "actors",
            Self::Environment => "environment",
            Self::Version => "version",
            Self::Suite => "suite",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Driver => "Driver",
            Self::Workload => "Workload",
            Self::MessageSize => "Message size",
            Self::Topology => "Topics × partitions",
            Self::Actors => "Actors per topic",
            Self::Environment => "Environment",
            Self::Version => "Version",
            Self::Suite => "Suite",
        }
    }

    /// The label of the option that clears the facet.
    pub const fn any_label(self) -> &'static str {
        match self {
            Self::Driver => "All drivers",
            Self::Workload => "All workloads",
            Self::MessageSize => "Any size",
            Self::Topology => "Any topology",
            Self::Actors => "Any actors",
            Self::Environment => "Any environment",
            Self::Version => "All versions",
            Self::Suite => "All suites",
        }
    }
}

impl OmbQuery {
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

    /// The same sort, without the search and the facets.
    pub fn without_filters(&self) -> Self {
        Self {
            sort: self.sort,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values as the catalog writes them for the fixtures, with the characters that a URL must
    /// escape: spaces, `/` and `,`.
    fn full_query() -> OmbQuery {
        let mut query = OmbQuery {
            search: "huge_pages  rc.1 ".to_owned(),
            sort: Sort::EndToEndP99,
            ..OmbQuery::default()
        };
        let values = [
            "12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s",
            "Iggy-persisted",
            "AWS i4i.4xlarge, 3 nodes",
            "0.10.0-rc.1",
            "cluster-3-nodes",
            "1000",
            "12x1",
            "1p1c",
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
                ("q", "huge_pages  rc.1 ".to_owned()),
                (
                    "workload",
                    "12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s".to_owned()
                ),
                ("driver", "Iggy-persisted".to_owned()),
                ("environment", "AWS i4i.4xlarge, 3 nodes".to_owned()),
                ("version", "0.10.0-rc.1".to_owned()),
                ("suite", "cluster-3-nodes".to_owned()),
                ("size", "1000".to_owned()),
                ("topology", "12x1".to_owned()),
                ("actors", "1p1c".to_owned()),
                ("sort", "end_to_end_p99".to_owned()),
            ]
        );
        assert_eq!(OmbQuery::from_pairs(pairs), query);
    }

    #[test]
    fn given_each_sort_when_written_and_read_back_should_round_trip() {
        let names: Vec<_> = Sort::ALL
            .into_iter()
            .map(|sort| {
                let pairs = OmbQuery {
                    sort,
                    ..OmbQuery::default()
                }
                .to_pairs();
                assert_eq!(OmbQuery::from_pairs(pairs.clone()).sort, sort);
                pairs
            })
            .collect();

        assert_eq!(
            names,
            [
                vec![],
                vec![("sort", "publish_throughput".to_owned())],
                vec![("sort", "publish_p99".to_owned())],
                vec![("sort", "end_to_end_p99".to_owned())],
            ]
        );
    }

    #[test]
    fn given_default_view_when_written_should_leave_url_bare() {
        assert_eq!(OmbQuery::default().to_pairs(), []);
        assert_eq!(OmbQuery::from_pairs::<&str, &str>([]), OmbQuery::default());
        assert_eq!(
            OmbQuery::from_pairs([("sort", "newest")]),
            OmbQuery::default()
        );
    }

    #[test]
    fn given_unknown_parameters_and_values_when_read_should_ignore_them() {
        let query = OmbQuery::from_pairs([
            ("sort", "fastest"),
            ("sort", "p99"),
            ("utm_source", "chat"),
            ("driver", ""),
            ("Driver", "Iggy"),
            ("scope", "all"),
        ]);

        assert_eq!(query, OmbQuery::default());
    }

    #[test]
    fn given_repeated_parameters_when_read_should_keep_last_valid_value() {
        let query = OmbQuery::from_pairs([
            ("sort", "publish_p99"),
            ("sort", "fastest"),
            ("driver", "Iggy"),
            ("driver", "Iggy-persisted"),
            ("q", "ext4"),
            ("q", "edge.8"),
        ]);

        assert_eq!(query.sort, Sort::PublishP99);
        assert_eq!(query.facet(Facet::Driver), Some("Iggy-persisted"));
        assert_eq!(query.search, "edge.8");
    }

    #[test]
    fn given_filters_when_cleared_should_keep_sort() {
        let query = full_query();

        let cleared = query.without_filters();

        assert_eq!(
            cleared,
            OmbQuery {
                sort: Sort::EndToEndP99,
                ..OmbQuery::default()
            }
        );
        assert_eq!(cleared.to_pairs(), [("sort", "end_to_end_p99".to_owned())]);
    }
}
