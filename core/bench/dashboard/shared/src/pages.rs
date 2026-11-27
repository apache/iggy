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

//! The paths of the dashboard pages. The frontend router and the server routes must name the same
//! pages: a page that only the router knows works under `trunk serve`, but the server answers 404
//! for it. A test on each side compares its routes with [`PAGE_PATHS`].

use crate::omb::id::OmbRunId;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

/// Every page path, in the syntax of the frontend router: a segment that starts with `:` matches
/// any one segment.
pub const PAGE_PATHS: [&str; 12] = [
    "/",
    "/explore",
    "/runs/:uuid",
    "/benchmarks/:uuid",
    "/compare",
    "/compare/releases",
    "/compare/:left/:right",
    "/omb",
    "/omb/runs/:id",
    "/omb/compare",
    "/methodology",
    "/404",
];

/// Whether `path` has the form of a page path. It checks only the form, so `/runs/x` matches
/// although no run has the id `x`.
pub fn is_page_path(path: &str) -> bool {
    PAGE_PATHS
        .iter()
        .any(|pattern| matches_pattern(pattern, path))
}

/// The page of an iggy-bench run.
pub fn run_path(uuid: &Uuid) -> String {
    format!("/runs/{uuid}")
}

/// The page of an OMB run.
pub fn omb_run_path(id: OmbRunId) -> String {
    format!("/omb/runs/{id}")
}

/// Which runs the Explore page lists. The page and the server read it from the query the same
/// way, so the head that the server writes matches the list of the page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExploreScope {
    /// The runs whose suite the dashboard configuration names.
    #[default]
    Official,
    All,
}

/// A `scope` value that names no [`ExploreScope`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("Invalid Explore scope: '{0}'. Must be official or all")]
pub struct UnknownExploreScope(pub String);

impl ExploreScope {
    pub const ALL: [Self; 2] = [Self::Official, Self::All];
    /// The query parameter that holds the scope.
    pub const PARAM: &'static str = "scope";

    /// The scope of the decoded pairs of a query string. A value that doesn't parse is ignored,
    /// and when the parameter repeats, the last valid one wins.
    pub fn of_query<K: AsRef<str>, V: AsRef<str>>(pairs: impl IntoIterator<Item = (K, V)>) -> Self {
        pairs
            .into_iter()
            .filter(|(key, _)| key.as_ref() == Self::PARAM)
            .filter_map(|(_, value)| value.as_ref().parse().ok())
            .last()
            .unwrap_or_default()
    }

    /// The value of [`Self::PARAM`].
    pub const fn name(self) -> &'static str {
        match self {
            Self::Official => "official",
            Self::All => "all",
        }
    }

    /// The list of the scope without a search or a filter. It is a page of its own, and the
    /// canonical link of every filtered view of the scope.
    pub const fn path(self) -> &'static str {
        match self {
            Self::Official => "/explore",
            Self::All => "/explore?scope=all",
        }
    }

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

impl FromStr for ExploreScope {
    type Err = UnknownExploreScope;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|scope| scope.name() == name)
            .ok_or_else(|| UnknownExploreScope(name.to_owned()))
    }
}

impl fmt::Display for ExploreScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

fn matches_pattern(pattern: &str, path: &str) -> bool {
    let mut pattern_segments = pattern.split('/');
    let mut path_segments = path.split('/');
    loop {
        match (pattern_segments.next(), path_segments.next()) {
            (None, None) => return true,
            (Some(expected), Some(segment)) => {
                let matches = if expected.starts_with(':') {
                    !segment.is_empty()
                } else {
                    expected == segment
                };
                if !matches {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_page_paths_when_matching_should_accept_each_form() {
        for path in [
            "/",
            "/explore",
            "/runs/0eb36c51-3f2a-4c1b-9d8e-7a6b5c4d3e2f",
            "/compare/releases",
            "/compare/a/b",
            "/omb/runs/3f2a9c1b0eb36c51",
            "/404",
        ] {
            assert!(is_page_path(path), "{path}");
        }
    }

    #[test]
    fn given_other_paths_when_matching_should_reject_them() {
        for path in [
            "",
            "//",
            "/explore/",
            "/runs",
            "/runs/",
            "/runs/a/b",
            "/omb/runs/",
            "//evil.example",
            "/assets/style.css",
        ] {
            assert!(!is_page_path(path), "{path}");
        }
    }

    #[test]
    fn given_queries_when_reading_scope_should_take_last_valid_value_else_official() {
        for (pairs, expected) in [
            (vec![], ExploreScope::Official),
            (vec![("scope", "all")], ExploreScope::All),
            (
                vec![("kind", "producer"), ("scope", "all")],
                ExploreScope::All,
            ),
            (
                vec![("scope", "all"), ("scope", "official")],
                ExploreScope::Official,
            ),
            (
                vec![("scope", "all"), ("scope", "everything")],
                ExploreScope::All,
            ),
            (
                vec![("scope", "ALL"), ("Scope", "all")],
                ExploreScope::Official,
            ),
        ] {
            assert_eq!(ExploreScope::of_query(pairs.clone()), expected, "{pairs:?}");
        }
    }

    #[test]
    fn given_each_scope_when_linked_should_name_its_own_value_in_path() {
        for scope in ExploreScope::ALL {
            let query = scope.path().split_once('?').map_or("", |(_, query)| query);
            let pairs = query.split('&').filter_map(|pair| pair.split_once('='));

            assert!(scope.path().starts_with("/explore"), "{scope}");
            assert_eq!(ExploreScope::of_query(pairs), scope);
            assert_eq!(scope.name().parse(), Ok(scope));
        }
    }
}
