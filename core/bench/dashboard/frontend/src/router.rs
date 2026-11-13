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

use yew_router::prelude::*;

#[derive(Debug, Clone, PartialEq, Routable)]
pub enum AppRoute {
    #[at("/")]
    Home,
    #[at("/benchmarks")]
    Explore,
    #[at("/benchmarks/:uuid")]
    Benchmark { uuid: String },
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
    #[at("/methodology")]
    Methodology,
    #[not_found]
    #[at("/404")]
    NotFound,
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
}
