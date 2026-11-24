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
}
