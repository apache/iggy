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

//! The data that the server renders a page with. The server writes it into the page as JSON, and
//! the browser starts from it, so its first render gives the HTML that the server sent. Hydration
//! needs that match.

use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary};
use bench_dashboard_shared::summary::BenchmarkSummary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pages;

/// API answers at the time of the render. A page gets only the parts that it reads, so an OMB page
/// does not carry the iggy-bench runs. The browser fetches a part that is `None` when a page needs
/// it, as it does on a page that the browser renders alone.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartData {
    /// The answer of `/api/benchmarks`, on a page that reads every run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summaries: Option<Vec<BenchmarkSummary>>,
    /// The answer of `/api/config`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<DashboardConfig>,
    /// The answer of `/api/omb/runs`, on a page that reads every OMB run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omb_runs: Option<OmbRunList>,
    /// On the page of one iggy-bench run, the runs that the page reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<PageRuns<Uuid, Vec<BenchmarkSummary>>>,
    /// On the page of one OMB run, the OMB runs that the page reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omb_run: Option<PageRuns<OmbRunId, OmbRunList>>,
}

/// The runs that the page of the run `id` reads: the run, the runs of its history and its related
/// runs. A run page carries these instead of every run, so its size does not grow with the number
/// of runs. The browser fetches every run when the reader moves to a page that needs them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageRuns<Id, Runs> {
    pub id: Id,
    pub runs: Runs,
}

impl StartData {
    /// The start data of the page of `run`, one of `summaries`.
    pub fn run_page(
        run: &BenchmarkSummary,
        summaries: &[BenchmarkSummary],
        config: &DashboardConfig,
    ) -> Self {
        let runs = summaries
            .iter()
            .filter(|other| pages::run::reads(run, other))
            .cloned()
            .collect();
        Self {
            config: Some(config.clone()),
            run: Some(PageRuns { id: run.uuid, runs }),
            ..Self::default()
        }
    }

    /// The start data of the page of `run`, one of the runs of `list`.
    pub fn omb_run_page(run: &OmbRunSummary, list: &OmbRunList) -> Self {
        let runs = list
            .runs
            .iter()
            .filter(|other| pages::omb::run::reads(run, other))
            .cloned()
            .collect();
        Self {
            omb_run: Some(PageRuns {
                id: run.id,
                runs: OmbRunList {
                    enabled: list.enabled,
                    runs,
                },
            }),
            ..Self::default()
        }
    }
}
