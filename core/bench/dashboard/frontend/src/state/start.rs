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
use bench_dashboard_shared::omb::summary::OmbRunList;
use bench_dashboard_shared::summary::BenchmarkSummary;
use serde::{Deserialize, Serialize};

/// API answers at the time of the render. A page gets only the parts that it reads, so an OMB page
/// does not carry the iggy-bench runs. The browser fetches a part that is `None` when a page needs
/// it, as it does on a page that the browser renders alone.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartData {
    /// The answer of `/api/benchmarks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summaries: Option<Vec<BenchmarkSummary>>,
    /// The answer of `/api/config`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<DashboardConfig>,
    /// The answer of `/api/omb/runs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omb_runs: Option<OmbRunList>,
}
