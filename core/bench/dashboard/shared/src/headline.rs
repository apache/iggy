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

//! The runs behind each headline workload of the configuration file. The Overview cards and the
//! head tags of the Overview read the same runs, so the description of the page states the numbers
//! that its cards show.

use crate::config::{DashboardConfig, HeadlineConfig};
use crate::history::{HistoryLine, LineVersion};
use crate::summary::BenchmarkSummary;

/// The official runs of one headline workload, on the history line of its newest release run. Runs
/// with the same labels on another line ran a different workload, so they're never compared.
pub struct HeadlineLine<'a> {
    pub config: &'a HeadlineConfig,
    /// Releases and builds.
    pub all: HistoryLine<'a>,
    pub releases: HistoryLine<'a>,
}

impl<'a> HeadlineLine<'a> {
    /// One line per headline workload, in configuration file order.
    pub fn all_of(summaries: &'a [BenchmarkSummary], config: &'a DashboardConfig) -> Vec<Self> {
        let official = official_runs(summaries, config);
        config
            .headlines
            .iter()
            .map(|headline| Self::new(headline, &official))
            .collect()
    }

    pub fn new(config: &'a HeadlineConfig, official: &[&'a BenchmarkSummary]) -> Self {
        let runs: Vec<_> = official
            .iter()
            .copied()
            .filter(|summary| {
                summary.suite.as_deref() == Some(config.suite.as_str())
                    && summary.workload_key.workload.as_deref() == Some(config.workload.as_str())
            })
            .collect();
        let key = newest_release(runs.iter().copied()).map(BenchmarkSummary::history_key);
        let on_line: Vec<_> = runs
            .into_iter()
            .filter(|summary| key.as_ref() == Some(&summary.history_key()))
            .collect();
        Self {
            config,
            releases: HistoryLine::from_runs(on_line.iter().copied().filter(|run| run.release)),
            all: HistoryLine::from_runs(on_line),
        }
    }

    /// The line's newest release version and that version's newest run.
    pub fn newest_release(&self) -> Option<(&LineVersion<'a>, &'a BenchmarkSummary)> {
        let version = self.releases.versions.last()?;
        Some((version, *version.runs.last()?))
    }
}

/// The runs of the official suites of the configuration file.
pub fn official_runs<'a>(
    summaries: &'a [BenchmarkSummary],
    config: &DashboardConfig,
) -> Vec<&'a BenchmarkSummary> {
    summaries
        .iter()
        .filter(|summary| config.is_official(summary.suite.as_deref()))
        .collect()
}

/// The newest release run in the shared version order. Builds never count, release candidates
/// included.
pub fn newest_release<'a>(
    runs: impl IntoIterator<Item = &'a BenchmarkSummary>,
) -> Option<&'a BenchmarkSummary> {
    runs.into_iter()
        .filter(|summary| summary.release)
        .max_by(|left, right| left.version_order.cmp(&right.version_order))
}
