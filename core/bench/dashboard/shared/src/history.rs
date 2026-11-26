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

//! The history line of a workload: its runs on one machine and topology, grouped by version. The
//! run page, the release compare and the Overview headlines read their changes from it.

use crate::delta::{Metric, ValueRange};
use crate::summary::BenchmarkSummary;

/// The runs of one workload on one machine and topology, grouped by version. A version is a
/// summary's version text, and it takes the place of its first run in version order, so interleaved
/// runs of two builds still give one order.
#[derive(Debug)]
pub struct HistoryLine<'a> {
    /// Oldest first.
    pub versions: Vec<LineVersion<'a>>,
}

#[derive(Debug)]
pub struct LineVersion<'a> {
    pub version: &'a str,
    /// In version order.
    pub runs: Vec<&'a BenchmarkSummary>,
}

/// The runs that a change compares.
#[derive(Debug)]
pub struct VersionStep<'a> {
    /// The version that the change compares with. On the run page, that's the version right before
    /// the run's own version on its history line.
    pub previous: &'a str,
    pub base: Vec<&'a BenchmarkSummary>,
    /// The runs of the run's own version, the run itself included.
    pub head: Vec<&'a BenchmarkSummary>,
}

impl<'a> HistoryLine<'a> {
    /// The history line of `run` within `summaries`.
    pub fn of(run: &BenchmarkSummary, summaries: &'a [BenchmarkSummary]) -> Self {
        let line_key = run.history_key();
        Self::from_runs(
            summaries
                .iter()
                .filter(|summary| summary.history_key() == line_key),
        )
    }

    /// Groups `runs`, which share one history key, by version.
    pub fn from_runs(runs: impl IntoIterator<Item = &'a BenchmarkSummary>) -> Self {
        let mut line: Vec<_> = runs.into_iter().collect();
        line.sort_by(|left, right| {
            left.version_order
                .cmp(&right.version_order)
                .then_with(|| left.version.cmp(&right.version))
                .then_with(|| left.uuid.cmp(&right.uuid))
        });
        let mut versions: Vec<LineVersion<'a>> = Vec::new();
        for summary in line {
            match versions
                .iter_mut()
                .find(|known| known.version == summary.version)
            {
                Some(known) => known.runs.push(summary),
                None => versions.push(LineVersion {
                    version: &summary.version,
                    runs: vec![summary],
                }),
            }
        }
        Self { versions }
    }

    /// `None` when no version comes before the version of `run`.
    pub fn step(&self, run: &BenchmarkSummary) -> Option<VersionStep<'a>> {
        let index = self
            .versions
            .iter()
            .position(|version| version.version == run.version)?;
        let base = self.versions.get(index.checked_sub(1)?)?;
        let head = self.versions.get(index)?;
        Some(VersionStep::between(base, head))
    }
}

impl LineVersion<'_> {
    /// The median of `metric` over the runs' main group.
    pub fn median(&self, metric: Metric) -> Option<f64> {
        let group = self.runs.first()?.kind_family.primary_group();
        ValueRange::of(&self.runs, metric, group).map(|range| range.median)
    }
}

impl<'a> VersionStep<'a> {
    pub fn between(base: &LineVersion<'a>, head: &LineVersion<'a>) -> Self {
        Self {
            previous: base.version,
            base: base.runs.clone(),
            head: head.runs.clone(),
        }
    }
}
