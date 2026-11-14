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

use super::{BenchmarkCache, Runs, SeriesCache};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError};
use uuid::Uuid;

impl BenchmarkCache {
    pub fn get_benchmark_json_path(&self, uuid: &Uuid) -> Option<PathBuf> {
        self.runs().reports.get(uuid).map(|(_, path)| path.clone())
    }

    pub fn get_benchmark_path(&self, uuid: &Uuid) -> Option<PathBuf> {
        self.runs()
            .reports
            .get(uuid)
            .map(|(_, path)| path.parent().unwrap().to_path_buf())
    }

    pub fn get_benchmark_report_light(&self, uuid: &Uuid) -> Option<BenchmarkReportLight> {
        self.runs()
            .reports
            .get(uuid)
            .map(|(report, _)| report.clone())
    }

    pub fn get_summaries(&self) -> Arc<Vec<BenchmarkSummary>> {
        Arc::clone(&self.runs().summaries)
    }

    pub fn series(&self) -> &SeriesCache {
        &self.series
    }

    /// Call it after every report read of the load. The cached series keep serving until then,
    /// and the new series generation keeps every build that began before the swap out of the
    /// cache.
    pub(super) fn replace_runs(&self, runs: Runs) {
        self.series.clear();
        *self.runs.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(runs);
    }

    fn runs(&self) -> Arc<Runs> {
        // The writer only swaps an Arc, so a poisoned lock still holds whole runs.
        Arc::clone(&self.runs.read().unwrap_or_else(PoisonError::into_inner))
    }
}
