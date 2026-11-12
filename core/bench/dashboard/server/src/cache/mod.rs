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

use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::Thresholds;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

mod loader;
mod series;
mod storage;
mod watcher;

use series::{SERIES_CACHE_MAX_BYTES, SeriesCache};

pub use series::{SeriesBuilder, SeriesKey, read_report};
pub use watcher::CacheWatcher;

pub type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;

#[derive(Debug)]
pub struct BenchmarkCache {
    /// The runs of the last load that read the results directory. A load builds the next runs
    /// aside and swaps them in whole, so a request during a reload, or after a failed one, reads
    /// the previous runs and their list together.
    runs: RwLock<Arc<Runs>>,

    /// Thresholds for the badges of the summaries
    thresholds: Thresholds,

    /// Reduced time series, built on the first request. A reload clears them.
    series: SeriesCache,

    /// Path to the results directory
    results_dir: PathBuf,
}

#[derive(Debug, Default)]
struct Runs {
    /// Map benchmark identifier to benchmark light report and path
    reports: HashMap<Uuid, (BenchmarkReportLight, PathBuf)>,

    /// Summaries of `reports`, newest first
    summaries: Arc<Vec<BenchmarkSummary>>,
}

impl BenchmarkCache {
    pub fn new(results_dir: PathBuf, thresholds: Thresholds) -> Self {
        Self {
            runs: RwLock::default(),
            thresholds,
            series: SeriesCache::new(SERIES_CACHE_MAX_BYTES),
            results_dir,
        }
    }
}
