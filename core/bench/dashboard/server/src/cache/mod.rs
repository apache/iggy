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
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

mod loader;
mod series;
mod storage;
mod watcher;

use series::{SERIES_CACHE_MAX_BYTES, SeriesCache};

pub use series::{SeriesBuilder, SeriesKey};
pub use watcher::CacheWatcher;

pub type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;

#[derive(Debug)]
pub struct BenchmarkCache {
    /// Map benchmark identifier to benchmark light report and path
    benchmarks: DashMap<Uuid, (BenchmarkReportLight, PathBuf)>,

    /// Summaries of all runs, newest first. A load swaps in the new list when it finishes, so a
    /// request during a reload gets the previous list.
    summaries: RwLock<Arc<Vec<BenchmarkSummary>>>,

    /// Thresholds for the badges of the summaries
    thresholds: Thresholds,

    /// Reduced time series, built on the first request. A reload clears them.
    series: SeriesCache,

    /// Path to the results directory
    results_dir: PathBuf,

    /// Last reload request time
    last_reload_request: Arc<Mutex<Option<Instant>>>,
}

impl BenchmarkCache {
    pub fn new(results_dir: PathBuf, thresholds: Thresholds) -> Self {
        Self {
            benchmarks: DashMap::new(),
            summaries: RwLock::default(),
            thresholds,
            series: SeriesCache::new(SERIES_CACHE_MAX_BYTES),
            results_dir,
            last_reload_request: Arc::new(Mutex::new(None)),
        }
    }
}
