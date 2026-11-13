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

//! Real reports with their time series cut, from `core/bench/dashboard/fixtures/results`.

use crate::BenchmarkReportLight;
use crate::config::Thresholds;

/// `server-0.4.202`, 8 producers and 8 consumers, no rate limit.
pub(crate) const OLD_ERA_RUN: &str =
    include_str!("../../fixtures/results/old-0.4.202-pinned-producer-and-consumer/report.json");
/// 0.9.0 at `71f29618b`, 20 producers, no rate limit. The report says 2731 MB/s.
pub(crate) const LIVE_UNCAPPED_RUN: &str =
    include_str!("../../fixtures/results/live-0.9.0-pinned-producer-20p/report.json");
/// 0.10.0-rc.1, 1 producer, persisted, rate limit 500 MB.
pub(crate) const PERSISTED_RUN: &str =
    include_str!("../../fixtures/results/rc1-pinned-producer-1p-persisted-500MB/report.json");
/// 0.10.0-rc.1 on 3 nodes, 20 producers, rate limit 800 MB, no recorded options.
pub(crate) const CLUSTER_RUN: &str =
    include_str!("../../fixtures/results/rc1-cluster3-pinned-producer-20p-800MB/report.json");
/// Written by a bench that records options, data volume, suite and workload.
pub(crate) const PHASE_1A_RUN: &str =
    include_str!("../../fixtures/results/phase1a-pinned-producer-2p-20MB/report.json");

/// Fixed thresholds for the unit tests, so editing `dashboard.toml` doesn't change what the tests
/// check.
pub(crate) const THRESHOLDS: Thresholds = Thresholds {
    throughput_pct: 3.0,
    p50_pct: 60.0,
    p90_pct: 60.0,
    p95_pct: 60.0,
    p99_pct: 60.0,
    p999_pct: 100.0,
    max_time_spread_pct: 10.0,
};

pub(crate) fn report(json: &str) -> BenchmarkReportLight {
    serde_json::from_str(json).expect("fixture reports parse")
}
