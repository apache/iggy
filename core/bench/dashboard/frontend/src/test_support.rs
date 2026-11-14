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

//! Runs for the page tests, summarized by the shared crate the way the server does it.

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::Thresholds;
use bench_dashboard_shared::metrics::GroupMetrics;
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::group_metrics_kind::GroupMetricsKind;
use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bench_report::params::BenchmarkParams;
use uuid::Uuid;

pub const RELEASE: &str = "server-0.9.0";
pub const OLD_RELEASE: &str = "server-0.8.0";
pub const CANDIDATE: &str = "server-0.10.0-rc.1";
pub const COMMIT: &str = "91e020636be5c0d1";
pub const SERVER_VERSION: &str = "0.10.0";
pub const RATE_LIMIT: &str = "800.00 MB";
pub const MACHINE: &str = "atlas";
pub const THRESHOLDS: Thresholds = Thresholds {
    throughput_pct: 3.0,
    p50_pct: 60.0,
    p90_pct: 60.0,
    p95_pct: 60.0,
    p99_pct: 60.0,
    p999_pct: 100.0,
    max_time_spread_pct: 10.0,
};
const MESSAGES_PER_MEGABYTE: f64 = 1000.0;

pub fn params(kind: BenchmarkKind, producers: u32, consumers: u32) -> BenchmarkParams {
    BenchmarkParams {
        benchmark_kind: kind,
        producers,
        consumers,
        message_size: BenchmarkNumericParameter::Value(1000),
        messages_per_batch: BenchmarkNumericParameter::Value(1000),
        ..BenchmarkParams::default()
    }
}

pub fn producer() -> BenchmarkParams {
    params(BenchmarkKind::PinnedProducer, 1, 0)
}

pub fn consumer() -> BenchmarkParams {
    params(BenchmarkKind::PinnedConsumer, 0, 1)
}

pub fn rate_limited(mut params: BenchmarkParams) -> BenchmarkParams {
    params.rate_limit = Some(RATE_LIMIT.to_owned());
    params
}

/// The summary that the shared crate builds from a report with these parameters.
pub fn summarize(params: BenchmarkParams) -> BenchmarkSummary {
    let report = BenchmarkReportLight {
        params,
        ..BenchmarkReportLight::default()
    };
    BenchmarkSummary::new(&report, &THRESHOLDS)
}

/// Run `id` started on day `id` of November 2026, so a higher id is a newer run.
pub fn run(id: u128, params: BenchmarkParams, machine: &str, gitref: &str) -> BenchmarkSummary {
    let mut report = BenchmarkReportLight {
        uuid: Uuid::from_u128(id),
        timestamp: format!("2026-11-{id:02}T09:30:00Z"),
        params,
        ..BenchmarkReportLight::default()
    };
    report.params.gitref = Some(gitref.to_owned());
    report.server_stats.iggy_server_version = SERVER_VERSION.to_owned();
    report.hardware.identifier = Some(machine.to_owned());
    BenchmarkSummary::new(&report, &THRESHOLDS)
}

/// A rate-limited producer run on [`MACHINE`].
pub fn limited(id: u128, gitref: &str) -> BenchmarkSummary {
    run(id, rate_limited(producer()), MACHINE, gitref)
}

/// A rate-limited producer run on [`MACHINE`] of a build at `commit`, committed at
/// `commit_date`.
pub fn build(id: u128, commit: &str, commit_date: &str) -> BenchmarkSummary {
    let mut params = rate_limited(producer());
    params.gitref_date = Some(commit_date.to_owned());
    run(id, params, MACHINE, commit)
}

/// The main group gets this throughput and P99. P50 is half of P99, and every other
/// percentile equals P99.
pub fn with_numbers(mut summary: BenchmarkSummary, throughput: f64, p99: f64) -> BenchmarkSummary {
    summary.groups = vec![GroupMetrics {
        kind: summary.kind_family.primary_group(),
        throughput_megabytes_per_second: throughput,
        throughput_messages_per_second: throughput * MESSAGES_PER_MEGABYTE,
        p50_latency_ms: p99 / 2.0,
        p90_latency_ms: p99,
        p95_latency_ms: p99,
        p99_latency_ms: p99,
        p999_latency_ms: p99,
        time_spread_pct: 0.0,
    }];
    summary
}

/// The report summary of a producer group that recorded every latency.
pub fn group_summary() -> BenchmarkGroupMetricsSummary {
    BenchmarkGroupMetricsSummary {
        kind: GroupMetricsKind::Producers,
        total_throughput_megabytes_per_second: 800.0,
        total_throughput_messages_per_second: 800_000.0,
        average_throughput_megabytes_per_second: 800.0,
        average_throughput_messages_per_second: 800_000.0,
        average_p50_latency_ms: 1.0,
        average_p90_latency_ms: 2.0,
        average_p95_latency_ms: 3.0,
        average_p99_latency_ms: 5.0,
        average_p999_latency_ms: 8.0,
        average_p9999_latency_ms: 9.0,
        average_latency_ms: 1.4,
        average_median_latency_ms: 1.0,
        min_latency_ms: 0.1,
        max_latency_ms: 10.0,
        std_dev_latency_ms: 0.7,
    }
}
