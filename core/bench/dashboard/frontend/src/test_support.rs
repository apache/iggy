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

//! Runs for the page tests, summarized by the shared crate the way the server does it. The OMB runs
//! are the real results in `core/bench/dashboard/fixtures/omb`.

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::Thresholds;
use bench_dashboard_shared::metrics::GroupMetrics;
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::result::OmbResult;
use bench_dashboard_shared::omb::setup::{OmbSetup, OmbSidecar};
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary, OmbRunTime};
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::group_metrics_kind::GroupMetricsKind;
use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bench_report::params::BenchmarkParams;
use chrono::{DateTime, TimeDelta, Utc};
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

/// The modification time of every OMB fixture file, so no test depends on the checkout.
const OMB_MODIFIED: &str = "2026-11-13T09:30:00Z";

/// One OMB run directory: the result file as OMB wrote it, and its optional `setup.toml`.
pub struct OmbFixture {
    pub file_name: &'static str,
    pub json: &'static [u8],
    pub sidecar: Option<&'static str>,
}

macro_rules! omb_fixture {
    ($directory:literal, $file_name:literal) => {
        OmbFixture {
            file_name: $file_name,
            json: include_bytes!(concat!("../../fixtures/omb/", $directory, "/", $file_name)),
            sidecar: None,
        }
    };
    ($directory:literal, $file_name:literal, with_sidecar) => {
        OmbFixture {
            sidecar: Some(include_str!(concat!(
                "../../fixtures/omb/",
                $directory,
                "/setup.toml"
            ))),
            ..omb_fixture!($directory, $file_name)
        }
    };
}

/// 0.10.0-rc.1 on 3 nodes of i4i.4xlarge with 2 client hosts: 12 topics of 1 partition, 1 producer
/// and 1 consumer per topic, 1000 B messages, 18 windows, driver `Iggy`.
pub const OMB_RC1_DEFAULT: OmbFixture = omb_fixture!(
    "rc1-12-topics-150mb",
    "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.json",
    with_sidecar
);
/// The workload and setup of [`OMB_RC1_DEFAULT`] with the driver configuration `Iggy-persisted`.
pub const OMB_RC1_PERSISTED: OmbFixture = omb_fixture!(
    "rc1-12-topics-150mb-persisted",
    "12-topics-1-partition-1kb-150mb-Iggy-persisted-2026-11-06-13-31-22.json",
    with_sidecar
);
/// The workload, driver and machines of [`OMB_RC1_DEFAULT`] on 0.9.1-edge.8, so it comes before
/// it in version order. Its sidecar has no `data_file_system` property.
pub const OMB_EDGE8_DEFAULT: OmbFixture = omb_fixture!(
    "edge8-12-topics-150mb",
    "12-topics-1-partition-1kb-150mb-Iggy-2026-11-03-06-24-42.json",
    with_sidecar
);
/// 0.9.1-edge.8 on 3 nodes of i7i.xlarge: 1 topic of 100 partitions, 4 producers and 4 consumers,
/// 1024 B messages, 30 windows. Every workload field differs from [`OMB_RC1_DEFAULT`].
pub const OMB_EDGE8_100_PARTITIONS: OmbFixture = omb_fixture!(
    "edge8-i7i-100-partitions-125mb",
    "1-topic-100-partitions-1kb-125mb-Iggy-2026-11-02-14-55-18.json",
    with_sidecar
);
/// No consumers, so the end-to-end latency is not measured. No sidecar, so the time comes from the
/// file name.
pub const OMB_NO_CONSUMERS: OmbFixture = omb_fixture!(
    "edge-12-topics-75mb-no-consumers",
    "12-topics-1-partition-1kb-75mb-no-consumers-Iggy-2026-10-22-10-21-15.json"
);
/// Publish errors in every window, so it has the publish errors badge. Driver `Iggy-persisted`, no
/// sidecar.
pub const OMB_PUBLISH_ERRORS: OmbFixture = omb_fixture!(
    "edge-12-topics-75mb-persisted-errors",
    "12-topics-1-partition-1kb-75mb-Iggy-persisted-2026-10-15-13-33-17.json"
);

pub const OMB_ALL: [&OmbFixture; 6] = [
    &OMB_RC1_DEFAULT,
    &OMB_RC1_PERSISTED,
    &OMB_EDGE8_DEFAULT,
    &OMB_EDGE8_100_PARTITIONS,
    &OMB_NO_CONSUMERS,
    &OMB_PUBLISH_ERRORS,
];

impl OmbFixture {
    pub fn id(&self) -> OmbRunId {
        OmbRunId::of_file(self.json)
    }

    pub fn result(&self) -> OmbResult {
        OmbResult::try_from(self.json).expect("fixture results parse")
    }

    pub fn setup(&self) -> Option<OmbSetup> {
        self.sidecar.map(|text| {
            text.parse::<OmbSidecar>()
                .expect("fixture sidecars parse")
                .setup
        })
    }

    /// The summary that the server sends in `GET /api/omb/runs`.
    pub fn summary(&self) -> OmbRunSummary {
        OmbRunSummary::new(
            self.id(),
            self.file_name,
            omb_modified(),
            &self.result(),
            self.setup(),
        )
    }

    /// The detail that the server sends in `GET /api/omb/runs/{id}`.
    pub fn detail(&self) -> OmbRunDetail {
        OmbRunDetail::new(self.summary(), &self.result())
    }
}

pub fn omb_modified() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(OMB_MODIFIED)
        .expect("the test time parses")
        .with_timezone(&Utc)
}

/// Every OMB fixture, newest first, as the server lists them.
pub fn omb_summaries() -> Vec<OmbRunSummary> {
    let mut runs: Vec<OmbRunSummary> = OMB_ALL.iter().map(|fixture| fixture.summary()).collect();
    runs.sort_by(|left, right| {
        right
            .time
            .instant
            .cmp(&left.time.instant)
            .then_with(|| left.id.cmp(&right.id))
    });
    runs
}

/// The answer of `GET /api/omb/runs` for a server with the OMB fixtures.
pub fn omb_run_list() -> OmbRunList {
    OmbRunList {
        enabled: true,
        runs: omb_summaries(),
    }
}

/// `run` as another run of its line, `hours` later and with its own id, for example
/// `00000000000000aa`.
pub fn omb_rerun(run: &OmbRunSummary, id: &str, hours: i64) -> OmbRunSummary {
    let mut rerun = run.clone();
    rerun.id = id.parse().expect("a test id parses");
    rerun.time = OmbRunTime {
        instant: run.time.instant + TimeDelta::hours(hours),
        ..run.time.clone()
    };
    rerun
}
