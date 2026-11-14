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

use std::fs;

use iggy::prelude::*;
use iggy_common::TransportProtocol;
use integration::bench_utils::{BENCH_WAIT_TIMEOUT, bench_run_dirs, run_bench_and_wait_for_finish};
use integration::harness::{TestHarness, TestServerConfig, cargo_bin_path};
use serde_json::Value;
use serial_test::parallel;

const BENCHMARK_DATA_BYTES: u64 = 5_000_000;
const REPORT_TEST_DATA: &str = "1MiB";
const REPORT_FILE: &str = "report.json";
const BENCH_LOG_FILE: &str = "bench.log";
const TEST_SUITE: &str = "integration-suite";
const TEST_WORKLOAD: &str = "pinned-producer-1p";
const REDACTED_PASSWORD_ARGUMENT: &str = "--password '***'";
const TEST_SECRET: &str = "sensitive-benchmark-test-value";
const WARMUP_MESSAGES_PER_BATCH: &str = "32";
const WARMUP_MESSAGE_SIZE: &str = "128";
const BENCH_STREAM: &str = "bench-stream-1";
const BENCH_TOPIC: &str = "topic-1";
const RATE_TEST_BYTES: u32 = 2_048;
const RATE_TEST_BYTES_PER_SECOND: u32 = 1_024;
const MIN_RATE_DURATION_RATIO: f64 = 0.8;

#[tokio::test]
#[parallel]
async fn given_rate_limit_when_running_end_to_end_benchmark_should_limit_aggregate_bytes() {
    for high_level in [false, true] {
        for group in [false, true] {
            let mut harness = TestHarness::builder()
                .cluster_nodes(1)
                .server(TestServerConfig::default())
                .build()
                .unwrap();
            harness.start().await.unwrap();
            let output_dir = tempfile::tempdir().unwrap();
            let server_address = harness.server().raw_tcp_addr().unwrap();
            let mut command = assert_cmd::Command::new(cargo_bin_path("iggy-bench").unwrap());
            command.args([
                "--total-data",
                &format!("{RATE_TEST_BYTES}B"),
                "--rate-limit",
                &format!("{RATE_TEST_BYTES_PER_SECOND}B"),
                "--messages-per-batch",
                "1",
                "--message-size",
                "64",
                "--warmup-time",
                "100ms",
            ]);
            if high_level {
                command.arg("--high-level-api");
            }
            if group {
                command.args([
                    "end-to-end-producing-consumer-group",
                    "--producers",
                    "1",
                    "--consumers",
                    "1",
                    "--consumer-groups",
                    "1",
                    "--partitions",
                    "1",
                ]);
            } else {
                command.args([
                    "end-to-end-producing-consumer",
                    "--producing-consumers",
                    "1",
                ]);
            }
            command
                .args([
                    "--streams",
                    "1",
                    "tcp",
                    "--server-address",
                    &server_address,
                    "output",
                    "--output-dir",
                ])
                .arg(output_dir.path())
                .timeout(BENCH_WAIT_TIMEOUT)
                .assert()
                .success();
            let run_dirs = bench_run_dirs(output_dir.path());
            let [run_dir] = run_dirs.as_slice() else {
                panic!("expected one run directory, found {run_dirs:?}");
            };
            let report: Value =
                serde_json::from_str(&fs::read_to_string(run_dir.join(REPORT_FILE)).unwrap())
                    .unwrap();
            let summary = &report["individual_metrics"][0]["summary"];
            assert_eq!(
                summary["total_user_data_bytes"].as_u64().unwrap(),
                u64::from(RATE_TEST_BYTES)
            );
            let elapsed = summary["total_time_secs"].as_f64().unwrap();
            // The last record precedes its pacing wait, and the limiter permits a short burst.
            let minimum_duration = f64::from(RATE_TEST_BYTES)
                / f64::from(RATE_TEST_BYTES_PER_SECOND)
                * MIN_RATE_DURATION_RATIO;
            assert!(
                elapsed >= minimum_duration,
                "high_level={high_level}, group={group}: {RATE_TEST_BYTES} aggregate bytes took {elapsed}s, expected at least {minimum_duration}s"
            );
        }
    }
}

#[tokio::test]
#[parallel]
async fn given_populated_topic_when_warming_consumer_group_should_replay_without_refused_deletes() {
    for high_level in [false, true] {
        let mut harness = TestHarness::builder()
            .cluster_nodes(1)
            .server(TestServerConfig::default())
            .build()
            .unwrap();
        harness.start().await.unwrap();
        let server_address = harness.server().raw_tcp_addr().unwrap();
        assert_cmd::Command::new(cargo_bin_path("iggy-bench").unwrap())
            .args([
                "--total-data",
                REPORT_TEST_DATA,
                "--messages-per-batch",
                WARMUP_MESSAGES_PER_BATCH,
                "--message-size",
                WARMUP_MESSAGE_SIZE,
                "--reuse-streams",
                "balanced-producer",
                "--producers",
                "1",
                "--streams",
                "1",
                "--partitions",
                "4",
                "tcp",
                "--server-address",
                &server_address,
            ])
            .timeout(BENCH_WAIT_TIMEOUT)
            .assert()
            .success();

        // An empty partition must not prevent replay of the populated partitions.
        harness
            .root_client()
            .await
            .unwrap()
            .create_partitions(
                &Identifier::named(BENCH_STREAM).unwrap(),
                &Identifier::named(BENCH_TOPIC).unwrap(),
                1,
            )
            .await
            .unwrap();

        let mut command = assert_cmd::Command::new(cargo_bin_path("iggy-bench").unwrap());
        command.args([
            "--total-data",
            REPORT_TEST_DATA,
            "--messages-per-batch",
            WARMUP_MESSAGES_PER_BATCH,
            "--warmup-time",
            "1s",
            "--reuse-streams",
        ]);
        if high_level {
            command.arg("--high-level-api");
        }
        command.args([
            "balanced-consumer-group",
            "--consumers",
            "2",
            "--consumer-groups",
            "1",
            "--streams",
            "1",
            "tcp",
            "--server-address",
            &server_address,
        ]);
        command.timeout(BENCH_WAIT_TIMEOUT).assert().success();
        let (stdout, stderr) = harness.server().collect_logs();
        for log in [stdout, stderr] {
            assert!(
                !log.lines()
                    .any(|line| (line.contains("DeleteConsumerOffset")
                        || line.contains("delete_consumer_offset"))
                        && line.contains("WARN")),
                "high_level={high_level}: {log}"
            );
        }
    }
}

#[tokio::test]
#[parallel]
async fn given_fresh_server_when_running_end_to_end_group_benchmark_should_finish() {
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::default())
        .build()
        .unwrap();
    harness.start().await.unwrap();

    run_bench_and_wait_for_finish(
        &harness.server().raw_tcp_addr().unwrap(),
        &TransportProtocol::Tcp,
        "end-to-end-producing-consumer-group",
        IggyByteSize::new(BENCHMARK_DATA_BYTES),
    );
}

#[tokio::test]
#[parallel]
async fn given_secret_environment_when_saving_benchmark_should_omit_credentials() {
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::default())
        .build()
        .unwrap();
    harness.start().await.unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let server_address = harness.server().raw_tcp_addr().unwrap();
    let output = assert_cmd::Command::new(cargo_bin_path("iggy-bench").unwrap())
        .args([
            "--total-data",
            REPORT_TEST_DATA,
            "pinned-producer",
            "--producers",
            "1",
            "--streams",
            "1",
            "tcp",
            "--server-address",
            &server_address,
            "output",
            "--output-dir",
        ])
        .arg(output_dir.path())
        .envs([
            ("IGGY_ROOT_PASSWORD", TEST_SECRET),
            ("IGGY_CLUSTER_AUTH_SHARED_SECRET", TEST_SECRET),
            ("IGGY_HTTP_JWT_ENCODING_SECRET", TEST_SECRET),
            ("IGGY_ENCRYPTION_KEY", TEST_SECRET),
            ("IGGY_UNRELATED_CREDENTIAL", TEST_SECRET),
            ("IGGY_CONFIG_PATH", "benchmark-config.toml"),
            ("IGGY_ENV_PATH", "benchmark.env"),
            ("IGGY_SHARDING_CPU_ALLOCATION", "4"),
            ("IGGY_SHARD_RUNTIME_CAPACITY", "8192"),
            ("IGGY_SHARD_EVENT_INTERVAL", "256"),
        ])
        .timeout(BENCH_WAIT_TIMEOUT)
        .unwrap();
    let run_dirs = bench_run_dirs(output_dir.path());
    let [run_dir] = run_dirs.as_slice() else {
        panic!("expected one run directory, found {run_dirs:?}");
    };
    let report = fs::read_to_string(run_dir.join(REPORT_FILE)).unwrap();
    assert!(report.contains("IGGY_CONFIG_PATH=benchmark-config.toml"));
    assert!(report.contains("IGGY_ENV_PATH=benchmark.env"));
    assert!(report.contains("IGGY_SHARDING_CPU_ALLOCATION=4"));
    assert!(report.contains("IGGY_SHARD_RUNTIME_CAPACITY=8192"));
    assert!(report.contains("IGGY_SHARD_EVENT_INTERVAL=256"));
    for (source, content) in [
        ("report", report.as_str()),
        ("stdout", &String::from_utf8_lossy(&output.stdout)),
        ("stderr", &String::from_utf8_lossy(&output.stderr)),
    ] {
        assert!(
            !content.contains(TEST_SECRET),
            "{source} exposes a credential"
        );
    }
}

#[tokio::test]
#[parallel]
async fn given_same_command_when_run_twice_should_keep_both_run_directories() {
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::default())
        .build()
        .unwrap();
    harness.start().await.unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let server_address = harness.server().raw_tcp_addr().unwrap();

    for _ in 0..2 {
        assert_cmd::Command::new(cargo_bin_path("iggy-bench").unwrap())
            .args([
                "--password",
                DEFAULT_ROOT_PASSWORD,
                "--total-data",
                REPORT_TEST_DATA,
                "pinned-producer",
                "--producers",
                "1",
                "--streams",
                "1",
                "tcp",
                "--server-address",
                &server_address,
                "output",
                "--suite",
                TEST_SUITE,
                "--workload",
                TEST_WORKLOAD,
                "--output-dir",
            ])
            .arg(output_dir.path())
            .timeout(BENCH_WAIT_TIMEOUT)
            .assert()
            .success();
    }

    let total_data_bytes = REPORT_TEST_DATA
        .parse::<IggyByteSize>()
        .unwrap()
        .as_bytes_u64();
    let run_dirs = bench_run_dirs(output_dir.path());
    assert_eq!(run_dirs.len(), 2, "{run_dirs:?}");
    for run_dir in &run_dirs {
        assert!(
            run_dir.join(BENCH_LOG_FILE).is_file(),
            "{} has no {BENCH_LOG_FILE}",
            run_dir.display()
        );
        let report: Value =
            serde_json::from_str(&fs::read_to_string(run_dir.join(REPORT_FILE)).unwrap()).unwrap();
        let params = &report["params"];
        assert_eq!(params["suite"], TEST_SUITE);
        assert_eq!(params["workload"], TEST_WORKLOAD);
        assert_eq!(
            params["data_volume"]["total_data_bytes"].as_u64(),
            Some(total_data_bytes)
        );
        assert!(params["options"].is_object(), "{params}");
        let command_line = params["command_line"].as_str().unwrap();
        assert!(
            command_line.contains(REDACTED_PASSWORD_ARGUMENT),
            "{command_line}"
        );
    }
}
