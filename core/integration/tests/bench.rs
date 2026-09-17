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

use std::{fs, process::Command};

use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::*;
use iggy_common::TransportProtocol;
use integration::bench_utils::{BENCH_WAIT_TIMEOUT, run_bench_and_wait_for_finish};
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;

const BENCHMARK_DATA_BYTES: u64 = 5_000_000;
const REPORT_TEST_DATA: &str = "1MiB";
const TEST_SECRET: &str = "sensitive-benchmark-test-value";

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
    #[allow(deprecated)]
    let command = Command::cargo_bin("iggy-bench").unwrap();
    let output = assert_cmd::Command::from_std(command)
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
    let report_dir = fs::read_dir(output_dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let report = fs::read_to_string(report_dir.join("report.json")).unwrap();
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
