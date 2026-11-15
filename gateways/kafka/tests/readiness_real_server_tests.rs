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

//! The readiness and the metrics of the gateway binary: while it waits for Iggy, while Iggy stops
//! and comes back, and while it drains at SIGTERM.
//!
//! Needs a built `iggy-server`, see `docs/TEST_SUITE.md`.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/http.rs"]
mod http;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;

use std::time::Duration;

use gateway_process::TestGateway;
use http::{get, sample, wait_for};
use iggy_server::{PortGuard, TestServer};
use tcp::hold_answers;

/// The series of a metric that `/metrics` must carry, and its value, for a gateway at rest.
const AT_REST: [(&str, f64); 14] = [
    ("iggy_kafka_gateway_ready", 1.0),
    ("iggy_kafka_gateway_iggy_reachable", 1.0),
    (
        "iggy_kafka_gateway_connections_open{listener=\"PLAINTEXT\"}",
        0.0,
    ),
    ("iggy_kafka_gateway_slots_used{pool=\"fetch\"}", 0.0),
    ("iggy_kafka_gateway_slots{pool=\"fetch\"}", 4.0),
    ("iggy_kafka_gateway_slots_used{pool=\"produce_large\"}", 0.0),
    ("iggy_kafka_gateway_slots{pool=\"produce_large\"}", 4.0),
    ("iggy_kafka_gateway_produce_memory_used_bytes", 0.0),
    ("iggy_kafka_gateway_produce_sessions", 0.0),
    ("iggy_kafka_gateway_produce_sessions_limit", 256.0),
    ("iggy_kafka_gateway_produce_shared_producers", 0.0),
    ("iggy_kafka_gateway_produce_sends_in_doubt_total", 0.0),
    ("iggy_kafka_gateway_group_members", 0.0),
    ("iggy_kafka_gateway_coordinator_partitions_served", 16.0),
];

/// The endpoint starts before the bridge connects, so an orchestrator sees a gateway that waits
/// for Iggy as live, and does not restart it.
#[tokio::test]
async fn given_iggy_not_up_yet_should_answer_live_and_not_ready_while_it_waits() {
    let stopped = PortGuard::acquire();
    let iggy = format!("127.0.0.1:{}", stopped.port);
    let gateway = TestGateway::start_with_env(&[
        ("IGGY_KAFKA_BRIDGE_ENABLED", "true"),
        ("IGGY_KAFKA_IGGY_ADDR", &iggy),
        ("IGGY_KAFKA_IGGY_USERNAME", "iggy"),
        ("IGGY_KAFKA_IGGY_PASSWORD", "iggy"),
    ]);
    let http = gateway
        .http_address
        .clone()
        .expect("the gateway serves HTTP");

    wait_for(&http, "/live", Duration::from_secs(10), |status, _| {
        status == 200
    })
    .await;
    let (status, body) = get(&http, "/ready").await.expect("/ready answers");
    assert_eq!(status, 503);
    assert!(body.contains("starting"), "{body}");
}

/// `/ready` answers 503 through the whole drain: the gateway turns unready before it drains, and
/// the endpoint stops last.
#[tokio::test]
async fn given_sigterm_with_a_connection_open_should_answer_not_ready_until_the_drain_ends() {
    let mut gateway = TestGateway::start_with_env(&[
        ("IGGY_KAFKA_WRITE_TIMEOUT_SECS", "4"),
        ("IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS", "10"),
    ]);
    let http = gateway
        .http_address
        .clone()
        .expect("the gateway serves HTTP");
    wait_for(&http, "/ready", Duration::from_secs(15), |status, _| {
        status == 200
    })
    .await;
    let _held = hold_answers(gateway.address.parse().expect("an address")).await;
    wait_for(&http, "/metrics", Duration::from_secs(10), |_, metrics| {
        metrics.contains("iggy_kafka_gateway_requests_total{api=\"Metadata\"")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    gateway.terminate();

    // Well inside the 4 s write timeout that holds the drain open.
    let (_, body, _) = wait_for(&http, "/ready", Duration::from_secs(2), |status, _| {
        status == 503
    })
    .await;
    assert!(body.contains("shutting down"), "{body}");
    let metrics = get(&http, "/metrics").await.expect("metrics answer").1;
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_open{listener=\"PLAINTEXT\"}"
        ),
        Some(1.0),
        "the drain still waits for the held connection:\n{metrics}"
    );
    assert!(gateway.is_running());
}

/// Each series that the census adds, read from a gateway with a bridge and the coordinator log.
#[tokio::test]
async fn given_a_gateway_at_rest_when_scraped_should_report_each_census_series() {
    let data_dir = tempfile::tempdir().expect("a temp data dir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut gateway = TestGateway::start_with_http_env(
        &server.test_config(),
        &[("IGGY_KAFKA_COORDINATOR_LOG", "true")],
    );
    gateway.wait_ready().await;
    let http = gateway
        .http_address
        .clone()
        .expect("the gateway serves HTTP");
    wait_for(&http, "/metrics", Duration::from_secs(30), |_, metrics| {
        sample(metrics, "iggy_kafka_gateway_coordinator_partitions_served") == Some(16.0)
            && sample(metrics, "iggy_kafka_gateway_ready") == Some(1.0)
    })
    .await;
    // A lease that the loop stopped reading would be older than this by now.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let metrics = get(&http, "/metrics").await.expect("metrics answer").1;
    for (series, value) in AT_REST {
        assert_eq!(
            sample(&metrics, series),
            Some(value),
            "{series} in:\n{metrics}"
        );
    }
    let memory = sample(&metrics, "iggy_kafka_gateway_produce_memory_capacity_bytes");
    assert!(memory.is_some_and(|bytes| bytes > 0.0), "{metrics}");
    let lease_age = sample(&metrics, "iggy_kafka_gateway_coordinator_lease_age_seconds");
    assert!(
        lease_age.is_some_and(|age| age < 3.0),
        "the lease is read each second: {lease_age:?}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_coordinator_partition_served{partition=\"0\"}"
        ),
        Some(1.0),
        "{metrics}"
    );
}

/// How long `/ready` may take to turn 503 after Iggy stops. The probe needs two failed pings, one
/// second apart, each bounded by 2 s.
const UNREADY_WITHIN: Duration = Duration::from_secs(8);

#[tokio::test]
async fn given_iggy_stops_should_answer_not_ready_within_seconds_and_ready_once_it_is_back() {
    let data_dir = tempfile::tempdir().expect("a temp data dir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut gateway = TestGateway::start_with_http(&server.test_config());
    gateway.wait_ready().await;
    let http = gateway
        .http_address
        .clone()
        .expect("the gateway serves HTTP");

    let (_, body, _) = wait_for(&http, "/ready", Duration::from_secs(15), |status, _| {
        status == 200
    })
    .await;
    assert_eq!(body.trim(), "ready");
    let check = gateway.check_ready();
    assert!(
        check.status.success(),
        "--check-ready exits 0 while ready: {}",
        String::from_utf8_lossy(&check.stderr)
    );
    let metrics = get(&http, "/metrics").await.expect("metrics answer").1;
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_iggy_reachable"),
        Some(1.0),
        "{metrics}"
    );

    let port = server.stop();
    let (_, body, took) =
        wait_for(&http, "/ready", UNREADY_WITHIN, |status, _| status == 503).await;
    assert!(body.contains("Iggy is unreachable"), "{body}");
    println!("/ready answered 503 {took:?} after Iggy stopped");
    let check = gateway.check_ready();
    assert!(
        !check.status.success(),
        "--check-ready fails while not ready"
    );
    assert!(
        String::from_utf8_lossy(&check.stderr).contains("503"),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    assert_eq!(
        get(&http, "/live").await.map(|(status, _)| status),
        Some(200),
        "a gateway that waits for Iggy stays live: a restart would not bring Iggy back"
    );
    let metrics = get(&http, "/metrics").await.expect("metrics answer").1;
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_iggy_reachable"),
        Some(0.0),
        "{metrics}"
    );
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_ready"),
        Some(0.0),
        "{metrics}"
    );

    let _server = TestServer::spawn_on(data_dir.path(), port).await;
    wait_for(&http, "/ready", Duration::from_secs(30), |status, _| {
        status == 200
    })
    .await;
    assert!(gateway.is_running());
}
