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

//! The readiness of the gateway binary while its Iggy server stops and comes back.
//!
//! Needs a built `iggy-server`, see `docs/TEST_SUITE.md`.

#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/http.rs"]
mod http;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use std::time::Duration;

use gateway_process::TestGateway;
use http::{get, sample, wait_for};
use iggy_server::TestServer;

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
