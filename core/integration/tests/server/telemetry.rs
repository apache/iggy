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

use iggy::prelude::*;
use integration::harness::TestHarness;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

#[tokio::test]
#[serial_test::parallel]
async fn given_http_telemetry_enabled_when_starting_should_serve_requests() {
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&collector)
        .await;
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(1)
        .build()
        .expect("build telemetry harness");
    for (key, value) in [
        ("IGGY_TELEMETRY_ENABLED", "true".to_string()),
        ("IGGY_TELEMETRY_LOGS_TRANSPORT", "http".to_string()),
        ("IGGY_TELEMETRY_TRACES_TRANSPORT", "http".to_string()),
        (
            "IGGY_TELEMETRY_LOGS_ENDPOINT",
            format!("{}/v1/logs", collector.uri()),
        ),
        (
            "IGGY_TELEMETRY_TRACES_ENDPOINT",
            format!("{}/v1/traces", collector.uri()),
        ),
    ] {
        harness.server_mut().add_env(key, value);
    }
    harness
        .start()
        .await
        .expect("HTTP telemetry must not prevent server startup");
    let client = harness
        .server()
        .tcp_client()
        .expect("TCP client")
        .with_root_login()
        .connect()
        .await
        .expect("connect with HTTP telemetry enabled");
    client.ping().await.expect("server must answer ping");
}
