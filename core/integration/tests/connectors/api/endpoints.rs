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

use iggy_connector_sdk::api::{
    ConnectorRuntimeStats, HealthResponse, SinkInfoResponse, SourceInfoResponse,
};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use serde_json::{Value, json};
use std::fs;

const API_KEY: &str = "test-api-key";
/// `config_dir` of `key_validation.toml`, relative to the crate root, which is
/// the working directory of both the test process and the spawned runtime. It
/// lives under the gitignored `test_logs/` so a regression that writes a config
/// file cannot land in the source tree or be loaded by another test.
const CONNECTORS_CONFIG_DIR: &str = "../../test_logs/connectors_api_key_validation";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn root_endpoint_returns_welcome_message(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, "Connector Runtime API");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn health_endpoint_returns_healthy(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/health", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let health: HealthResponse = response.json().await.unwrap();
    assert_eq!(health.status, "healthy");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn stats_endpoint_returns_runtime_stats(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let stats: ConnectorRuntimeStats = response.json().await.unwrap();

    assert!(stats.process_id > 0);
    assert!(stats.connectors.is_empty());
    assert_eq!(stats.sources_total, 0);
    assert_eq!(stats.sources_running, 0);
    assert_eq!(stats.sinks_total, 0);
    assert_eq!(stats.sinks_running, 0);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn metrics_endpoint_returns_prometheus_format(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/metrics", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();

    assert!(body.contains("iggy_connectors_sources_total"));
    assert!(body.contains("iggy_connectors_sources_running"));
    assert!(body.contains("iggy_connectors_sinks_total"));
    assert!(body.contains("iggy_connectors_sinks_running"));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn sources_endpoint_returns_list(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/sources", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let sources: Vec<SourceInfoResponse> = response.json().await.unwrap();
    assert!(sources.is_empty());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn sinks_endpoint_returns_list(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.unwrap();
    assert!(sinks.is_empty());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn api_key_authentication_required(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/metrics", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/sources", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/sinks", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn api_key_authentication_rejected_with_invalid_key(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .header("api-key", "invalid-key")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/metrics", api_address))
        .header("api-key", "wrong-api-key")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/key_validation.toml")),
    seed = seeds::connector_stream
)]
async fn key_endpoints_reject_key_that_is_not_a_single_path_component(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();
    let config = json!({
        "enabled": false,
        "name": "x",
        "path": "/tmp/evil.so",
        "streams": []
    });
    let config_dir_before = config_dir_entries();

    // Each entry is a percent-encoded path segment: axum decodes it before
    // handing it to the handler, so `..%2F..%2Fpwned` arrives as `../../pwned`.
    // The charset itself is covered by unit tests; these are the two probes
    // from the issue report plus a hidden-file name.
    let keys = ["..%2F..%2Fpwned", "x%2F..%2F..%2F..%2Ftmp%2Fpwn", ".hidden"];
    for key in keys {
        for kind in ["sources", "sinks"] {
            let response = client
                .post(format!("{api_address}/{kind}/{key}/configs"))
                .header("api-key", API_KEY)
                .json(&config)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "POST /{kind}/{key}/configs");
            let body: Value = response.json().await.unwrap();
            assert_eq!(
                body["code"], "invalid_connector_key",
                "POST /{kind}/{key}/configs body: {body}"
            );

            let response = client
                .get(format!("{api_address}/{kind}/{key}"))
                .header("api-key", API_KEY)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "GET /{kind}/{key}");
            let body: Value = response.json().await.unwrap();
            assert_eq!(
                body["code"], "invalid_connector_key",
                "GET /{kind}/{key} body: {body}"
            );
        }
    }

    assert_eq!(
        config_dir_entries(),
        config_dir_before,
        "no config file should have been written"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/api/config.toml")),
    seed = seeds::connector_stream
)]
async fn key_endpoints_accept_single_component_key(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = Client::new();

    for (kind, code) in [("sources", "source_not_found"), ("sinks", "sink_not_found")] {
        let response = client
            .get(format!("{api_address}/{kind}/postgres-cdc.v2_1"))
            .header("api-key", API_KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "GET /{kind}/postgres-cdc.v2_1");
        let body: Value = response.json().await.unwrap();
        assert_eq!(
            body["code"], code,
            "GET /{kind}/postgres-cdc.v2_1 body: {body}"
        );
    }
}

fn config_dir_entries() -> Vec<String> {
    let mut entries: Vec<String> = fs::read_dir(CONNECTORS_CONFIG_DIR)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
}
