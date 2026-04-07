/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use iggy_connector_sdk::api::{
    ConnectorRuntimeStats, HealthResponse, SinkInfoResponse, SourceInfoResponse,
};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;

const API_KEY: &str = "test-api-key";

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
