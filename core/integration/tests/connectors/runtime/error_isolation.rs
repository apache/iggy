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

//! Per-connector error isolation tests for the connectors runtime.
//!
//! Each test drives the runtime with two connectors of the same kind: one
//! deliberately misconfigured and one healthy. The assertions verify that:
//!   * the runtime stays alive (`/health` returns "healthy"),
//!   * the broken connector is surfaced via the API with
//!     [`ConnectorStatus::Error`] and a populated `last_error`,
//!   * the sibling healthy connector still reaches [`ConnectorStatus::Running`].
//!
//! Together the tests cover every per-connector failure branch in
//! `source::init` and `sink::init`:
//!   * plugin path resolution failure (missing `.so`),
//!   * source state-load failure (unreachable state file),
//!   * post-container setup failure (invalid duration in stream config).

use iggy_connector_sdk::api::{
    ConnectorStatus, HealthResponse, SinkInfoResponse, SourceInfoResponse,
};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

async fn assert_runtime_healthy(http_client: &Client, api_address: &str) {
    let response = http_client
        .get(format!("{api_address}/health"))
        .send()
        .await
        .expect("Failed to query health endpoint");
    assert_eq!(response.status(), 200);
    let health: HealthResponse = response
        .json()
        .await
        .expect("Failed to parse health response");
    assert_eq!(health.status, "healthy");
}

async fn fetch_sinks(http_client: &Client, api_address: &str) -> Vec<SinkInfoResponse> {
    let response = http_client
        .get(format!("{api_address}/sinks"))
        .send()
        .await
        .expect("Failed to query /sinks");
    assert_eq!(response.status(), 200);
    response.json().await.expect("Failed to parse sinks")
}

async fn fetch_sources(http_client: &Client, api_address: &str) -> Vec<SourceInfoResponse> {
    let response = http_client
        .get(format!("{api_address}/sources"))
        .send()
        .await
        .expect("Failed to query /sources");
    assert_eq!(response.status(), 200);
    response.json().await.expect("Failed to parse sources")
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/sink_invalid_config.toml"
    )),
    seed = seeds::connector_stream
)]
async fn sink_with_invalid_config_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    assert_runtime_healthy(&http_client, &api_address).await;
    let sinks = fetch_sinks(&http_client, &api_address).await;

    assert_eq!(
        sinks.len(),
        2,
        "Both the invalid and the valid sink should be visible in the API"
    );

    let invalid_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_invalid")
        .expect("Invalid sink should be reported");
    assert_eq!(invalid_sink.status, ConnectorStatus::Error);
    let last_error = invalid_sink
        .last_error
        .as_ref()
        .expect("Invalid sink should expose a last_error");
    assert!(
        last_error.message.contains("poll interval"),
        "last_error should mention the misconfigured poll_interval, got: {}",
        last_error.message
    );

    let valid_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_valid")
        .expect("Healthy sibling sink should be reported");
    assert_eq!(valid_sink.status, ConnectorStatus::Running);
    assert!(
        valid_sink.last_error.is_none(),
        "Healthy sibling sink should have no last_error"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/sink_missing_plugin.toml"
    )),
    seed = seeds::connector_stream
)]
async fn sink_with_missing_plugin_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    assert_runtime_healthy(&http_client, &api_address).await;
    let sinks = fetch_sinks(&http_client, &api_address).await;

    assert_eq!(
        sinks.len(),
        2,
        "Both the missing-plugin sink and the valid sink should be visible in the API"
    );

    let missing_plugin_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_missing_plugin")
        .expect("Missing-plugin sink should be reported");
    assert_eq!(missing_plugin_sink.status, ConnectorStatus::Error);
    let last_error = missing_plugin_sink
        .last_error
        .as_ref()
        .expect("Missing-plugin sink should expose a last_error");
    assert!(
        last_error.message.contains("Plugin library not found")
            || last_error.message.contains("Failed to resolve plugin path"),
        "last_error should mention the missing plugin path, got: {}",
        last_error.message
    );

    let valid_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_valid")
        .expect("Healthy sibling sink should be reported");
    assert_eq!(valid_sink.status, ConnectorStatus::Running);
    assert!(
        valid_sink.last_error.is_none(),
        "Healthy sibling sink should have no last_error"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/source_invalid_state.toml"
    )),
    seed = seeds::connector_stream
)]
async fn source_with_invalid_state_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    sleep(Duration::from_millis(500)).await;

    assert_runtime_healthy(&http_client, &api_address).await;
    let sources = fetch_sources(&http_client, &api_address).await;

    assert_eq!(
        sources.len(),
        2,
        "Both the broken-state source and the valid source should be visible in the API"
    );

    let invalid_source = sources
        .iter()
        .find(|source| source.key == "random_invalid/state_missing_parent")
        .expect("Source with broken state path should be reported");
    assert_eq!(invalid_source.status, ConnectorStatus::Error);
    let last_error = invalid_source
        .last_error
        .as_ref()
        .expect("Source with broken state should expose a last_error");
    assert!(
        last_error.message.contains("Failed to load source state"),
        "last_error should mention the failed state load, got: {}",
        last_error.message
    );

    let valid_source = sources
        .iter()
        .find(|source| source.key == "random_valid")
        .expect("Healthy sibling source should be reported");
    assert_eq!(valid_source.status, ConnectorStatus::Running);
    assert!(
        valid_source.last_error.is_none(),
        "Healthy sibling source should have no last_error"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/source_missing_plugin.toml"
    )),
    seed = seeds::connector_stream
)]
async fn source_with_missing_plugin_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    sleep(Duration::from_millis(500)).await;

    assert_runtime_healthy(&http_client, &api_address).await;
    let sources = fetch_sources(&http_client, &api_address).await;

    assert_eq!(
        sources.len(),
        2,
        "Both the missing-plugin source and the valid source should be visible in the API"
    );

    let missing_plugin_source = sources
        .iter()
        .find(|source| source.key == "random_missing_plugin")
        .expect("Missing-plugin source should be reported");
    assert_eq!(missing_plugin_source.status, ConnectorStatus::Error);
    let last_error = missing_plugin_source
        .last_error
        .as_ref()
        .expect("Missing-plugin source should expose a last_error");
    assert!(
        last_error.message.contains("Plugin library not found")
            || last_error.message.contains("Failed to resolve plugin path"),
        "last_error should mention the missing plugin path, got: {}",
        last_error.message
    );

    let valid_source = sources
        .iter()
        .find(|source| source.key == "random_valid")
        .expect("Healthy sibling source should be reported");
    assert_eq!(valid_source.status, ConnectorStatus::Running);
    assert!(
        valid_source.last_error.is_none(),
        "Healthy sibling source should have no last_error"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/source_invalid_config.toml"
    )),
    seed = seeds::connector_stream
)]
async fn source_with_invalid_config_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    sleep(Duration::from_millis(500)).await;

    assert_runtime_healthy(&http_client, &api_address).await;
    let sources = fetch_sources(&http_client, &api_address).await;

    assert_eq!(
        sources.len(),
        2,
        "Both the invalid and the valid source should be visible in the API"
    );

    let invalid_source = sources
        .iter()
        .find(|source| source.key == "random_invalid")
        .expect("Invalid source should be reported");
    assert_eq!(invalid_source.status, ConnectorStatus::Error);
    let last_error = invalid_source
        .last_error
        .as_ref()
        .expect("Invalid source should expose a last_error");
    assert!(
        last_error.message.contains("linger time"),
        "last_error should mention the misconfigured linger_time, got: {}",
        last_error.message
    );

    let valid_source = sources
        .iter()
        .find(|source| source.key == "random_valid")
        .expect("Healthy sibling source should be reported");
    assert_eq!(valid_source.status, ConnectorStatus::Running);
    assert!(
        valid_source.last_error.is_none(),
        "Healthy sibling source should have no last_error"
    );
}
