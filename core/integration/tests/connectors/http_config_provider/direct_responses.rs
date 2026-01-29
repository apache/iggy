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

use crate::connectors::fixtures::WireMockDirectFixture;
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::StatusCode;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn source_configs_list_returns_all_versions(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs"))
        .send()
        .await
        .expect("GET /sources/random/configs request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_array(), "Should return JSON array");
    let configs = body.as_array().unwrap();
    assert_eq!(configs.len(), 2, "Should have 2 config versions");

    let v0 = &configs[0];
    assert_eq!(v0["key"].as_str().unwrap(), "random");
    assert!(v0["enabled"].as_bool().unwrap());
    assert_eq!(v0["version"].as_u64().unwrap(), 0);
    assert_eq!(v0["name"].as_str().unwrap(), "Random source");
    assert!(
        v0["path"]
            .as_str()
            .unwrap()
            .contains("libiggy_connector_random_source")
    );

    let streams = v0["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "Should have 1 stream config");

    let stream = &streams[0];
    assert_eq!(stream["stream"].as_str().unwrap(), "test_stream");
    assert_eq!(stream["topic"].as_str().unwrap(), "test_topic");
    assert_eq!(stream["schema"].as_str().unwrap(), "json");
    assert!(stream["batch_length"].as_u64().is_some());
    assert!(stream["linger_time"].as_str().is_some());

    assert_eq!(v0["plugin_config_format"].as_str().unwrap(), "json");
    assert!(v0["plugin_config"].is_object());

    let v1 = &configs[1];
    assert_eq!(v1["version"].as_u64().unwrap(), 1);
    assert_eq!(v1["key"].as_str().unwrap(), "random");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn source_config_by_version_returns_specific_version(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs/1"))
        .send()
        .await
        .expect("GET /sources/random/configs/1 request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    assert_eq!(body["version"].as_u64().unwrap(), 1);
    assert_eq!(body["key"].as_str().unwrap(), "random");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Random source");

    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["topic"].as_str().unwrap(), "test_topic");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn source_active_config_returns_current_version(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs/active"))
        .send()
        .await
        .expect("GET /sources/random/configs/active request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    assert_eq!(body["version"].as_u64().unwrap(), 1);
    assert_eq!(body["key"].as_str().unwrap(), "random");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Random source");

    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(body["plugin_config_format"].as_str().unwrap(), "json");
    assert!(body["plugin_config"].is_object());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn sink_configs_list_returns_all_versions(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sinks/stdout/configs"))
        .send()
        .await
        .expect("GET /sinks/stdout/configs request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_array(), "Should return JSON array");
    let configs = body.as_array().unwrap();
    assert_eq!(configs.len(), 1, "Should have 1 config version");

    let v0 = &configs[0];
    assert_eq!(v0["key"].as_str().unwrap(), "stdout");
    assert!(v0["enabled"].as_bool().unwrap());
    assert_eq!(v0["version"].as_u64().unwrap(), 0);
    assert_eq!(v0["name"].as_str().unwrap(), "Stdout sink");
    assert!(
        v0["path"]
            .as_str()
            .unwrap()
            .contains("libiggy_connector_stdout_sink")
    );

    let streams = v0["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "Should have 1 stream config");

    let stream = &streams[0];
    assert_eq!(stream["stream"].as_str().unwrap(), "test_stream");
    let topics = stream["topics"].as_array().unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].as_str().unwrap(), "test_topic");
    assert_eq!(stream["schema"].as_str().unwrap(), "json");
    assert!(stream["batch_length"].as_u64().is_some());
    assert!(stream["poll_interval"].as_str().is_some());
    assert!(stream["consumer_group"].as_str().is_some());

    assert!(v0["plugin_config_format"].is_null());
    assert!(v0["plugin_config"].is_object());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn sink_config_by_version_returns_specific_version(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sinks/stdout/configs/0"))
        .send()
        .await
        .expect("GET /sinks/stdout/configs/0 request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    assert_eq!(body["version"].as_u64().unwrap(), 0);
    assert_eq!(body["key"].as_str().unwrap(), "stdout");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Stdout sink");

    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    let topics = streams[0]["topics"].as_array().unwrap();
    assert_eq!(topics[0].as_str().unwrap(), "test_topic");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http_config_provider/config_direct.toml")),
    seed = seeds::connector_stream
)]
async fn sink_active_config_returns_current_version(
    harness: &TestHarness,
    _wiremock: WireMockDirectFixture,
) {
    let http_api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sinks/stdout/configs/active"))
        .send()
        .await
        .expect("GET /sinks/stdout/configs/active request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    assert_eq!(body["version"].as_u64().unwrap(), 0);
    assert_eq!(body["key"].as_str().unwrap(), "stdout");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Stdout sink");

    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert!(body["plugin_config"].is_object());
}
