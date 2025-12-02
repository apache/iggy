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
use crate::connectors::http_config_provider::WireMockMode;
use reqwest::StatusCode;

#[tokio::test]
async fn test_source_configs_list_returns_all_versions() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs"))
        .send()
        .await
        .expect("GET /sources/random/configs request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    // Note: The connector runtime API always returns direct format,
    // even when it fetches configs from a wrapped-format provider
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_array(), "Should return JSON array");
    let configs = body.as_array().unwrap();
    assert_eq!(configs.len(), 2, "Should have 2 config versions");

    // Validate version 0 schema
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

    // Validate streams array (sources have StreamProducerConfig)
    let streams = v0["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "Should have 1 stream config");

    let stream = &streams[0];
    assert_eq!(stream["stream"].as_str().unwrap(), "test_stream");
    assert_eq!(stream["topic"].as_str().unwrap(), "test_topic"); // String, not array!
    assert_eq!(stream["schema"].as_str().unwrap(), "json");
    assert!(stream["batch_length"].as_u64().is_some());
    assert!(stream["linger_time"].as_str().is_some()); // Source-specific field

    // Validate plugin configuration
    assert_eq!(v0["plugin_config_format"].as_str().unwrap(), "json");
    assert!(v0["plugin_config"].is_object());

    // Validate version 1 exists and is different
    let v1 = &configs[1];
    assert_eq!(v1["version"].as_u64().unwrap(), 1);
    assert_eq!(v1["key"].as_str().unwrap(), "random");
}

#[tokio::test]
async fn test_source_config_by_version_returns_specific_version() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs/1"))
        .send()
        .await
        .expect("GET /sources/random/configs/1 request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    // Validate it's version 1
    assert_eq!(body["version"].as_u64().unwrap(), 1);
    assert_eq!(body["key"].as_str().unwrap(), "random");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Random source");

    // Validate streams structure
    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["topic"].as_str().unwrap(), "test_topic"); // String for sources
}

#[tokio::test]
async fn test_source_active_config_returns_current_version() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sources/random/configs/active"))
        .send()
        .await
        .expect("GET /sources/random/configs/active request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    // Validate it's the active version (version 1)
    assert_eq!(body["version"].as_u64().unwrap(), 1);
    assert_eq!(body["key"].as_str().unwrap(), "random");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Random source");

    // Validate complete structure
    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(body["plugin_config_format"].as_str().unwrap(), "json");
    assert!(body["plugin_config"].is_object());
}

#[tokio::test]
async fn test_sink_configs_list_returns_all_versions() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
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

    // Validate version 0 schema
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

    // Validate streams array (sinks have StreamConsumerConfig)
    let streams = v0["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "Should have 1 stream config");

    let stream = &streams[0];
    assert_eq!(stream["stream"].as_str().unwrap(), "test_stream");
    // CRITICAL: Sinks have topics as ARRAY, not string!
    let topics = stream["topics"].as_array().unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].as_str().unwrap(), "test_topic");
    assert_eq!(stream["schema"].as_str().unwrap(), "json");
    assert!(stream["batch_length"].as_u64().is_some());
    assert!(stream["poll_interval"].as_str().is_some()); // Sink-specific
    assert!(stream["consumer_group"].as_str().is_some()); // Sink-specific

    // Validate plugin configuration (can be null)
    assert!(v0["plugin_config_format"].is_null());
    assert!(v0["plugin_config"].is_object());
}

#[tokio::test]
async fn test_sink_config_by_version_returns_specific_version() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sinks/stdout/configs/0"))
        .send()
        .await
        .expect("GET /sinks/stdout/configs/0 request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    // Validate it's version 0
    assert_eq!(body["version"].as_u64().unwrap(), 0);
    assert_eq!(body["key"].as_str().unwrap(), "stdout");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Stdout sink");

    // Validate streams structure (topics as array for sinks)
    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    let topics = streams[0]["topics"].as_array().unwrap();
    assert_eq!(topics[0].as_str().unwrap(), "test_topic");
}

#[tokio::test]
async fn test_sink_active_config_returns_current_version() {
    let wiremock_runtime =
        crate::connectors::http_config_provider::setup(WireMockMode::Wrapped).await;
    let http_api_address = wiremock_runtime
        .connectors_runtime
        .connectors_api_address()
        .expect("connector runtime should be available");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{http_api_address}/sinks/stdout/configs/active"))
        .send()
        .await
        .expect("GET /sinks/stdout/configs/active request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.is_object(), "Should return JSON object");

    // Validate it's the active version (version 0, only one available)
    assert_eq!(body["version"].as_u64().unwrap(), 0);
    assert_eq!(body["key"].as_str().unwrap(), "stdout");
    assert!(body["enabled"].as_bool().unwrap());
    assert_eq!(body["name"].as_str().unwrap(), "Stdout sink");

    // Validate complete structure
    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert!(body["plugin_config"].is_object());
}
