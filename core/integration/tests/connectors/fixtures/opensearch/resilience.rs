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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_BATCH_SIZE,
    ENV_SOURCE_CIRCUIT_BREAKER_COOL_DOWN, ENV_SOURCE_CIRCUIT_BREAKER_THRESHOLD, ENV_SOURCE_INDEX,
    ENV_SOURCE_MAX_OPEN_RETRIES, ENV_SOURCE_MAX_RETRIES, ENV_SOURCE_OPEN_RETRY_MAX_DELAY,
    ENV_SOURCE_PATH, ENV_SOURCE_POLLING_INTERVAL, ENV_SOURCE_RETRY_DELAY,
    ENV_SOURCE_RETRY_MAX_DELAY, ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM,
    ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TIMESTAMP_FIELD, ENV_SOURCE_URL,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub const RESILIENCE_INDEX: &str = "resilience_test";

fn index_path_pattern() -> &'static str {
    r"/resilience_test$"
}

fn search_path_pattern() -> &'static str {
    r"/resilience_test/_search"
}

fn search_success_body() -> serde_json::Value {
    serde_json::json!({
        "hits": {
            "hits": [{
                "_id": "doc-retry-1",
                "_source": {
                    "id": 42,
                    "name": "retry_doc",
                    "value": 100,
                    "timestamp": "2024-01-15T12:00:00.000Z"
                },
                "sort": ["2024-01-15T12:00:00.000Z", "doc-retry-1"]
            }]
        }
    })
}

fn resilience_base_envs(mock_uri: &str) -> HashMap<String, String> {
    let mut envs = HashMap::new();
    envs.insert(ENV_SOURCE_URL.to_string(), mock_uri.to_string());
    envs.insert(ENV_SOURCE_INDEX.to_string(), RESILIENCE_INDEX.to_string());
    envs.insert(ENV_SOURCE_POLLING_INTERVAL.to_string(), "100ms".to_string());
    envs.insert(ENV_SOURCE_BATCH_SIZE.to_string(), "10".to_string());
    envs.insert(
        ENV_SOURCE_TIMESTAMP_FIELD.to_string(),
        "timestamp".to_string(),
    );
    envs.insert(
        ENV_SOURCE_STREAMS_0_STREAM.to_string(),
        DEFAULT_TEST_STREAM.to_string(),
    );
    envs.insert(
        ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
        DEFAULT_TEST_TOPIC.to_string(),
    );
    envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
    envs.insert(
        ENV_SOURCE_PATH.to_string(),
        "../../target/debug/libiggy_connector_opensearch_source".to_string(),
    );
    envs.insert(ENV_SOURCE_RETRY_DELAY.to_string(), "50ms".to_string());
    envs.insert(ENV_SOURCE_RETRY_MAX_DELAY.to_string(), "200ms".to_string());
    envs.insert(
        ENV_SOURCE_OPEN_RETRY_MAX_DELAY.to_string(),
        "200ms".to_string(),
    );
    envs.insert(ENV_SOURCE_MAX_OPEN_RETRIES.to_string(), "2".to_string());
    envs
}

async fn mount_index_exists(mock_server: &MockServer) {
    Mock::given(method("HEAD"))
        .and(path_regex(index_path_pattern()))
        .respond_with(ResponseTemplate::new(200))
        .mount(mock_server)
        .await;
}

async fn mount_transient_search_mocks(mock_server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(search_path_pattern()))
        .respond_with(ResponseTemplate::new(503).set_body_string("temporary"))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(search_path_pattern()))
        .respond_with(ResponseTemplate::new(200).set_body_json(search_success_body()))
        .with_priority(2)
        .mount(mock_server)
        .await;
}

async fn mount_persistent_search_failure(mock_server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(search_path_pattern()))
        .respond_with(ResponseTemplate::new(500).set_body_string("persistent"))
        .mount(mock_server)
        .await;
}

/// Wiremock-backed fixture: two transient `503` search responses, then one hit.
pub struct OpenSearchSourceTransientErrorFixture {
    mock_server: MockServer,
}

impl OpenSearchSourceTransientErrorFixture {
    pub async fn search_request_count(&self) -> usize {
        self.mock_server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| {
                request.url.path().contains(search_path_pattern())
                    && request.method.as_str() == "POST"
            })
            .count()
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourceTransientErrorFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let mock_server = MockServer::start().await;
        mount_index_exists(&mock_server).await;
        mount_transient_search_mocks(&mock_server).await;

        Ok(Self { mock_server })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = resilience_base_envs(&self.mock_server.uri());
        envs.insert(ENV_SOURCE_MAX_RETRIES.to_string(), "5".to_string());
        envs
    }
}

/// Wiremock-backed fixture: every search returns `500` with a low circuit-breaker threshold.
pub struct OpenSearchSourceCircuitBreakerFixture {
    mock_server: MockServer,
}

impl OpenSearchSourceCircuitBreakerFixture {
    pub async fn search_request_count(&self) -> usize {
        self.mock_server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| {
                request.url.path().contains(search_path_pattern())
                    && request.method.as_str() == "POST"
            })
            .count()
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourceCircuitBreakerFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let mock_server = MockServer::start().await;
        mount_index_exists(&mock_server).await;
        mount_persistent_search_failure(&mock_server).await;

        Ok(Self { mock_server })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = resilience_base_envs(&self.mock_server.uri());
        envs.insert(
            ENV_SOURCE_CIRCUIT_BREAKER_THRESHOLD.to_string(),
            "3".to_string(),
        );
        envs.insert(
            ENV_SOURCE_CIRCUIT_BREAKER_COOL_DOWN.to_string(),
            "500ms".to_string(),
        );
        envs
    }
}
