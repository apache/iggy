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

use integration::harness::TestBinaryError;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::WaitFor::Healthcheck;
use testcontainers_modules::testcontainers::core::wait::HealthWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const WIREMOCK_IMAGE: &str = "wiremock/wiremock";
const WIREMOCK_TAG: &str = "3.13.2";
const WIREMOCK_PORT: u16 = 8080;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_TEST_TOPIC_2: &str = "test_topic_2";

pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 100;

// HTTP sink env vars follow the convention: IGGY_CONNECTORS_SINK_HTTP_<SECTION>_<FIELD>
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_HTTP_PATH";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_HTTP_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_HTTP_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_HTTP_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_HTTP_STREAMS_0_CONSUMER_GROUP";

// plugin_config fields
pub(super) const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_URL";
pub(super) const ENV_SINK_BATCH_MODE: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_BATCH_MODE";
pub(super) const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SINK_METHOD: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_METHOD";
pub(super) const ENV_SINK_TIMEOUT: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_TIMEOUT";
pub(super) const ENV_SINK_MAX_RETRIES: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_MAX_RETRIES";
pub(super) const ENV_SINK_RETRY_DELAY: &str = "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_RETRY_DELAY";
pub(super) const ENV_SINK_VERBOSE_LOGGING: &str =
    "IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_VERBOSE_LOGGING";

/// WireMock container for HTTP sink integration tests.
///
/// Provides a real HTTP endpoint that accepts requests and exposes an admin API
/// for verifying received requests at `/__admin/requests`.
pub struct HttpSinkWireMockContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    /// Base URL of the WireMock container (e.g., `http://localhost:32768`).
    pub(super) base_url: String,
}

impl HttpSinkWireMockContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let current_dir = std::env::current_dir().map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "HttpSinkWireMockContainer".to_string(),
            message: format!("Failed to get current dir: {e}"),
        })?;

        let container = GenericImage::new(WIREMOCK_IMAGE, WIREMOCK_TAG)
            .with_exposed_port(WIREMOCK_PORT.tcp())
            .with_wait_for(Healthcheck(HealthWaitStrategy::default()))
            .with_mount(Mount::bind_mount(
                current_dir
                    .join("tests/connectors/http/wiremock/mappings")
                    .to_string_lossy()
                    .to_string(),
                "/home/wiremock/mappings",
            ))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "HttpSinkWireMockContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host = container
            .get_host()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "HttpSinkWireMockContainer".to_string(),
                message: format!("Failed to get host: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(WIREMOCK_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "HttpSinkWireMockContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let base_url = format!("http://{host}:{host_port}");
        info!("HTTP sink WireMock container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }

    /// Query WireMock's admin API and return all received requests.
    pub async fn get_received_requests(&self) -> Result<Vec<WireMockRequest>, TestBinaryError> {
        let url = format!("{}/__admin/requests", self.base_url);
        let response = reqwest::get(&url)
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to query WireMock admin API: {e}"),
            })?;

        let body: serde_json::Value =
            response
                .json()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse WireMock admin response: {e}"),
                })?;

        let empty = vec![];
        let requests = body["requests"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .map(|r| WireMockRequest {
                method: r["request"]["method"].as_str().unwrap_or("").to_string(),
                url: r["request"]["url"].as_str().unwrap_or("").to_string(),
                body: r["request"]["body"].as_str().unwrap_or("").to_string(),
                headers: r["request"]["headers"].clone(),
            })
            .collect();

        Ok(requests)
    }

    /// Poll WireMock until the expected number of requests have been received.
    pub async fn wait_for_requests(
        &self,
        expected: usize,
    ) -> Result<Vec<WireMockRequest>, TestBinaryError> {
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            let requests = self.get_received_requests().await?;
            if requests.len() >= expected {
                info!(
                    "WireMock received {} requests (expected {})",
                    requests.len(),
                    expected
                );
                return Ok(requests);
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }

        let actual = self.get_received_requests().await?.len();
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected} requests in WireMock after {} attempts, got {actual}",
                DEFAULT_POLL_ATTEMPTS
            ),
        })
    }

    /// Reset WireMock's request journal (clear received requests).
    #[allow(dead_code)]
    pub async fn reset_requests(&self) -> Result<(), TestBinaryError> {
        let url = format!("{}/__admin/requests", self.base_url);
        let client = reqwest::Client::new();
        client
            .delete(&url)
            .send()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to reset WireMock requests: {e}"),
            })?;
        Ok(())
    }
}

/// A request captured by WireMock's admin API.
#[derive(Debug, Clone)]
pub struct WireMockRequest {
    pub method: String,
    pub url: String,
    pub body: String,
    pub headers: serde_json::Value,
}

impl WireMockRequest {
    /// Parse the body as JSON.
    pub fn body_as_json(&self) -> Result<serde_json::Value, TestBinaryError> {
        serde_json::from_str(&self.body).map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to parse request body as JSON: {e}"),
        })
    }

    /// Get a header value by name (case-insensitive per RFC 7230).
    /// WireMock may return header keys in any case, so we iterate and compare.
    pub fn header(&self, name: &str) -> Option<String> {
        // WireMock returns headers as {"Header-Name": {"values": ["value"]}}
        // or just as a direct string value depending on version.
        let obj = self.headers.as_object()?;
        for (key, value) in obj {
            if key.eq_ignore_ascii_case(name) {
                if let Some(values) = value.get("values") {
                    return values.get(0).and_then(|v| v.as_str()).map(String::from);
                }
                return value.as_str().map(String::from);
            }
        }
        None
    }
}
