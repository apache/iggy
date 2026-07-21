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

use crate::connectors::fixtures;
use integration::harness::TestBinaryError;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::WaitFor::Healthcheck;
use testcontainers_modules::testcontainers::core::wait::HealthWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const WIREMOCK_IMAGE: &str = "docker.io/wiremock/wiremock";
const WIREMOCK_TAG: &str = "3.13.2";
const WIREMOCK_PORT: u16 = 8080;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 100;

pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_PATH";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_STREAMS_0_CONSUMER_GROUP";

pub(super) const ENV_SINK_BASE_URL: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_BASE_URL";
pub(super) const ENV_SINK_DAG_ID: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_DAG_ID";
pub(super) const ENV_SINK_AUTH: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_AUTH";
pub(super) const ENV_SINK_TIMEOUT: &str = "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_TIMEOUT";
pub(super) const ENV_SINK_MAX_RETRIES: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_MAX_RETRIES";
pub(super) const ENV_SINK_RETRY_DELAY: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_RETRY_DELAY";
pub(super) const ENV_SINK_HEALTH_CHECK: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_HEALTH_CHECK_ENABLED";
pub(super) const ENV_SINK_VERBOSE_LOGGING: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_VERBOSE_LOGGING";
pub(super) const ENV_SINK_INCLUDE_IGGY_META: &str =
    "IGGY_CONNECTORS_SINK_AIRFLOW_PLUGIN_CONFIG_INCLUDE_IGGY_METADATA_IN_CONF";

/// WireMock stand-in for Airflow REST API endpoints used by the sink.
pub struct AirflowWireMockContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) base_url: String,
}

impl AirflowWireMockContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let current_dir = std::env::current_dir().map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "AirflowWireMockContainer".to_string(),
            message: format!("Failed to get current dir: {e}"),
        })?;

        let container = GenericImage::new(WIREMOCK_IMAGE, WIREMOCK_TAG)
            .with_exposed_port(WIREMOCK_PORT.tcp())
            .with_wait_for(Healthcheck(HealthWaitStrategy::default()))
            .with_mount(Mount::bind_mount(
                current_dir
                    .join("tests/connectors/airflow/wiremock/mappings")
                    .to_string_lossy()
                    .to_string(),
                "/home/wiremock/mappings",
            ))
            .with_container_name(fixtures::unique_container_name("wiremock-airflow"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "AirflowWireMockContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host = container
            .get_host()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "AirflowWireMockContainer".to_string(),
                message: format!("Failed to get host: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(WIREMOCK_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "AirflowWireMockContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let base_url = format!("http://{host}:{host_port}");
        info!("Airflow sink WireMock container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }

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
            })
            .collect();

        Ok(requests)
    }

    pub async fn wait_for_dag_run_requests(
        &self,
        expected: usize,
    ) -> Result<Vec<WireMockRequest>, TestBinaryError> {
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            let requests = self.get_received_requests().await?;
            let dag_runs: Vec<_> = requests
                .into_iter()
                .filter(|r| r.method == "POST" && r.url.contains("/dagRuns"))
                .collect();
            if dag_runs.len() >= expected {
                info!(
                    "WireMock received {} DAG-run POSTs (expected {})",
                    dag_runs.len(),
                    expected
                );
                return Ok(dag_runs);
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }

        let actual = self
            .get_received_requests()
            .await?
            .into_iter()
            .filter(|r| r.method == "POST" && r.url.contains("/dagRuns"))
            .count();
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected} DAG-run POSTs after {} attempts, got {actual}",
                DEFAULT_POLL_ATTEMPTS
            ),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WireMockRequest {
    pub method: String,
    pub url: String,
    pub body: String,
}

impl WireMockRequest {
    pub fn body_as_json(&self) -> Result<serde_json::Value, TestBinaryError> {
        serde_json::from_str(&self.body).map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to parse request body as JSON: {e}"),
        })
    }
}
