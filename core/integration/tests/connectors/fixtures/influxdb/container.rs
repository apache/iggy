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
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;

const INFLUXDB_IMAGE: &str = "influxdb";
const INFLUXDB_TAG: &str = "2.7-alpine";
const INFLUXDB_PORT: u16 = 8086;

pub const INFLUXDB_ORG: &str = "iggy-test-org";
pub const INFLUXDB_BUCKET: &str = "iggy-test-bucket";
pub const INFLUXDB_TOKEN: &str = "iggy-test-secret-token";
pub const INFLUXDB_USERNAME: &str = "iggy-admin";
pub const INFLUXDB_PASSWORD: &str = "iggy-password";

/// Number of attempts to poll `/ping` before giving up.
pub const HEALTH_CHECK_ATTEMPTS: usize = 60;
/// Milliseconds between each `/ping` attempt.
pub const HEALTH_CHECK_INTERVAL_MS: u64 = 1_000;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

// ── env-var keys injected into the connectors runtime ────────────────────────

pub const ENV_SOURCE_URL: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_URL";
pub const ENV_SOURCE_ORG: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_ORG";
pub const ENV_SOURCE_TOKEN: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_TOKEN";
pub const ENV_SOURCE_QUERY: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_QUERY";
pub const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_POLL_INTERVAL";
pub const ENV_SOURCE_BATCH_SIZE: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_SOURCE_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_STREAM";
pub const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_TOPIC";
pub const ENV_SOURCE_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_SCHEMA";
pub const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PATH";

pub const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL";
pub const ENV_SINK_ORG: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_ORG";
pub const ENV_SINK_TOKEN: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_TOKEN";
pub const ENV_SINK_BUCKET: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_BUCKET";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_CONSUMER_GROUP";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PATH";
pub const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub const ENV_SINK_PRECISION: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PRECISION";
pub const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_METADATA";
pub const ENV_SINK_INCLUDE_CHECKSUM: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_CHECKSUM";
pub const ENV_SINK_INCLUDE_ORIGIN_TIMESTAMP: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_ORIGIN_TIMESTAMP";
pub const ENV_SINK_INCLUDE_STREAM_TAG: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_STREAM_TAG";
pub const ENV_SINK_INCLUDE_TOPIC_TAG: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_TOPIC_TAG";
pub const ENV_SINK_INCLUDE_PARTITION_TAG: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_INCLUDE_PARTITION_TAG";
pub const ENV_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub const ENV_SOURCE_PAYLOAD_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_COLUMN";

// ── Container ────────────────────────────────────────────────────────────────

pub struct InfluxDbContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl InfluxDbContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container: ContainerAsync<GenericImage> =
            GenericImage::new(INFLUXDB_IMAGE, INFLUXDB_TAG)
                .with_exposed_port(INFLUXDB_PORT.tcp())
                .with_wait_for(WaitFor::http(
                    HttpWaitStrategy::new("/ping")
                        .with_port(INFLUXDB_PORT.tcp())
                        .with_expected_status_code(204u16),
                ))
                .with_mapped_port(0, INFLUXDB_PORT.tcp())
                .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
                .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", INFLUXDB_USERNAME)
                .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", INFLUXDB_PASSWORD)
                .with_env_var("DOCKER_INFLUXDB_INIT_ORG", INFLUXDB_ORG)
                .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", INFLUXDB_BUCKET)
                .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", INFLUXDB_TOKEN)
                .start()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDbContainer".to_string(),
                    message: format!("Failed to start container: {e}"),
                })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "InfluxDbContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(INFLUXDB_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "InfluxDbContainer".to_string(),
                message: "No mapping for InfluxDB port".to_string(),
            })?;

        let base_url = format!("http://localhost:{mapped_port}");
        info!("InfluxDB container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

// ── HTTP client ───────────────────────────────────────────────────────────────

pub fn create_http_client() -> HttpClient {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    reqwest_middleware::ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

// ── Shared InfluxDB operations ────────────────────────────────────────────────

pub trait InfluxDbOps: Sync {
    fn container(&self) -> &InfluxDbContainer;
    fn http_client(&self) -> &HttpClient;

    /// Write line-protocol lines into the test bucket.
    fn write_lines(
        &self,
        lines: &[&str],
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!(
                "{}/api/v2/write?org={}&bucket={}&precision=ns",
                self.container().base_url,
                INFLUXDB_ORG,
                INFLUXDB_BUCKET,
            );
            let body = lines.join("\n");

            let response = self
                .http_client()
                .post(&url)
                .header("Authorization", format!("Token {INFLUXDB_TOKEN}"))
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body)
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDbOps".to_string(),
                    message: format!("Failed to write lines: {e}"),
                })?;

            let status = response.status();
            if !status.is_success() && status.as_u16() != 204 {
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDbOps".to_string(),
                    message: format!("Write error: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn query_count(
        &self,
        flux: &str,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            let url = format!(
                "{}/api/v2/query?org={}",
                self.container().base_url,
                INFLUXDB_ORG
            );
            let body = serde_json::json!({ "query": flux, "type": "flux" });

            let response = self
                .http_client()
                .post(&url)
                .header("Authorization", format!("Token {INFLUXDB_TOKEN}"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/csv")
                .json(&body)
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to query InfluxDB: {e}"),
                })?;

            let text = response.text().await.unwrap_or_default();
            // InfluxDB annotated CSV format:
            //   - Annotation rows start with '#'
            //   - Header row starts with ',result,table,...'  (empty first field)
            //   - Data rows start with an empty annotation field, e.g. ',_result,0,...'
            //     where the THIRD field (index 2) is the numeric table index.
            //   - Empty lines separate tables
            // Count lines whose third CSV field is a non-negative integer (data rows).
            let count = text
                .lines()
                .filter(|l| {
                    let mut fields = l.splitn(4, ',');
                    let annotation = fields.next().unwrap_or("");
                    // Data rows have an empty first field (the annotation column)
                    if !annotation.is_empty() {
                        return false;
                    }
                    fields.next(); // skip _result
                    let table = fields.next().unwrap_or("");
                    table.parse::<u64>().is_ok()
                })
                .count();
            Ok(count)
        }
    }
}
