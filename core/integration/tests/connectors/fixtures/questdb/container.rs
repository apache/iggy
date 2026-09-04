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
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;

const QUESTDB_IMAGE: &str = "docker.io/questdb/questdb";
/// QWP over WebSocket requires QuestDB 10.0 or newer, so this tag cannot be
/// rolled back to a 9.x line without disabling the whole suite.
const QUESTDB_TAG: &str = "10.0.1";
/// QuestDB serves the REST API and the QWP WebSocket upgrade on the same port.
const QUESTDB_HTTP_PORT: u16 = 9000;

pub const HEALTH_CHECK_ATTEMPTS: usize = 60;
pub const HEALTH_CHECK_INTERVAL_MS: u64 = 1_000;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

// ── env-var keys injected into the connectors runtime ────────────────────────

pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PATH";
pub const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_CONNECTION_STRING";
pub const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TABLE";
pub const ENV_SINK_TIMESTAMP_SOURCE: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_SOURCE";
pub const ENV_SINK_TIMESTAMP_FIELD: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_FIELD";
pub const ENV_SINK_TIMESTAMP_UNIT: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_UNIT";
pub const ENV_SINK_SYMBOL_COLUMNS: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_SYMBOL_COLUMNS";
pub const ENV_SINK_UUID_COLUMNS: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_UUID_COLUMNS";
pub const ENV_SINK_INCLUDE_STREAM_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_STREAM_COLUMN";
pub const ENV_SINK_INCLUDE_TOPIC_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_TOPIC_COLUMN";
pub const ENV_SINK_INCLUDE_PARTITION_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_PARTITION_COLUMN";
pub const ENV_SINK_INCLUDE_OFFSET_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_OFFSET_COLUMN";
pub const ENV_SINK_INCLUDE_HEADERS: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_HEADERS";
pub const ENV_SINK_LOG_REJECTED_PAYLOAD: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_LOG_REJECTED_PAYLOAD";
pub const ENV_SINK_BATCH_SIZE: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_SINK_FLUSH_TIMEOUT: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_FLUSH_TIMEOUT";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_CONSUMER_GROUP";

// ── Container ────────────────────────────────────────────────────────────────

pub struct QuestDbContainer {
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
    pub host_port: u16,
}

impl QuestDbContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container: ContainerAsync<GenericImage> = GenericImage::new(QUESTDB_IMAGE, QUESTDB_TAG)
            .with_exposed_port(QUESTDB_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/ping")
                    .with_port(QUESTDB_HTTP_PORT.tcp())
                    .with_expected_status_code(204u16),
            ))
            .with_mapped_port(0, QUESTDB_HTTP_PORT.tcp())
            // Keep the footprint small; these suites write a few thousand rows.
            .with_env_var("QDB_CAIRO_COMMIT_LAG", "100")
            .with_container_name(fixtures::unique_container_name("questdb"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "QuestDbContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let ports = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "QuestDbContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?;
        let host_port = ports
            .map_to_host_port_ipv4(QUESTDB_HTTP_PORT)
            .or_else(|| ports.map_to_host_port_ipv6(QUESTDB_HTTP_PORT))
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "QuestDbContainer".to_string(),
                message: "No mapping for QuestDB port".to_string(),
            })?;

        let base_url = format!("http://localhost:{host_port}");
        info!("QuestDB container available at {base_url}");

        Ok(Self {
            container,
            base_url,
            host_port,
        })
    }

    /// Connect string handed to the sink. QWP shares the REST port.
    pub fn connection_string(&self) -> String {
        format!("ws::addr=localhost:{};", self.host_port)
    }

    /// Freeze the server to simulate an outage.
    ///
    /// Pause rather than stop: the container was published with an ephemeral
    /// host port (`-p 0:9000`), and Docker re-resolves that to a *different*
    /// port when a stopped container is started again, which would strand both
    /// the sink and this fixture on a dead address. Pausing leaves networking
    /// untouched, so the port survives and the sink reconnects to the same
    /// endpoint.
    pub async fn simulate_outage(&self) -> Result<(), TestBinaryError> {
        self.container
            .pause()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to pause QuestDB container: {e}"),
            })
    }

    pub async fn resume(&self) -> Result<(), TestBinaryError> {
        self.container
            .unpause()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to unpause QuestDB container: {e}"),
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

// ── Shared QuestDB operations ─────────────────────────────────────────────────

pub trait QuestDbOps: Sync {
    fn container(&self) -> &QuestDbContainer;
    fn http_client(&self) -> &HttpClient;

    /// Run SQL through the REST `/exec` endpoint and return the parsed JSON.
    fn exec(
        &self,
        query: &str,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/exec", self.container().base_url);
            let response = self
                .http_client()
                .get(&url)
                .query(&[("query", query)])
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to query QuestDB: {e}"),
                })?;

            let text = response.text().await.unwrap_or_default();
            serde_json::from_str(&text).map_err(|e| TestBinaryError::InvalidState {
                message: format!("QuestDB returned non-JSON for `{query}`: {e}. Body: {text}"),
            })
        }
    }

    /// Row count for `table`, or `None` while the table does not exist yet.
    /// QWP auto-creates on first write, so "missing" is a normal early state
    /// rather than a failure.
    fn count_rows(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<Option<usize>, TestBinaryError>> + Send {
        async move {
            let value = self.exec(&format!("select count() from '{table}'")).await?;
            if value.get("error").is_some() {
                return Ok(None);
            }
            let count = value
                .get("dataset")
                .and_then(|rows| rows.get(0))
                .and_then(|row| row.get(0))
                .and_then(serde_json::Value::as_u64);
            Ok(count.map(|count| count as usize))
        }
    }
}
