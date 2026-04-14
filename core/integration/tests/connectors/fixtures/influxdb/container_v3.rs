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

//! InfluxDB 3.x test container and shared HTTP operations.
//!
//! InfluxDB 3.x uses a simplified data model:
//! - No `org` required in write/query URLs.
//! - `db` replaces `bucket`.
//! - Write: `POST /api/v3/write_lp?db=X&precision=P`
//! - Query: `POST /api/v3/query_sql` with `{"db":…,"q":…,"format":"jsonl"}`
//! - Auth: `Authorization: Bearer {token}`

use integration::harness::TestBinaryError;
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;

// InfluxDB 3.x Core Docker image.
// `influxdb:3-core` is the official OSS image for InfluxDB 3 Core on Docker Hub.
const INFLUXDB3_IMAGE: &str = "influxdb";
const INFLUXDB3_TAG: &str = "3-core";
const INFLUXDB3_PORT: u16 = 8181;

pub const INFLUXDB3_DB: &str = "iggy-test-db";
pub const INFLUXDB3_TOKEN: &str = "iggy-v3-test-token";

/// Number of `/ping` attempts before giving up.
pub const HEALTH_CHECK_ATTEMPTS_V3: usize = 60;
/// Milliseconds between each `/ping` attempt.
pub const HEALTH_CHECK_INTERVAL_MS_V3: u64 = 1_000;

pub const DEFAULT_TEST_STREAM_V3: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC_V3: &str = "test_topic";

// ── env-var keys injected into the connectors runtime ────────────────────────

pub const ENV_V3_SINK_URL: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL";
pub const ENV_V3_SINK_TOKEN: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_TOKEN";
pub const ENV_V3_SINK_DB: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_DB";
pub const ENV_V3_SINK_VERSION: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_VERSION";
pub const ENV_V3_SINK_ORG: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_ORG";
pub const ENV_V3_SINK_BUCKET: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_BUCKET";
pub const ENV_V3_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_STREAM";
pub const ENV_V3_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_TOPICS";
pub const ENV_V3_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_SCHEMA";
pub const ENV_V3_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_CONSUMER_GROUP";
pub const ENV_V3_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_INFLUXDB_PATH";
pub const ENV_V3_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT";

pub const ENV_V3_SOURCE_URL: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_URL";
pub const ENV_V3_SOURCE_TOKEN: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_TOKEN";
pub const ENV_V3_SOURCE_DB: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_DB";
pub const ENV_V3_SOURCE_VERSION: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_VERSION";
pub const ENV_V3_SOURCE_ORG: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_ORG";
pub const ENV_V3_SOURCE_QUERY: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_QUERY";
pub const ENV_V3_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_POLL_INTERVAL";
pub const ENV_V3_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_V3_SOURCE_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_STREAM";
pub const ENV_V3_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_TOPIC";
pub const ENV_V3_SOURCE_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_SCHEMA";
pub const ENV_V3_SOURCE_CURSOR_FIELD: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_CURSOR_FIELD";
pub const ENV_V3_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_INFLUXDB_PATH";
pub const ENV_V3_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT";

// ── Container ────────────────────────────────────────────────────────────────

pub struct InfluxDb3Container {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl InfluxDb3Container {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container: ContainerAsync<GenericImage> =
            GenericImage::new(INFLUXDB3_IMAGE, INFLUXDB3_TAG)
                .with_exposed_port(INFLUXDB3_PORT.tcp())
                // InfluxDB 3 Core logs "startup time:" on stdout when the HTTP
                // listener is accepting connections.
                .with_wait_for(WaitFor::message_on_stdout("startup time:"))
                .with_startup_timeout(std::time::Duration::from_secs(60))
                .with_mapped_port(0, INFLUXDB3_PORT.tcp())
                // The influxdb:3-core image has no ENTRYPOINT, so the full
                // binary invocation must be the CMD.  `--object-store memory`
                // uses an ephemeral in-memory store (no disk I/O, perfect for
                // tests).  `--without-auth` disables token auth so test
                // fixtures can write/query without managing real tokens.
                .with_cmd(vec![
                    "influxdb3",
                    "serve",
                    "--node-id",
                    "node0",
                    "--object-store",
                    "memory",
                    "--without-auth",
                ])
                .start()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDb3Container".to_string(),
                    message: format!("Failed to start container: {e}"),
                })?;

        let mapped_port = container
            .get_host_port_ipv4(INFLUXDB3_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "InfluxDb3Container".to_string(),
                message: format!("No mapping for InfluxDB 3 port: {e}"),
            })?;

        let base_url = format!("http://localhost:{mapped_port}");
        info!("InfluxDB 3 container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

// ── HTTP client ───────────────────────────────────────────────────────────────

pub fn create_http_client_v3() -> HttpClient {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    reqwest_middleware::ClientBuilder::new(client).build()
}

// ── Shared InfluxDB 3 operations ──────────────────────────────────────────────

pub trait InfluxDb3Ops: Sync {
    fn container(&self) -> &InfluxDb3Container;
    fn http_client(&self) -> &HttpClient;

    /// Write line-protocol lines into the test database.
    fn write_lines(
        &self,
        lines: &[&str],
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!(
                "{}/api/v3/write_lp?db={}&precision=ns",
                self.container().base_url,
                INFLUXDB3_DB,
            );
            let body = lines.join("\n");

            let response = self
                .http_client()
                .post(&url)
                .header("Authorization", format!("Bearer {INFLUXDB3_TOKEN}"))
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body)
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDb3Ops".to_string(),
                    message: format!("Failed to write lines: {e}"),
                })?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "InfluxDb3Ops".to_string(),
                    message: format!("Write error: status={status}, body={body}"),
                });
            }
            Ok(())
        }
    }

    /// Count rows matching a SQL query in the test database.
    fn query_count(
        &self,
        sql: &str,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/api/v3/query_sql", self.container().base_url);
            let body = serde_json::json!({
                "db":     INFLUXDB3_DB,
                "q":      sql,
                "format": "jsonl"
            });

            let response = match self
                .http_client()
                .post(&url)
                .header("Authorization", format!("Bearer {INFLUXDB3_TOKEN}"))
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
            {
                Ok(r) => r,
                // Network / timeout errors — table likely not yet created.
                Err(_) => return Ok(0),
            };

            // InfluxDB 3 returns 5xx when the table/namespace does not yet
            // exist. Treat any non-2xx as "0 rows" so the polling loop keeps
            // going without triggering the retry middleware.
            if !response.status().is_success() {
                return Ok(0);
            }

            let text = response.text().await.unwrap_or_default();
            // JSONL: count non-empty lines — each is one result row.
            let count = text.lines().filter(|l| !l.trim().is_empty()).count();
            Ok(count)
        }
    }
}
