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
use reqwest::Client as HttpClient;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.1";
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

pub const CLICKHOUSE_TEST_USER: &str = "default";
pub const CLICKHOUSE_TEST_PASSWORD: &str = "iggy_test";

pub const HEALTH_CHECK_ATTEMPTS: usize = 60;
pub const HEALTH_CHECK_INTERVAL_MS: u64 = 500;
pub const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 100;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub const DEFAULT_DATABASE: &str = "default";
pub const DEFAULT_SINK_TABLE: &str = "iggy_messages";

pub const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_URL";
pub const ENV_SINK_DATABASE: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_DATABASE";
pub const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_TABLE";
pub const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_USERNAME";
pub const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_PASSWORD";
pub const ENV_SINK_INSERT_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_INSERT_FORMAT";
pub const ENV_SINK_STRING_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_STRING_FORMAT";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_CONSUMER_GROUP";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PATH";

/// DDL for the test table — matches the `TestMessage` struct.
pub const CREATE_TABLE_SQL: &str = "
    CREATE TABLE IF NOT EXISTS iggy_messages (
        id        UInt64,
        name      String,
        count     UInt32,
        amount    Float64,
        active    Bool,
        timestamp Int64
    ) ENGINE = MergeTree()
    ORDER BY id
";

#[derive(Debug, Deserialize)]
pub struct ClickHouseJsonResponse {
    pub data: Vec<serde_json::Value>,
    #[allow(dead_code)]
    pub rows: usize,
}

pub struct ClickHouseContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl ClickHouseContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(CLICKHOUSE_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::Nothing)
            .with_mapped_port(0, CLICKHOUSE_HTTP_PORT.tcp())
            .with_env_var("CLICKHOUSE_USER", CLICKHOUSE_TEST_USER)
            .with_env_var("CLICKHOUSE_PASSWORD", CLICKHOUSE_TEST_PASSWORD)
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        info!("Started ClickHouse container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(CLICKHOUSE_HTTP_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: "No mapping for ClickHouse HTTP port".to_string(),
            })?;

        let base_url = format!("http://localhost:{mapped_port}");
        info!("ClickHouse container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

pub fn create_http_client() -> HttpClient {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-ClickHouse-User",
        HeaderValue::from_static(CLICKHOUSE_TEST_USER),
    );
    headers.insert(
        "X-ClickHouse-Key",
        HeaderValue::from_static(CLICKHOUSE_TEST_PASSWORD),
    );
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()
        .expect("Failed to build HTTP client")
}

/// Common operations against ClickHouse via its HTTP API.
pub trait ClickHouseOps: Sync {
    fn container(&self) -> &ClickHouseContainer;
    fn http_client(&self) -> &HttpClient;

    /// Execute a DDL or DML statement (no result expected).
    #[allow(dead_code)]
    fn execute_query(
        &self,
        sql: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = &self.container().base_url;
            let response = self
                .http_client()
                .post(url)
                .body(sql.to_string())
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "ClickHouseOps".to_string(),
                    message: format!("Failed to execute query: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "ClickHouseOps".to_string(),
                    message: format!("Query failed: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    /// Count the rows in a table.
    fn count_rows(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            let sql = format!("SELECT count() AS c FROM {table} FORMAT JSON");
            let url = &self.container().base_url;
            let response = self
                .http_client()
                .post(url)
                .body(sql)
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to count rows: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("count() failed: status={status}, body={body}"),
                });
            }

            let parsed: ClickHouseJsonResponse =
                response
                    .json()
                    .await
                    .map_err(|e| TestBinaryError::InvalidState {
                        message: format!("Failed to parse count response: {e}"),
                    })?;

            let count = parsed
                .data
                .first()
                .and_then(|row| row.get("c"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);

            Ok(count)
        }
    }

    /// Poll until the table contains at least `expected` rows.
    fn wait_for_rows(
        &self,
        table: &str,
        expected: usize,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            for _ in 0..DEFAULT_POLL_ATTEMPTS {
                match self.count_rows(table).await {
                    Ok(count) if count >= expected => {
                        info!("Found {count} rows in {table} (expected {expected})");
                        return Ok(count);
                    }
                    Ok(_) => {}
                    Err(_) => {}
                }
                sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
            }

            let final_count = self.count_rows(table).await.unwrap_or(0);
            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected at least {expected} rows in {table}, found {final_count} after {} attempts",
                    DEFAULT_POLL_ATTEMPTS
                ),
            })
        }
    }

    /// Fetch all rows from a table ordered by `id`, returned as JSON objects.
    fn fetch_rows_as_json(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<Vec<serde_json::Value>, TestBinaryError>> + Send
    {
        async move {
            let sql = format!("SELECT * FROM {table} ORDER BY id FORMAT JSON");
            let url = &self.container().base_url;
            let response = self
                .http_client()
                .post(url)
                .body(sql)
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to fetch rows: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("SELECT failed: status={status}, body={body}"),
                });
            }

            let parsed: ClickHouseJsonResponse =
                response
                    .json()
                    .await
                    .map_err(|e| TestBinaryError::InvalidState {
                        message: format!("Failed to parse SELECT response: {e}"),
                    })?;

            Ok(parsed.data)
        }
    }
}