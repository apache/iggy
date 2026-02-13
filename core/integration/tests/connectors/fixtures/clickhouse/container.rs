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

use clickhouse::{Row, RowOwned, RowRead};
use integration::harness::TestBinaryError;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;

pub(super) const CLICKHOUSE_HTTP_PORT: u16 = 8123;
pub(super) const CLICKHOUSE_IMAGE_TAG: &str = "latest-alpine"; //"25.3-alpine";
pub(super) const CLICKHOUSE_DEFAULT_USER: &str = "default";
pub(super) const CLICKHOUSE_DEFAULT_PASSWORD: &str = "";
pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";

// Env var constants — prefix: IGGY_CONNECTORS_SINK_CLICKHOUSE_
pub(super) const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_URL";
pub(super) const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_TABLE";
pub(super) const ENV_SINK_INSERT_TYPE: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_INSERT_TYPE";
pub(super) const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SINK_COMPRESSION: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_COMPRESSION_ENABLED";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_AUTH_TYPE: &str =
    "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_AUTH_TYPE";
pub(super) const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_USERNAME";
pub(super) const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PLUGIN_CONFIG_PASSWORD";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_CLICKHOUSE_PATH";

/// Trait for ClickHouse fixtures with common container operations.
pub trait ClickHouseOps: Sync {
    fn container(&self) -> &ClickHouseContainer;

    fn create_client(&self) -> clickhouse::Client {
        self.container().create_client()
    }

    fn wait_for_table(
        &self,
        client: &clickhouse::Client,
        table: &str,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let query = format!("SELECT 1 FROM {table} LIMIT 1");
            for _ in 0..DEFAULT_POLL_ATTEMPTS {
                if client.query(&query).fetch_optional::<u8>().await.is_ok() {
                    return;
                }
                sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
            }
            panic!("Table {table} was not created in time");
        }
    }

    fn fetch_rows<T>(
        &self,
        client: &clickhouse::Client,
        query: &str,
        expected_count: usize,
    ) -> impl std::future::Future<Output = Result<Vec<T>, TestBinaryError>> + Send
    where
        T: Row + RowOwned + RowRead + Send,
    {
        async move {
            let mut rows = Vec::new();
            for _ in 0..DEFAULT_POLL_ATTEMPTS {
                if let Ok(fetched) = client.query(query).fetch_all::<T>().await {
                    rows = fetched;
                    if rows.len() >= expected_count {
                        return Ok(rows);
                    }
                }
                sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
            }
            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected {} rows but got {} after {} poll attempts",
                    expected_count,
                    rows.len(),
                    DEFAULT_POLL_ATTEMPTS
                ),
            })
        }
    }
}

/// Base container management for ClickHouse fixtures.
pub struct ClickHouseContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) url: String,
}

const CONTAINER_START_RETRIES: usize = 3;

impl ClickHouseContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let mut last_err = None;

        for attempt in 1..=CONTAINER_START_RETRIES {
            match Self::try_start().await {
                Ok(container) => return Ok(container),
                Err(e) => {
                    eprintln!(
                        "ClickHouse container start attempt {attempt}/{CONTAINER_START_RETRIES} failed: {e}"
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    async fn try_start() -> Result<Self, TestBinaryError> {
        let current_dir = std::env::current_dir().map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "ClickHouseContainer".to_string(),
            message: format!("Failed to get current dir: {e}"),
        })?;

        let fixtures_dir = current_dir.join("tests/connectors/fixtures/clickhouse/image_configs");

        let container = GenericImage::new("clickhouse/clickhouse-server", CLICKHOUSE_IMAGE_TAG)
            .with_exposed_port(CLICKHOUSE_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/")
                    .with_port(CLICKHOUSE_HTTP_PORT.tcp())
                    .with_expected_status_code(200_u16),
            ))
            .with_mount(Mount::bind_mount(
                fixtures_dir
                    .join("config.xml")
                    .to_string_lossy()
                    .to_string(),
                "/etc/clickhouse-server/config.xml",
            ))
            .with_mount(Mount::bind_mount(
                fixtures_dir.join("users.xml").to_string_lossy().to_string(),
                "/etc/clickhouse-server/users.xml",
            ))
            .with_mount(Mount::bind_mount(
                fixtures_dir
                    .join("default-user.xml")
                    .to_string_lossy()
                    .to_string(),
                "/etc/clickhouse-server/users.d/default-user.xml",
            ))
            .with_mount(Mount::bind_mount(
                fixtures_dir
                    .join("docker_related_config.xml")
                    .to_string_lossy()
                    .to_string(),
                "/etc/clickhouse-server/config.d/docker_related_config.xml",
            ))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(CLICKHOUSE_HTTP_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let url = format!("http://localhost:{host_port}");

        Ok(Self { container, url })
    }

    pub fn create_client(&self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.url)
            .with_user(CLICKHOUSE_DEFAULT_USER)
            .with_password(CLICKHOUSE_DEFAULT_PASSWORD)
            .with_compression(clickhouse::Compression::None)
    }

    pub async fn execute_ddl(
        &self,
        client: &clickhouse::Client,
        ddl: &str,
    ) -> Result<(), TestBinaryError> {
        client
            .query(ddl)
            .execute()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ClickHouseContainer".to_string(),
                message: format!("Failed to execute DDL: {e}"),
            })
    }
}
