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
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client as WsClient, Ws};
use surrealdb::opt::auth::Root;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const SURREALDB_IMAGE: &str = "docker.io/surrealdb/surrealdb";
const SURREALDB_TAG: &str = "v3.1.4";
const SURREALDB_PORT: u16 = 8000;
const SURREALDB_READY_MSG: &str = "Started web server on";
const SURREALDB_BOOT_ATTEMPTS: usize = 120;
const SURREALDB_BOOT_INTERVAL_MS: u64 = 250;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_NAMESPACE: &str = "iggy";
pub(super) const DEFAULT_DATABASE: &str = "connectors";
pub(super) const DEFAULT_TABLE: &str = "iggy_messages";
pub(super) const ROOT_USERNAME: &str = "root";
pub(super) const ROOT_PASSWORD: &str = "root";

pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 120;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

pub(super) const ENV_SINK_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_ENDPOINT";
pub(super) const ENV_SINK_NAMESPACE: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_NAMESPACE";
pub(super) const ENV_SINK_DATABASE: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_DATABASE";
pub(super) const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_TABLE";
pub(super) const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_USERNAME";
pub(super) const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_PASSWORD";
pub(super) const ENV_SINK_AUTH_SCOPE: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_AUTH_SCOPE";
pub(super) const ENV_SINK_AUTO_DEFINE_TABLE: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_AUTO_DEFINE_TABLE";
pub(super) const ENV_SINK_DEFINE_INDEXES: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_DEFINE_INDEXES";
pub(super) const ENV_SINK_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_BATCH_SIZE";
pub(super) const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_SURREALDB_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_SURREALDB_PATH";

pub type SurrealDbClient = Surreal<WsClient>;

pub struct SurrealDbContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) endpoint: String,
}

impl SurrealDbContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(SURREALDB_IMAGE, SURREALDB_TAG)
            .with_exposed_port(SURREALDB_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(SURREALDB_READY_MSG))
            .with_mapped_port(0, SURREALDB_PORT.tcp())
            .with_container_name(fixtures::unique_container_name("surrealdb"))
            .with_cmd([
                "start",
                "--log",
                "info",
                "--user",
                ROOT_USERNAME,
                "--pass",
                ROOT_PASSWORD,
                "memory",
            ])
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(SURREALDB_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: "No mapping for SurrealDB port".to_string(),
            })?;

        let endpoint = format!("127.0.0.1:{mapped_port}");
        let instance = Self {
            container,
            endpoint,
        };
        instance.wait_until_ready().await?;

        info!("SurrealDB container available at {}", instance.endpoint);
        Ok(instance)
    }

    pub async fn create_client(&self) -> Result<SurrealDbClient, TestBinaryError> {
        let client = Surreal::new::<Ws>(self.endpoint.as_str())
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: format!("Failed to connect to SurrealDB: {e}"),
            })?;

        client
            .signin(Root {
                username: ROOT_USERNAME.to_string(),
                password: ROOT_PASSWORD.to_string(),
            })
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: format!("Failed to authenticate with SurrealDB: {e}"),
            })?;

        client
            .use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "SurrealDbContainer".to_string(),
                message: format!("Failed to select namespace/database: {e}"),
            })?;

        Ok(client)
    }

    async fn wait_until_ready(&self) -> Result<(), TestBinaryError> {
        for _ in 0..SURREALDB_BOOT_ATTEMPTS {
            if let Ok(client) = self.create_client().await
                && client.health().await.is_ok()
            {
                return Ok(());
            }
            sleep(Duration::from_millis(SURREALDB_BOOT_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "SurrealDbContainer".to_string(),
            message: "SurrealDB did not become ready".to_string(),
        })
    }
}

pub trait SurrealDbOps: Sync {
    fn container(&self) -> &SurrealDbContainer;

    fn create_client(
        &self,
    ) -> impl std::future::Future<Output = Result<SurrealDbClient, TestBinaryError>> + Send {
        self.container().create_client()
    }
}
