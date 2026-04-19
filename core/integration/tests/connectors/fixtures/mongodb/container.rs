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
use mongodb::{Client, bson::doc, options::ClientOptions};
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const MONGODB_IMAGE: &str = "mongo";
const MONGODB_TAG: &str = "7";
const MONGODB_PORT: u16 = 27017;
const MONGODB_READY_MSG: &str = "Waiting for connections";
const MONGODB_REPLICA_SET_NAME: &str = "rs0";
const MONGODB_INIT_ATTEMPTS: usize = 120;
const MONGODB_INIT_INTERVAL_MS: u64 = 250;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_SINK_COLLECTION: &str = "iggy_messages";
pub(super) const DEFAULT_TEST_DATABASE: &str = "iggy_test";

pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

// Sink env vars
pub(super) const ENV_SINK_CONNECTION_URI: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_CONNECTION_URI";
pub(super) const ENV_SINK_DATABASE: &str = "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_DATABASE";
pub(super) const ENV_SINK_COLLECTION: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_COLLECTION";
pub(super) const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SINK_AUTO_CREATE_COLLECTION: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_AUTO_CREATE_COLLECTION";
pub(super) const ENV_SINK_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_BATCH_SIZE";
pub(super) const ENV_SINK_MAX_RETRIES: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_MAX_RETRIES";
pub(super) const ENV_SINK_RETRY_DELAY: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_PLUGIN_CONFIG_RETRY_DELAY";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_MONGODB_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_MONGODB_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_MONGODB_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_MONGODB_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_MONGODB_PATH";

/// Base container management for MongoDB fixtures.
pub struct MongoDbContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) connection_uri: String,
}

impl MongoDbContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(MONGODB_IMAGE, MONGODB_TAG)
            .with_exposed_port(MONGODB_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(MONGODB_READY_MSG))
            .with_mapped_port(0, MONGODB_PORT.tcp())
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        info!("Started MongoDB container");

        let ports = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?;
        let mapped_port = ports
            .map_to_host_port_ipv4(MONGODB_PORT)
            .or_else(|| ports.map_to_host_port_ipv6(MONGODB_PORT))
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: "No mapping for MongoDB port".to_string(),
            })?;

        // Standalone mode: plain URI. No ?directConnection=true needed
        // (directConnection is only required for single-node replica sets).
        let connection_uri = format!("mongodb://localhost:{mapped_port}");

        info!("MongoDB container available at {connection_uri}");

        Ok(Self {
            container,
            connection_uri,
        })
    }

    pub(super) async fn start_single_node_replica_set(
        enable_test_commands: bool,
    ) -> Result<Self, TestBinaryError> {
        let mut image = GenericImage::new(MONGODB_IMAGE, MONGODB_TAG)
            .with_exposed_port(MONGODB_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(MONGODB_READY_MSG))
            .with_mapped_port(0, MONGODB_PORT.tcp())
            .with_cmd(["--replSet", MONGODB_REPLICA_SET_NAME, "--bind_ip_all"]);

        if enable_test_commands {
            image = image.with_cmd([
                "--replSet",
                MONGODB_REPLICA_SET_NAME,
                "--bind_ip_all",
                "--setParameter",
                "enableTestCommands=1",
            ]);
        }

        let container = image
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to start replica-set container: {e}"),
            })?;

        let ports = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?;
        let mapped_port = ports
            .map_to_host_port_ipv4(MONGODB_PORT)
            .or_else(|| ports.map_to_host_port_ipv6(MONGODB_PORT))
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: "No mapping for MongoDB port".to_string(),
            })?;

        let bootstrap_uri = format!("mongodb://localhost:{mapped_port}/?directConnection=true");
        let options = ClientOptions::parse(&bootstrap_uri).await.map_err(|e| {
            TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to parse bootstrap URI: {e}"),
            }
        })?;
        let bootstrap_client =
            Client::with_options(options).map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to create bootstrap client: {e}"),
            })?;

        let _ = bootstrap_client
            .database("admin")
            .run_command(doc! {
                "replSetInitiate": {
                    "_id": MONGODB_REPLICA_SET_NAME,
                    "members": [
                        {
                            "_id": 0,
                            "host": format!("localhost:{MONGODB_PORT}")
                        }
                    ]
                }
            })
            .await;

        let mut initialized = false;
        for _ in 0..MONGODB_INIT_ATTEMPTS {
            let status = bootstrap_client
                .database("admin")
                .run_command(doc! { "replSetGetStatus": 1 })
                .await;

            if let Ok(status) = status
                && status.get_i32("myState").ok() == Some(1)
            {
                initialized = true;
                break;
            }

            sleep(Duration::from_millis(MONGODB_INIT_INTERVAL_MS)).await;
        }

        if !initialized {
            return Err(TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: "Replica set failed to reach PRIMARY state".to_string(),
            });
        }

        let connection_uri = format!(
            "mongodb://localhost:{mapped_port}/?replicaSet={MONGODB_REPLICA_SET_NAME}&directConnection=true"
        );
        info!("MongoDB single-node replica set available at {connection_uri}");

        Ok(Self {
            container,
            connection_uri,
        })
    }

    pub async fn create_client(&self) -> Result<Client, TestBinaryError> {
        let options = ClientOptions::parse(&self.connection_uri)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbContainer".to_string(),
                message: format!("Failed to parse URI: {e}"),
            })?;

        Client::with_options(options).map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "MongoDbContainer".to_string(),
            message: format!("Failed to create client: {e}"),
        })
    }
}

/// Common MongoDB operations for fixtures.
pub trait MongoDbOps: Sync {
    fn container(&self) -> &MongoDbContainer;

    fn create_client(
        &self,
    ) -> impl std::future::Future<Output = Result<Client, TestBinaryError>> + Send {
        self.container().create_client()
    }
}
