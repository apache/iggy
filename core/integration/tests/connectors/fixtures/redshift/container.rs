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

use std::sync::Arc;

use integration::harness::TestBinaryError;
use pgwire::tokio::process_socket;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use testcontainers_modules::postgres;
use tokio::{net::TcpListener, task::JoinHandle};

use crate::connectors::fixtures::{
    self,
    redshift::redshift_mock::handler::{RedshiftHandler, RedshiftHandlerFactory},
};

const MINIO_IMAGE: &str = "docker.io/minio/minio";
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";
const MINIO_PORT: u16 = 9000;
const MINIO_CONSOLE_PORT: u16 = 9001;
const POSTGRES_PORT: u16 = 5432;

pub const MINIO_ACCESS_KEY: &str = "admin";
pub const MINIO_SECRET_KEY: &str = "password";
pub const MINIO_BUCKET: &str = "iggystaging";
pub const DEFAULT_SINK_TABLE: &str = "iggy_messages";
pub const STAGING_REGION: &str = "us-east-1";
pub const STAGING_PREFIX: &str = "iggy/messages";

pub const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_CONNECTION_STRING";
pub const ENV_SINK_TARGET_TABLE: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_TARGET_TABLE";
pub const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub const ENV_SINK_STAGING_ACCESS_KEY: &str =
    "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_AWS_ACCESS_KEY_ID";
pub const ENV_SINK_STAGING_SECRET: &str =
    "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_AWS_SECRET_ACCESS_KEY";
pub const ENV_SINK_S3_BUCKET: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_BUCKET";
pub const ENV_SINK_S3_PREFIX: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_PREFIX";
pub const ENV_SINK_S3_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_ENDPOINT";
pub const ENV_SINK_STAGING_REGION: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_AWS_REGION";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PATH";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_CONSUMER_GROUP";
pub const ENV_SINK_ARCHIVE: &str = "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_ARCHIVE";
pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

pub const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

pub struct MinioContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
}

impl MinioContainer {
    pub async fn start(network: &str, container_name: &str) -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(MINIO_PORT.tcp())
            .with_exposed_port(MINIO_CONSOLE_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/minio/health/live")
                    .with_port(MINIO_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_network(network)
            .with_container_name(container_name)
            .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
            .with_mapped_port(0, MINIO_PORT.tcp())
            .with_mapped_port(0, MINIO_CONSOLE_PORT.tcp())
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MinioContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        tracing::info!("Started MinIO container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MinioContainer".to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(MINIO_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MinioContainer".to_string(),
                message: "No mapping for MinIO port".to_string(),
            })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        tracing::info!("MinIO container available at {endpoint}");

        Ok(Self {
            container,
            endpoint,
        })
    }
}

/// Base container management for PostgreSQL fixtures.
pub struct PostgresContainer {
    #[allow(dead_code)]
    container: ContainerAsync<postgres::Postgres>,
    pub connection_string: String,
}

impl PostgresContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = postgres::Postgres::default()
            .with_container_name(fixtures::unique_container_name("postgres"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "PostgresContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(POSTGRES_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "PostgresContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let connection_string = format!("postgres://postgres:postgres@localhost:{host_port}");

        Ok(Self {
            container,
            connection_string,
        })
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.connection_string)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "PostgresContainer".to_string(),
                message: format!("Failed to connect: {e}"),
            })
    }
}

pub struct RedshiftContainer {
    #[allow(dead_code)]
    accept_task: JoinHandle<()>,
    pub connection_string: String,
}

impl RedshiftContainer {
    pub async fn start(
        target_connection: String,
        s3_endpoint: String,
    ) -> Result<Self, TestBinaryError> {
        let (pg_client, connection) =
            tokio_postgres::connect(&target_connection, tokio_postgres::NoTls)
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "RedshiftContainer".into(),
                    message: e.to_string(),
                })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("{}", e.to_string())
            }
        });

        let redshql = RedshiftHandler::new(pg_client, s3_endpoint);

        let factory = Arc::new(RedshiftHandlerFactory {
            handler: Arc::new(redshql),
        });

        let listener =
            TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "RedshiftMockContainer".to_string(),
                    message: format!("bind failed: {e}"),
                })?;

        let host_port = listener
            .local_addr()
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RedshiftMockContainer".to_string(),
                message: format!("failed to get local address: {e}"),
            })?
            .port();

        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((incoming_socket, _addr)) => {
                        let factory_ref = factory.clone();

                        tokio::spawn(async move {
                            if let Err(e) = process_socket(incoming_socket, None, factory_ref).await
                            {
                                panic!("{}", e.to_string())
                            }
                        });
                    }

                    Err(e) => {
                        panic!("{}", e.to_string())
                    }
                }
            }
        });

        Ok(Self {
            accept_task,
            connection_string: format!("postgres://postgres@localhost:{host_port}/postgres"),
        })
    }
}

/// Payload format for sink connector.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkPayloadFormat {
    #[default]
    Bytea,
    Text,
}

/// Schema format for message encoding.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkSchema {
    #[default]
    Json,
    Raw,
}
