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
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use testcontainers_modules::{
    postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};

pub(super) const POSTGRES_PORT: u16 = 5432;
pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

pub(super) const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";
pub(super) const ENV_SINK_TARGET_TABLE: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_TARGET_TABLE";
pub(super) const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_POSTGRES_PATH";

pub(super) const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";
pub(super) const ENV_SOURCE_TABLES: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_TABLES";
pub(super) const ENV_SOURCE_TRACKING_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_TRACKING_COLUMN";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_PATH";
pub(super) const ENV_SOURCE_PAYLOAD_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PAYLOAD_COLUMN";
pub(super) const ENV_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SOURCE_DELETE_AFTER_READ: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_DELETE_AFTER_READ";
pub(super) const ENV_SOURCE_PRIMARY_KEY_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PRIMARY_KEY_COLUMN";
pub(super) const ENV_SOURCE_PROCESSED_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PROCESSED_COLUMN";
pub(super) const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_INCLUDE_METADATA";

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_SINK_TABLE: &str = "iggy_messages";

/// Payload format for sink connector.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkPayloadFormat {
    #[default]
    Bytea,
    Json,
}

/// Schema format for message encoding.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkSchema {
    #[default]
    Json,
    Raw,
}

/// Trait for PostgreSQL fixtures with common container operations.
pub trait PostgresOps: Sync {
    fn container(&self) -> &PostgresContainer;

    fn create_pool(
        &self,
    ) -> impl std::future::Future<Output = Result<Pool<Postgres>, TestBinaryError>> + Send {
        self.container().create_pool()
    }
}

/// Extension of `PostgresOps` for source fixtures that operate on a specific table.
pub trait PostgresSourceOps: PostgresOps {
    fn table_name(&self) -> &str;

    fn count_rows<'a>(
        &'a self,
        pool: &'a Pool<Postgres>,
    ) -> impl std::future::Future<Output = i64> + Send + 'a {
        async move {
            let query = format!("SELECT COUNT(*) FROM {}", self.table_name());
            let count: (i64,) = sqlx::query_as(&query)
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
            count.0
        }
    }
}

/// Base container management for PostgreSQL fixtures.
pub struct PostgresContainer {
    #[allow(dead_code)]
    container: ContainerAsync<postgres::Postgres>,
    pub(super) connection_string: String,
}

impl PostgresContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = postgres::Postgres::default().start().await.map_err(|e| {
            TestBinaryError::FixtureSetup {
                fixture_type: "PostgresContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            }
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
