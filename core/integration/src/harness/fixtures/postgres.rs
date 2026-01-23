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

//! PostgreSQL container fixtures for connector tests.

use super::TestFixture;
use crate::harness::error::TestBinaryError;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::{
    postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use tokio::time::sleep;

const POSTGRES_PORT: u16 = 5432;
const DEFAULT_POLL_ATTEMPTS: usize = 100;
const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";
const ENV_SINK_TARGET_TABLE: &str = "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_TARGET_TABLE";
const ENV_SINK_PAYLOAD_FORMAT: &str = "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_PAYLOAD_FORMAT";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_POSTGRES_PATH";

const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";
const ENV_SOURCE_TABLES: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_TABLES";
const ENV_SOURCE_TRACKING_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_TRACKING_COLUMN";
const ENV_SOURCE_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_STREAM";
const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_TOPIC";
const ENV_SOURCE_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_STREAMS_0_SCHEMA";
const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_POLL_INTERVAL";
const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_POSTGRES_PATH";
const ENV_SOURCE_PAYLOAD_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PAYLOAD_COLUMN";
const ENV_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PAYLOAD_FORMAT";
const ENV_SOURCE_DELETE_AFTER_READ: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_DELETE_AFTER_READ";
const ENV_SOURCE_PRIMARY_KEY_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PRIMARY_KEY_COLUMN";
const ENV_SOURCE_PROCESSED_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_PROCESSED_COLUMN";
const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_INCLUDE_METADATA";

const DEFAULT_TEST_STREAM: &str = "test_stream";
const DEFAULT_TEST_TOPIC: &str = "test_topic";
const DEFAULT_SINK_TABLE: &str = "iggy_messages";

/// Base container management for PostgreSQL fixtures.
pub struct PostgresContainer {
    #[allow(dead_code)]
    container: ContainerAsync<postgres::Postgres>,
    connection_string: String,
}

impl PostgresContainer {
    async fn start() -> Result<Self, TestBinaryError> {
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

    pub fn connection_string(&self) -> &str {
        &self.connection_string
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

/// Payload format for sink connector.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkPayloadFormat {
    /// Store as bytea (default)
    #[default]
    Bytea,
    /// Store as JSONB
    Json,
}

/// Schema format for message encoding.
#[derive(Debug, Clone, Copy, Default)]
pub enum SinkSchema {
    /// JSON encoding
    #[default]
    Json,
    /// Raw binary
    Raw,
}

/// PostgreSQL sink connector fixture.
///
/// Starts a PostgreSQL container and provides environment variables
/// for the sink connector to connect to it.
pub struct PostgresSinkFixture {
    container: PostgresContainer,
    payload_format: SinkPayloadFormat,
    schema: SinkSchema,
}

impl PostgresSinkFixture {
    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    /// Wait for a table to be created by the sink connector.
    pub async fn wait_for_table(&self, pool: &Pool<Postgres>, table: &str) {
        let query = format!("SELECT 1 FROM {table} LIMIT 1");
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            if sqlx::query(&query).fetch_optional(pool).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }
        panic!("Table {table} was not created in time");
    }

    /// Fetch rows from the sink table with polling until expected count is reached.
    pub async fn fetch_rows_as<T>(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        expected_count: usize,
    ) -> Vec<T>
    where
        T: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let mut rows = Vec::new();
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            if let Ok(fetched) = sqlx::query_as::<_, T>(query).fetch_all(pool).await {
                rows = fetched;
                if rows.len() >= expected_count {
                    break;
                }
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS / 5)).await;
        }
        rows
    }
}

#[async_trait]
impl TestFixture for PostgresSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self {
            container,
            payload_format: SinkPayloadFormat::default(),
            schema: SinkSchema::default(),
        })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();

        envs.insert(
            ENV_SINK_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(
            ENV_SINK_TARGET_TABLE.to_string(),
            DEFAULT_SINK_TABLE.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "test".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_sink".to_string(),
        );

        let schema_str = match self.schema {
            SinkSchema::Json => "json",
            SinkSchema::Raw => "raw",
        };
        envs.insert(
            ENV_SINK_STREAMS_0_SCHEMA.to_string(),
            schema_str.to_string(),
        );

        let format_str = match self.payload_format {
            SinkPayloadFormat::Bytea => "bytea",
            SinkPayloadFormat::Json => "json",
        };
        envs.insert(ENV_SINK_PAYLOAD_FORMAT.to_string(), format_str.to_string());

        envs
    }
}

/// PostgreSQL sink fixture for bytea payload format.
pub struct PostgresSinkByteaFixture {
    inner: PostgresSinkFixture,
}

#[async_trait]
impl TestFixture for PostgresSinkByteaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self {
            inner: PostgresSinkFixture {
                container,
                payload_format: SinkPayloadFormat::Bytea,
                schema: SinkSchema::Raw,
            },
        })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        self.inner.connector_envs()
    }
}

impl PostgresSinkByteaFixture {
    pub fn connection_string(&self) -> &str {
        self.inner.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.inner.create_pool().await
    }

    pub async fn wait_for_table(&self, pool: &Pool<Postgres>, table: &str) {
        self.inner.wait_for_table(pool, table).await
    }

    pub async fn fetch_rows_as<T>(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        expected_count: usize,
    ) -> Vec<T>
    where
        T: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        self.inner.fetch_rows_as(pool, query, expected_count).await
    }
}

/// PostgreSQL sink fixture for JSON payload format.
pub struct PostgresSinkJsonFixture {
    inner: PostgresSinkFixture,
}

#[async_trait]
impl TestFixture for PostgresSinkJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self {
            inner: PostgresSinkFixture {
                container,
                payload_format: SinkPayloadFormat::Json,
                schema: SinkSchema::Json,
            },
        })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        self.inner.connector_envs()
    }
}

impl PostgresSinkJsonFixture {
    pub fn connection_string(&self) -> &str {
        self.inner.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.inner.create_pool().await
    }

    pub async fn wait_for_table(&self, pool: &Pool<Postgres>, table: &str) {
        self.inner.wait_for_table(pool, table).await
    }

    pub async fn fetch_rows_as<T>(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        expected_count: usize,
    ) -> Vec<T>
    where
        T: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        self.inner.fetch_rows_as(pool, query, expected_count).await
    }
}

/// PostgreSQL source connector fixture.
///
/// Starts a PostgreSQL container and provides environment variables
/// for the source connector to connect to it.
pub struct PostgresSourceFixture {
    container: PostgresContainer,
    table_name: String,
    schema: SinkSchema,
}

impl PostgresSourceFixture {
    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Execute a query (e.g., CREATE TABLE).
    pub async fn execute(&self, pool: &Pool<Postgres>, query: &str) {
        sqlx::query(query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to execute query: {e}"));
    }

    /// Count rows in a table.
    pub async fn count_rows(&self, pool: &Pool<Postgres>, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) as count FROM {table}");
        let row: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        row.0
    }

    /// Count rows matching a condition.
    pub async fn count_rows_where(
        &self,
        pool: &Pool<Postgres>,
        table: &str,
        condition: &str,
    ) -> i64 {
        let query = format!("SELECT COUNT(*) as count FROM {table} WHERE {condition}");
        let row: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        row.0
    }
}

#[async_trait]
impl TestFixture for PostgresSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        let table_name = "test_messages".to_string();
        Ok(Self {
            container,
            table_name,
            schema: SinkSchema::Json,
        })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();

        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(
            ENV_SOURCE_TABLES.to_string(),
            format!("[{}]", self.table_name),
        );
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );

        let schema_str = match self.schema {
            SinkSchema::Json => "json",
            SinkSchema::Raw => "raw",
        };
        envs.insert(
            ENV_SOURCE_STREAMS_0_SCHEMA.to_string(),
            schema_str.to_string(),
        );

        envs
    }
}

/// PostgreSQL source fixture for JSON rows with metadata.
///
/// Creates a table with typed columns that get serialized as JSON with metadata.
pub struct PostgresSourceJsonFixture {
    container: PostgresContainer,
}

impl PostgresSourceJsonFixture {
    const TABLE: &'static str = "test_messages";

    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        Self::TABLE
    }

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                count INTEGER NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                active BOOLEAN NOT NULL,
                timestamp BIGINT NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_row(
        &self,
        pool: &Pool<Postgres>,
        id: i32,
        name: &str,
        count: i32,
        amount: f64,
        active: bool,
        timestamp: i64,
    ) {
        let query = format!(
            "INSERT INTO {} (id, name, count, amount, active, timestamp) VALUES ($1, $2, $3, $4, $5, $6)",
            Self::TABLE
        );
        sqlx::query(&query)
            .bind(id)
            .bind(name)
            .bind(count)
            .bind(amount)
            .bind(active)
            .bind(timestamp)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for PostgresSourceJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}

/// PostgreSQL source fixture for bytea payload column.
pub struct PostgresSourceByteaFixture {
    container: PostgresContainer,
}

impl PostgresSourceByteaFixture {
    const TABLE: &'static str = "test_payloads";

    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        Self::TABLE
    }

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                payload BYTEA NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_payload(&self, pool: &Pool<Postgres>, id: i32, payload: &[u8]) {
        let query = format!("INSERT INTO {} (id, payload) VALUES ($1, $2)", Self::TABLE);
        sqlx::query(&query)
            .bind(id)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert payload: {e}"));
    }
}

#[async_trait]
impl TestFixture for PostgresSourceByteaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_PAYLOAD_COLUMN.to_string(), "payload".to_string());
        envs.insert(ENV_SOURCE_PAYLOAD_FORMAT.to_string(), "bytea".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "raw".to_string());
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}

/// PostgreSQL source fixture for JSONB payload column.
pub struct PostgresSourceJsonbFixture {
    container: PostgresContainer,
}

impl PostgresSourceJsonbFixture {
    const TABLE: &'static str = "test_json_payloads";

    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        Self::TABLE
    }

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_json(&self, pool: &Pool<Postgres>, id: i32, data: &serde_json::Value) {
        let query = format!("INSERT INTO {} (id, data) VALUES ($1, $2)", Self::TABLE);
        sqlx::query(&query)
            .bind(id)
            .bind(data)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert json: {e}"));
    }
}

#[async_trait]
impl TestFixture for PostgresSourceJsonbFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_PAYLOAD_COLUMN.to_string(), "data".to_string());
        envs.insert(
            ENV_SOURCE_PAYLOAD_FORMAT.to_string(),
            "json_direct".to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}

/// PostgreSQL source fixture with delete_after_read enabled.
pub struct PostgresSourceDeleteFixture {
    container: PostgresContainer,
}

impl PostgresSourceDeleteFixture {
    const TABLE: &'static str = "test_delete_rows";

    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        Self::TABLE
    }

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INTEGER NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<Postgres>, name: &str, value: i32) {
        let query = format!("INSERT INTO {} (name, value) VALUES ($1, $2)", Self::TABLE);
        sqlx::query(&query)
            .bind(name)
            .bind(value)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    pub async fn count_rows(&self, pool: &Pool<Postgres>) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {}", Self::TABLE);
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }
}

#[async_trait]
impl TestFixture for PostgresSourceDeleteFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_DELETE_AFTER_READ.to_string(), "true".to_string());
        envs.insert(ENV_SOURCE_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}

/// PostgreSQL source fixture with processed_column marking.
pub struct PostgresSourceMarkFixture {
    container: PostgresContainer,
}

impl PostgresSourceMarkFixture {
    const TABLE: &'static str = "test_mark_rows";

    pub fn connection_string(&self) -> &str {
        self.container.connection_string()
    }

    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    pub fn table_name(&self) -> &str {
        Self::TABLE
    }

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INTEGER NOT NULL,
                is_processed BOOLEAN NOT NULL DEFAULT FALSE
            )",
            Self::TABLE
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<Postgres>, name: &str, value: i32) {
        let query = format!(
            "INSERT INTO {} (name, value, is_processed) VALUES ($1, $2, $3)",
            Self::TABLE
        );
        sqlx::query(&query)
            .bind(name)
            .bind(value)
            .bind(false)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    pub async fn count_rows(&self, pool: &Pool<Postgres>) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {}", Self::TABLE);
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }

    pub async fn count_unprocessed(&self, pool: &Pool<Postgres>) -> i64 {
        let query = format!(
            "SELECT COUNT(*) FROM {} WHERE is_processed = FALSE",
            Self::TABLE
        );
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }

    pub async fn count_processed(&self, pool: &Pool<Postgres>) -> i64 {
        let query = format!(
            "SELECT COUNT(*) FROM {} WHERE is_processed = TRUE",
            Self::TABLE
        );
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }
}

#[async_trait]
impl TestFixture for PostgresSourceMarkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connector_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "id".to_string());
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        envs.insert(
            ENV_SOURCE_PROCESSED_COLUMN.to_string(),
            "is_processed".to_string(),
        );
        envs.insert(ENV_SOURCE_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}
