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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_CONNECTION_STRING,
    ENV_SOURCE_DELETE_AFTER_READ, ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_PATH,
    ENV_SOURCE_PAYLOAD_COLUMN, ENV_SOURCE_PAYLOAD_FORMAT, ENV_SOURCE_POLL_INTERVAL,
    ENV_SOURCE_PRIMARY_KEY_COLUMN, ENV_SOURCE_PROCESSED_COLUMN, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TABLES,
    ENV_SOURCE_TRACKING_COLUMN, PostgresContainer, PostgresOps, PostgresSourceOps, SinkSchema,
};
use crate::harness::error::TestBinaryError;
use crate::harness::fixtures::TestFixture;
use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;

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

    pub async fn execute(&self, pool: &Pool<Postgres>, query: &str) {
        sqlx::query(query)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to execute query: {e}"));
    }

    pub async fn count_rows(&self, pool: &Pool<Postgres>, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) as count FROM {table}");
        let row: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        row.0
    }

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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl PostgresOps for PostgresSourceJsonFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceJsonFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceJsonFixture {
    const TABLE: &'static str = "test_messages";

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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl PostgresOps for PostgresSourceByteaFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceByteaFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceByteaFixture {
    const TABLE: &'static str = "test_payloads";

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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl PostgresOps for PostgresSourceJsonbFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceJsonbFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceJsonbFixture {
    const TABLE: &'static str = "test_json_payloads";

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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl PostgresOps for PostgresSourceDeleteFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceDeleteFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceDeleteFixture {
    const TABLE: &'static str = "test_delete_rows";

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
        PostgresSourceOps::count_rows(self, pool).await
    }
}

#[async_trait]
impl TestFixture for PostgresSourceDeleteFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl PostgresOps for PostgresSourceMarkFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceMarkFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceMarkFixture {
    const TABLE: &'static str = "test_mark_rows";

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
        PostgresSourceOps::count_rows(self, pool).await
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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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
