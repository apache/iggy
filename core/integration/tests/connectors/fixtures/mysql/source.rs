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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_CONNECTION_STRING,
    ENV_SOURCE_DELETE_AFTER_READ, ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_PATH,
    ENV_SOURCE_PAYLOAD_COLUMN, ENV_SOURCE_PAYLOAD_FORMAT, ENV_SOURCE_PLUGIN_PATH,
    ENV_SOURCE_POLL_INTERVAL, ENV_SOURCE_PRIMARY_KEY_COLUMN, ENV_SOURCE_PROCESSED_COLUMN,
    ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC,
    ENV_SOURCE_TABLES, ENV_SOURCE_TRACKING_COLUMN, MySqlContainer, MySqlOps, MySqlSourceOps,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use sqlx::{MySql, Pool};
use std::collections::HashMap;

/// MySQL source fixture for JSON rows with metadata.
///
/// Creates a table with typed columns that get serialized as JSON with metadata.
/// The boolean column is declared `BOOLEAN` (i.e. `tinyint(1)`) so `sqlx-mysql`
/// reports it as `"BOOLEAN"` and the source emits JSON `true`/`false`, letting the
/// shared `TestMessage { active: bool }` deserialize unchanged.
pub struct MySqlSourceJsonFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceJsonFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceJsonFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceJsonFixture {
    const TABLE: &'static str = "test_messages";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                `count` INT NOT NULL,
                amount DOUBLE NOT NULL,
                active BOOLEAN NOT NULL,
                `timestamp` BIGINT NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_row(
        &self,
        pool: &Pool<MySql>,
        id: i32,
        name: &str,
        count: i32,
        amount: f64,
        active: bool,
        timestamp: i64,
    ) {
        let query = format!(
            "INSERT INTO `{}` (id, name, `count`, amount, active, `timestamp`) VALUES (?, ?, ?, ?, ?, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
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
impl TestFixture for MySqlSourceJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}

/// MySQL source fixture with `include_metadata = false`.
///
/// Same typed table as [`MySqlSourceJsonFixture`], but the connector is configured
/// to emit the bare column map with no `DatabaseRecord` envelope.
pub struct MySqlSourceNoMetadataFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceNoMetadataFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceNoMetadataFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceNoMetadataFixture {
    const TABLE: &'static str = "test_no_metadata";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                `count` INT NOT NULL,
                amount DOUBLE NOT NULL,
                active BOOLEAN NOT NULL,
                `timestamp` BIGINT NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_row(
        &self,
        pool: &Pool<MySql>,
        id: i32,
        name: &str,
        count: i32,
        amount: f64,
        active: bool,
        timestamp: i64,
    ) {
        let query = format!(
            "INSERT INTO `{}` (id, name, `count`, amount, active, `timestamp`) VALUES (?, ?, ?, ?, ?, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
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
impl TestFixture for MySqlSourceNoMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
        envs.insert(ENV_SOURCE_INCLUDE_METADATA.to_string(), "false".to_string());
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}

/// MySQL source fixture for a `BLOB` payload column.
pub struct MySqlSourceRawFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceRawFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceRawFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceRawFixture {
    const TABLE: &'static str = "test_payloads";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                payload BLOB NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_payload(&self, pool: &Pool<MySql>, id: i32, payload: &[u8]) {
        let query = format!("INSERT INTO `{}` (id, payload) VALUES (?, ?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert payload: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceRawFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}

/// MySQL source fixture for a native `JSON` payload column.
pub struct MySqlSourceJsonDirectFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceJsonDirectFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceJsonDirectFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceJsonDirectFixture {
    const TABLE: &'static str = "test_json_payloads";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data JSON NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_json(&self, pool: &Pool<MySql>, id: i32, data: &serde_json::Value) {
        let query = format!("INSERT INTO `{}` (id, data) VALUES (?, ?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .bind(data)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert json: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceJsonDirectFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}

/// MySQL source fixture with `delete_after_read` enabled.
pub struct MySqlSourceDeleteFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceDeleteFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceDeleteFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceDeleteFixture {
    const TABLE: &'static str = "test_delete_rows";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                `value` INT NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str, value: i32) {
        let query = format!(
            "INSERT INTO `{}` (name, `value`) VALUES (?, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .bind(value)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    pub async fn count_rows(&self, pool: &Pool<MySql>) -> i64 {
        MySqlSourceOps::count_rows(self, pool).await
    }
}

#[async_trait]
impl TestFixture for MySqlSourceDeleteFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}

/// MySQL source fixture with `processed_column` marking.
pub struct MySqlSourceMarkFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceMarkFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceMarkFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceMarkFixture {
    const TABLE: &'static str = "test_mark_rows";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                `value` INT NOT NULL,
                is_processed BOOLEAN NOT NULL DEFAULT FALSE
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str, value: i32) {
        let query = format!(
            "INSERT INTO `{}` (name, `value`, is_processed) VALUES (?, ?, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .bind(value)
            .bind(false)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    pub async fn count_rows(&self, pool: &Pool<MySql>) -> i64 {
        MySqlSourceOps::count_rows(self, pool).await
    }

    pub async fn count_unprocessed(&self, pool: &Pool<MySql>) -> i64 {
        let query = format!(
            "SELECT COUNT(*) FROM `{}` WHERE is_processed = FALSE",
            Self::TABLE
        );
        let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(query))
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }

    pub async fn count_processed(&self, pool: &Pool<MySql>) -> i64 {
        let query = format!(
            "SELECT COUNT(*) FROM `{}` WHERE is_processed = TRUE",
            Self::TABLE
        );
        let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(query))
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
        count.0
    }
}

#[async_trait]
impl TestFixture for MySqlSourceMarkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
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
            ENV_SOURCE_PLUGIN_PATH.to_string(),
        );
        envs
    }
}
