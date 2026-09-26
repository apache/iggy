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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_CONNECTION_STRING, ENV_SOURCE_CUSTOM_QUERY,
    ENV_SOURCE_DELETE_AFTER_READ, ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_INITIAL_OFFSET,
    ENV_SOURCE_PATH, ENV_SOURCE_PAYLOAD_COLUMN, ENV_SOURCE_PAYLOAD_FORMAT, ENV_SOURCE_PLUGIN_PATH,
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

/// MySQL source fixture whose `payload_column` names a column the table does not
/// have, so the source must fail the table instead of falling back to whole-row
/// JSON that would contradict the `raw` schema the stream is configured with.
pub struct MySqlSourceMissingPayloadColumnFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceMissingPayloadColumnFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceMissingPayloadColumnFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceMissingPayloadColumnFixture {
    const TABLE: &'static str = "test_missing_payload";
    const MISSPELLED_PAYLOAD_COLUMN: &'static str = "paylod";

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
impl TestFixture for MySqlSourceMissingPayloadColumnFixture {
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
        envs.insert(
            ENV_SOURCE_PAYLOAD_COLUMN.to_string(),
            Self::MISSPELLED_PAYLOAD_COLUMN.to_string(),
        );
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

/// MySQL source fixture tracking a `VARCHAR` column that holds digit-like values.
///
/// MySQL orders this column by collation, so `'100'` sorts before `'2'`. If the
/// offset is written into the query as a bare number, MySQL converts the column and
/// the literal to a double and the filter compares numerically while `ORDER BY`
/// compares by collation. The cursor then climbs to the numeric maximum and every
/// value below it becomes unreachable, however far its collation order says it
/// should still be read.
pub struct MySqlSourceTextTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceTextTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceTextTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceTextTrackingFixture {
    const TABLE: &'static str = "test_text_tracking";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                code VARCHAR(8) NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, code: &str) {
        let query = format!("INSERT INTO `{}` (code, name) VALUES (?, ?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(code)
            .bind(format!("row_{code}"))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceTextTrackingFixture {
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
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "code".to_string());
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

/// MySQL source fixture whose `custom_query` orders rows descending by a computed
/// column that does not exist on the table.
///
/// `information_schema` has no row for `tracking_id`, so the comparison order can
/// only come from the type MySQL reports for it in the result set. Without that,
/// descending values like 100, 99 read as a decrease numerically and an increase by
/// collation, the two disagree, and the batch is let through - skipping every row
/// below the top of the first batch, permanently.
pub struct MySqlSourceComputedTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceComputedTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceComputedTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceComputedTrackingFixture {
    const TABLE: &'static str = "test_computed_tracking";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    /// Takes an explicit id because which ids are used is the point of the test.
    pub async fn insert_row(&self, pool: &Pool<MySql>, id: i32, name: &str) {
        let query = format!("INSERT INTO `{}` (id, name) VALUES (?, ?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceComputedTrackingFixture {
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
        envs.insert(
            ENV_SOURCE_TRACKING_COLUMN.to_string(),
            "tracking_id".to_string(),
        );
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT *, id AS tracking_id FROM $table WHERE id > $offset \
             ORDER BY tracking_id DESC LIMIT $limit"
                .to_string(),
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

/// MySQL source fixture whose `custom_query` orders rows descending.
///
/// The rows are correctly readable, but the batch's tracking values decrease, so
/// the cursor would move backwards and skip rows. Exercises the ordering guard:
/// the connector must publish nothing and disable the table rather than advance
/// its offset.
pub struct MySqlSourceDescendingQueryFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceDescendingQueryFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceDescendingQueryFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceDescendingQueryFixture {
    const TABLE: &'static str = "test_descending_rows";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!("INSERT INTO `{}` (name) VALUES (?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceDescendingQueryFixture {
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
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT * FROM $table WHERE id > $offset ORDER BY id DESC LIMIT $limit".to_string(),
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

/// MySQL source fixture whose `custom_query` aliases the tracking column away.
///
/// `SELECT id AS row_id` returns a result set with no `id` in it, so no row can yield
/// a cursor. The connector would otherwise publish every row and leave the offset
/// unset, replaying the same result set on every poll forever. The column exists on
/// the table, so the startup probe passes and only the result set can catch this.
pub struct MySqlSourceAliasedTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceAliasedTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceAliasedTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceAliasedTrackingFixture {
    const TABLE: &'static str = "test_aliased_tracking";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!("INSERT INTO `{}` (name) VALUES (?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceAliasedTrackingFixture {
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
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT id AS row_id, name FROM $table WHERE id > $offset \
             ORDER BY row_id LIMIT $limit"
                .to_string(),
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

/// MySQL source fixture whose tracking column is projected but NULL.
///
/// A NULL yields no cursor, so publishing the row would advance nothing and the same
/// rows would be re-read forever. Unlike a missing column this is the row's own value,
/// which an `UPDATE` can fix, so the table must stay enabled and recover on its own
/// once it is. The column is nullable and a `custom_query` is set, which is exactly
/// the combination the startup probe cannot decide: a query can filter NULLs out of a
/// nullable column just as easily as a join can put them into a `NOT NULL` one.
pub struct MySqlSourceNullTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceNullTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceNullTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceNullTrackingFixture {
    const TABLE: &'static str = "test_null_tracking";
    /// Any fixed point in the past; `+ id` spreads the repaired rows out in `id`
    /// order so the batch that follows the repair is ascending.
    const BACKFILL_EPOCH: i64 = 1_700_000_000;
    /// Below every repaired value, and non-numeric, which the tracking column being a
    /// `DATETIME` makes natural: the config env provider types a bare `0` as a number
    /// and then fails to deserialize it into the string field `initial_offset`.
    const INITIAL_OFFSET: &'static str = "2000-01-01 00:00:00";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                updated_at DATETIME NULL,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    /// Inserts a row whose tracking column is NULL.
    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!(
            "INSERT INTO `{}` (updated_at, name) VALUES (NULL, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    /// Gives every row a usable tracking value, as an operator repairing the data
    /// would. Distinct and ascending by `id`, so the repaired batch is ordered. The
    /// table was never disabled, so the next poll must pick every row up.
    pub async fn backfill_tracking_values(&self, pool: &Pool<MySql>) {
        let query = format!(
            "UPDATE `{}` SET updated_at = FROM_UNIXTIME({} + id) WHERE updated_at IS NULL",
            Self::TABLE,
            Self::BACKFILL_EPOCH
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to backfill tracking values: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceNullTrackingFixture {
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
        envs.insert(
            ENV_SOURCE_TRACKING_COLUMN.to_string(),
            "updated_at".to_string(),
        );
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        envs.insert(
            ENV_SOURCE_INITIAL_OFFSET.to_string(),
            Self::INITIAL_OFFSET.to_string(),
        );
        // `IS NULL` is what puts the unusable rows in the result set at all: a NULL
        // fails every comparison, so the $offset filter alone would hide them and the
        // connector would have nothing to reject.
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT * FROM $table WHERE (updated_at > $offset OR updated_at IS NULL) \
             ORDER BY updated_at LIMIT $limit"
                .to_string(),
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

/// MySQL source fixture whose tracking column is a `TIMESTAMP`.
///
/// `TIMESTAMP` is the one temporal type sqlx decodes into `DateTime<Utc>`, whose
/// display appends a ` UTC` no other column type carries. `DATETIME` decodes into
/// `NaiveDateTime` and renders without a suffix, which is why every other temporal
/// fixture here misses this path: the two are one keyword apart.
///
/// MySQL prefix-parses that suffix in a `SELECT`, so resuming works whichever way
/// the cursor is rendered and this fixture cannot discriminate that fix. It covers
/// the payload rendering, which must not move when the cursor rendering does, and
/// `TIMESTAMP` tracking working across a cursor boundary at all. The rendering that
/// only a `DELETE` can tell apart is covered by
/// `MySqlSourceTimestampDeleteFixture`.
pub struct MySqlSourceTimestampTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceTimestampTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceTimestampTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceTimestampTrackingFixture {
    const TABLE: &'static str = "test_timestamp_tracking";
    /// Any fixed point inside the `TIMESTAMP` range. Rows take `EPOCH + sequence`
    /// so their tracking values are distinct and ascending without depending on
    /// insert timing, which whole-second resolution would otherwise collide on.
    const EPOCH: i64 = 1_700_000_000;
    /// Below every row and non-numeric, which the config env provider requires: it
    /// types a bare number and then fails to deserialize it into the string field.
    const INITIAL_OFFSET: &'static str = "2000-01-01 00:00:00";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        // NOT NULL is explicit because `explicit_defaults_for_timestamp` (ON since
        // MySQL 8.0) makes a bare TIMESTAMP nullable, which the built-query path
        // refuses. No ON UPDATE clause either: a value that moves under the cursor
        // is the mutable-column case the connector documents as unsupported.
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                updated_at TIMESTAMP(6) NOT NULL,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, sequence: i64, name: &str) {
        let query = format!(
            "INSERT INTO `{}` (updated_at, name) VALUES (FROM_UNIXTIME(?), ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(Self::EPOCH + sequence)
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceTimestampTrackingFixture {
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
        envs.insert(
            ENV_SOURCE_TRACKING_COLUMN.to_string(),
            "updated_at".to_string(),
        );
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        envs.insert(
            ENV_SOURCE_INITIAL_OFFSET.to_string(),
            Self::INITIAL_OFFSET.to_string(),
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

/// MySQL source fixture that deletes rows keyed by a `TIMESTAMP` tracking column.
///
/// `primary_key_column` is deliberately unset so it defaults to `tracking_column`,
/// which is the configuration that sends a `TIMESTAMP` value into the mark/delete
/// `WHERE pk IN (...)`. That predicate runs inside DML, where the default
/// `STRICT_TRANS_TABLES` turns a datetime conversion that a `SELECT` only warns
/// about into a hard error, so a value rendered with a zone suffix fails the
/// statement outright and the batch is never published.
pub struct MySqlSourceTimestampDeleteFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceTimestampDeleteFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceTimestampDeleteFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceTimestampDeleteFixture {
    const TABLE: &'static str = "test_timestamp_delete";
    const EPOCH: i64 = 1_700_000_000;
    const INITIAL_OFFSET: &'static str = "2000-01-01 00:00:00";

    pub async fn create_table(&self, pool: &Pool<MySql>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                updated_at TIMESTAMP(6) NOT NULL,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table: {e}"));
    }

    pub async fn insert_row(&self, pool: &Pool<MySql>, sequence: i64, name: &str) {
        let query = format!(
            "INSERT INTO `{}` (updated_at, name) VALUES (FROM_UNIXTIME(?), ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(Self::EPOCH + sequence)
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceTimestampDeleteFixture {
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
        envs.insert(
            ENV_SOURCE_TRACKING_COLUMN.to_string(),
            "updated_at".to_string(),
        );
        envs.insert(
            ENV_SOURCE_INITIAL_OFFSET.to_string(),
            Self::INITIAL_OFFSET.to_string(),
        );
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

/// MySQL source fixture whose tracking column is a `JSON` column, with a
/// `custom_query` set.
///
/// A `JSON` column has no ordered scalar form, so it can never yield a cursor. The
/// startup probe rejects the type whether or not a `custom_query` is set, because a
/// projection cannot change it, and the connector must refuse to start rather than
/// poll a table it can never advance.
///
/// Unlike every other fixture here, the table is created during `setup()`: the
/// harness starts the connectors runtime before the test body runs, and a table that
/// does not exist yet defers validation instead of failing it.
pub struct MySqlSourceJsonTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceJsonTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceJsonTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceJsonTrackingFixture {
    const TABLE: &'static str = "test_json_tracking";

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!(
            "INSERT INTO `{}` (doc, name) VALUES (JSON_OBJECT('name', ?), ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceJsonTrackingFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
        let pool = container.create_pool().await?;
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                doc JSON NOT NULL,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(&pool)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlSourceJsonTrackingFixture".to_string(),
                message: format!("Failed to create table: {e}"),
            })?;
        pool.close().await;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "doc".to_string());
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit".to_string(),
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

/// MySQL source fixture whose `tracking_column` differs in case from the column the
/// table declares.
///
/// MySQL matches column identifiers case-insensitively and so does the startup
/// probe, so `tracking_column = "ID"` against a column declared `id` starts up. The
/// result set then carries `id`, and the two sites this fixture reaches - the
/// projection check and the row loop that reads the tracking value - each leave the
/// table publishing nothing if they compare exactly.
///
/// The table is created during `setup()`, before the runtime starts, so the probe
/// runs against the real column instead of deferring. That also means the kind is
/// already resolved when the first query is built, which is why the result-set kind
/// lookup is out of reach here: matching exactly there would fall back to the
/// resolved kind and change nothing.
pub struct MySqlSourceCaseMismatchedTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceCaseMismatchedTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceCaseMismatchedTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceCaseMismatchedTrackingFixture {
    const TABLE: &'static str = "test_case_mismatched_tracking";

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!("INSERT INTO `{}` (name) VALUES (?)", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceCaseMismatchedTrackingFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
        let pool = container.create_pool().await?;
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(&pool)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlSourceCaseMismatchedTrackingFixture".to_string(),
                message: format!("Failed to create table: {e}"),
            })?;
        pool.close().await;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "ID".to_string());
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

/// MySQL source fixture whose `custom_query` aliases a `tinyint(1)` column onto the
/// tracking column name.
///
/// The startup gate judges the column the table declares, so a `tinyint` named
/// directly is refused there. Under an alias the table has no such column at all,
/// the probe defers to the result set, and the value reaches the row loop: sqlx
/// names a width-1 tinyint `BOOLEAN` and decodes it to `true`/`false`, which has no
/// ordered scalar form and cannot be a cursor. That is caught per row rather than at
/// startup, which is the whole of what this fixture pins.
pub struct MySqlSourceTinyintTrackingFixture {
    container: MySqlContainer,
}

impl MySqlOps for MySqlSourceTinyintTrackingFixture {
    fn container(&self) -> &MySqlContainer {
        &self.container
    }
}

impl MySqlSourceOps for MySqlSourceTinyintTrackingFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl MySqlSourceTinyintTrackingFixture {
    const TABLE: &'static str = "test_tinyint_tracking";

    pub async fn insert_row(&self, pool: &Pool<MySql>, name: &str) {
        let query = format!(
            "INSERT INTO `{}` (is_active, name) VALUES (TRUE, ?)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }
}

#[async_trait]
impl TestFixture for MySqlSourceTinyintTrackingFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MySqlContainer::start().await?;
        let pool = container.create_pool().await?;
        // Created before the runtime starts so the probe takes the "the table has no
        // such column, assume custom_query projects it" branch rather than the one
        // for a table that does not exist yet.
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                is_active BOOLEAN NOT NULL,
                name VARCHAR(255) NOT NULL
            )",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(&pool)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlSourceTinyintTrackingFixture".to_string(),
                message: format!("Failed to create table: {e}"),
            })?;
        pool.close().await;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
        envs.insert(ENV_SOURCE_TRACKING_COLUMN.to_string(), "flag".to_string());
        envs.insert(ENV_SOURCE_PRIMARY_KEY_COLUMN.to_string(), "id".to_string());
        // Ordered and filtered by `id`, so the only thing wrong with the batch is the
        // value the cursor would be taken from.
        envs.insert(
            ENV_SOURCE_CUSTOM_QUERY.to_string(),
            "SELECT *, is_active AS flag FROM $table WHERE id > $offset ORDER BY id LIMIT $limit"
                .to_string(),
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
