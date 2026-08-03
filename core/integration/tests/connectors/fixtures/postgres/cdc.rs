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
    ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_MODE, ENV_SOURCE_PATH, ENV_SOURCE_POLL_INTERVAL,
    ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC,
    ENV_SOURCE_TABLES, ENV_SOURCE_TRACKING_COLUMN, PostgresContainer, PostgresOps,
    PostgresSourceOps,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use sqlx::{Pool, Postgres};
use std::collections::HashMap;

/// PostgreSQL source fixture for CDC (WAL logical decoding) mode.
///
/// Starts the container with `wal_level = logical` so the connector's
/// `setup_cdc` can create a real `test_decoding` replication slot.
pub struct PostgresSourceCdcFixture {
    container: PostgresContainer,
}

impl PostgresOps for PostgresSourceCdcFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSourceOps for PostgresSourceCdcFixture {
    fn table_name(&self) -> &str {
        Self::TABLE
    }
}

impl PostgresSourceCdcFixture {
    const TABLE: &'static str = "cdc_events";
    const UNTRACKED_TABLE: &'static str = "cdc_events_untracked";

    pub async fn create_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                count INTEGER NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                active BOOLEAN NOT NULL,
                timestamp BIGINT NOT NULL,
                tag CHAR(10) NOT NULL
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
        pool: &Pool<Postgres>,
        id: i32,
        name: &str,
        count: i32,
        amount: f64,
        active: bool,
        timestamp: i64,
    ) {
        let tag = format!("{:<10}", format!("tag_{id}"));
        let query = format!(
            "INSERT INTO {} (id, name, count, amount, active, timestamp, tag) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .bind(name)
            .bind(count)
            .bind(amount)
            .bind(active)
            .bind(timestamp)
            .bind(&tag)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
    }

    pub async fn update_row(&self, pool: &Pool<Postgres>, id: i32, new_count: i32) {
        let query = format!("UPDATE {} SET count = $1 WHERE id = $2", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(new_count)
            .bind(id)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to update row: {e}"));
    }

    pub async fn update_primary_key(&self, pool: &Pool<Postgres>, old_id: i32, new_id: i32) {
        let query = format!("UPDATE {} SET id = $1 WHERE id = $2", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(new_id)
            .bind(old_id)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to update primary key: {e}"));
    }

    pub async fn delete_row(&self, pool: &Pool<Postgres>, id: i32) {
        let query = format!("DELETE FROM {} WHERE id = $1", Self::TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to delete row: {e}"));
    }

    pub async fn insert_row_rolled_back(&self, pool: &Pool<Postgres>, id: i32, name: &str) {
        let mut tx = pool
            .begin()
            .await
            .unwrap_or_else(|e| panic!("Failed to begin transaction: {e}"));
        let query = format!(
            "INSERT INTO {} (id, name, count, amount, active, timestamp, tag) \
             VALUES ($1, $2, 0, 0.0, true, 0, 'rollback  ')",
            Self::TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(id)
            .bind(name)
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert row inside transaction: {e}"));
        tx.rollback()
            .await
            .unwrap_or_else(|e| panic!("Failed to roll back transaction: {e}"));
    }

    pub async fn create_untracked_table(&self, pool: &Pool<Postgres>) {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)",
            Self::UNTRACKED_TABLE
        );
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create untracked table: {e}"));
    }

    pub async fn insert_untracked_row(&self, pool: &Pool<Postgres>, name: &str) {
        let query = format!("INSERT INTO {} (name) VALUES ($1)", Self::UNTRACKED_TABLE);
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(name)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert untracked row: {e}"));
    }
}

#[async_trait]
impl TestFixture for PostgresSourceCdcFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start_with_logical_replication().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_STRING.to_string(),
            self.container.connection_string.clone(),
        );
        envs.insert(ENV_SOURCE_MODE.to_string(), "cdc".to_string());
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
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "50ms".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_postgres_source".to_string(),
        );
        envs
    }
}
