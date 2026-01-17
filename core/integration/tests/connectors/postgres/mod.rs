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

mod postgres_sink;
mod postgres_source;

use crate::connectors::{
    ConnectorsRuntime, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, IggySetup, TestMessage,
    setup_runtime,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::{
    postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use tokio::time::sleep;

const SOURCE_TABLE: &str = "test_messages";
const SOURCE_TABLE_BYTEA: &str = "test_payloads";
const SOURCE_TABLE_JSON: &str = "test_json_payloads";
const SOURCE_TABLE_DELETE: &str = "test_delete_rows";
const SOURCE_TABLE_MARK: &str = "test_mark_rows";
const SINK_TABLE: &str = "iggy_messages";
const TEST_STREAM: &str = DEFAULT_TEST_STREAM;
const TEST_TOPIC: &str = DEFAULT_TEST_TOPIC;
const TEST_MESSAGE_COUNT: usize = 3;
const POSTGRES_PORT: u16 = 5432;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const FETCH_INTERVAL_MS: u64 = 10;
const TABLE_WAIT_INTERVAL_MS: u64 = 50;
const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";
const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";

type SinkRow = (i64, String, String, Vec<u8>);
type SinkJsonRow = (i64, serde_json::Value);

#[derive(Debug, Deserialize)]
struct DatabaseRecord {
    table_name: String,
    operation_type: String,
    data: TestMessage,
}

struct PostgresTestSetup {
    runtime: ConnectorsRuntime,
    connection_string: String,
    #[allow(dead_code)]
    container: ContainerAsync<postgres::Postgres>,
}

async fn start_container() -> (ContainerAsync<postgres::Postgres>, String) {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("Failed to start Postgres container");
    let host_port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Failed to get Postgres port");
    let connection_string = format!("postgres://postgres:postgres@localhost:{host_port}");
    (container, connection_string)
}

async fn create_pool(connection_string: &str) -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(connection_string)
        .await
        .expect("Failed to connect to PostgreSQL")
}

async fn setup_sink() -> PostgresTestSetup {
    setup_sink_with_config("postgres/sink.toml").await
}

async fn setup_sink_bytea() -> PostgresTestSetup {
    setup_sink_with_config("postgres/sink_raw.toml").await
}

async fn setup_sink_json() -> PostgresTestSetup {
    setup_sink_with_config("postgres/sink_json.toml").await
}

async fn setup_sink_with_config(config_path: &str) -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SINK_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init(config_path, Some(envs), IggySetup::default())
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

async fn setup_source() -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;
    let pool = create_pool(&connection_string).await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            count INTEGER NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            active BOOLEAN NOT NULL,
            timestamp BIGINT NOT NULL
        )"
    ))
    .execute(&pool)
    .await
    .expect("Failed to create source table");
    pool.close().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SOURCE_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init("postgres/source.toml", Some(envs), IggySetup::default())
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

async fn setup_source_bytea() -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;
    let pool = create_pool(&connection_string).await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE_BYTEA} (
            id SERIAL PRIMARY KEY,
            payload BYTEA NOT NULL
        )"
    ))
    .execute(&pool)
    .await
    .expect("Failed to create bytea source table");
    pool.close().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SOURCE_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init(
            "postgres/source_bytea.toml",
            Some(envs),
            IggySetup::default(),
        )
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

async fn setup_source_json() -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;
    let pool = create_pool(&connection_string).await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE_JSON} (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )"
    ))
    .execute(&pool)
    .await
    .expect("Failed to create json source table");
    pool.close().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SOURCE_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init(
            "postgres/source_json.toml",
            Some(envs),
            IggySetup::default(),
        )
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

async fn setup_source_delete() -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;
    let pool = create_pool(&connection_string).await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE_DELETE} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INTEGER NOT NULL
        )"
    ))
    .execute(&pool)
    .await
    .expect("Failed to create delete source table");
    pool.close().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SOURCE_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init(
            "postgres/source_delete.toml",
            Some(envs),
            IggySetup::default(),
        )
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

async fn setup_source_mark() -> PostgresTestSetup {
    let (container, connection_string) = start_container().await;
    let pool = create_pool(&connection_string).await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE_MARK} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INTEGER NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT FALSE
        )"
    ))
    .execute(&pool)
    .await
    .expect("Failed to create mark source table");
    pool.close().await;

    let mut envs = HashMap::new();
    envs.insert(
        ENV_SOURCE_CONNECTION_STRING.to_owned(),
        connection_string.clone(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init(
            "postgres/source_mark.toml",
            Some(envs),
            IggySetup::default(),
        )
        .await;

    PostgresTestSetup {
        runtime,
        connection_string,
        container,
    }
}

impl PostgresTestSetup {
    async fn create_pool(&self) -> Pool<Postgres> {
        create_pool(&self.connection_string).await
    }

    async fn wait_for_table(&self, pool: &Pool<Postgres>, table: &str) {
        let query = format!("SELECT 1 FROM {table} LIMIT 1");
        for _ in 0..POLL_ATTEMPTS {
            if sqlx::query(&query).fetch_optional(pool).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(TABLE_WAIT_INTERVAL_MS)).await;
        }
        panic!("Table {table} was not created in time");
    }

    async fn insert_test_messages(&self, pool: &Pool<Postgres>, messages: &[TestMessage]) {
        let query = format!(
            "INSERT INTO {SOURCE_TABLE} (id, name, count, amount, active, timestamp) \
             VALUES ($1, $2, $3, $4, $5, $6)"
        );
        for msg in messages {
            sqlx::query(&query)
                .bind(msg.id as i32)
                .bind(&msg.name)
                .bind(msg.count as i32)
                .bind(msg.amount)
                .bind(msg.active)
                .bind(msg.timestamp)
                .execute(pool)
                .await
                .expect("Failed to insert test message");
        }
    }

    async fn insert_bytea_payloads(&self, pool: &Pool<Postgres>, payloads: &[Vec<u8>]) {
        let query = format!("INSERT INTO {SOURCE_TABLE_BYTEA} (id, payload) VALUES ($1, $2)");
        for (i, payload) in payloads.iter().enumerate() {
            sqlx::query(&query)
                .bind((i + 1) as i32)
                .bind(payload)
                .execute(pool)
                .await
                .expect("Failed to insert bytea payload");
        }
    }

    async fn insert_json_payloads(&self, pool: &Pool<Postgres>, payloads: &[serde_json::Value]) {
        let query = format!("INSERT INTO {SOURCE_TABLE_JSON} (id, data) VALUES ($1, $2)");
        for (i, payload) in payloads.iter().enumerate() {
            sqlx::query(&query)
                .bind((i + 1) as i32)
                .bind(payload)
                .execute(pool)
                .await
                .expect("Failed to insert json payload");
        }
    }

    async fn insert_delete_rows(&self, pool: &Pool<Postgres>, count: usize) {
        let query = format!("INSERT INTO {SOURCE_TABLE_DELETE} (name, value) VALUES ($1, $2)");
        for i in 0..count {
            sqlx::query(&query)
                .bind(format!("row_{i}"))
                .bind(i as i32 * 10)
                .execute(pool)
                .await
                .expect("Failed to insert delete row");
        }
    }

    async fn insert_mark_rows(&self, pool: &Pool<Postgres>, count: usize) {
        let query = format!(
            "INSERT INTO {SOURCE_TABLE_MARK} (name, value, is_processed) VALUES ($1, $2, $3)"
        );
        for i in 0..count {
            sqlx::query(&query)
                .bind(format!("row_{i}"))
                .bind(i as i32 * 10)
                .bind(false)
                .execute(pool)
                .await
                .expect("Failed to insert mark row");
        }
    }

    async fn count_rows(&self, pool: &Pool<Postgres>, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {table}");
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .expect("Failed to count rows");
        count.0
    }

    async fn count_unprocessed_rows(&self, pool: &Pool<Postgres>, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {table} WHERE is_processed = FALSE");
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .expect("Failed to count unprocessed rows");
        count.0
    }

    async fn count_processed_rows(&self, pool: &Pool<Postgres>, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {table} WHERE is_processed = TRUE");
        let count: (i64,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await
            .expect("Failed to count processed rows");
        count.0
    }

    async fn fetch_sink_rows(&self, pool: &Pool<Postgres>, expected_count: usize) -> Vec<SinkRow> {
        let query = format!(
            "SELECT iggy_offset, iggy_stream, iggy_topic, payload \
             FROM {SINK_TABLE} ORDER BY iggy_offset"
        );
        let mut rows = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(fetched) = sqlx::query_as::<_, SinkRow>(&query).fetch_all(pool).await {
                rows = fetched;
                if rows.len() >= expected_count {
                    break;
                }
            }
            sleep(Duration::from_millis(FETCH_INTERVAL_MS)).await;
        }
        rows
    }

    async fn fetch_sink_json_rows(
        &self,
        pool: &Pool<Postgres>,
        expected_count: usize,
    ) -> Vec<SinkJsonRow> {
        let query = format!("SELECT iggy_offset, payload FROM {SINK_TABLE} ORDER BY iggy_offset");
        let mut rows = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(fetched) = sqlx::query_as::<_, SinkJsonRow>(&query)
                .fetch_all(pool)
                .await
            {
                rows = fetched;
                if rows.len() >= expected_count {
                    break;
                }
            }
            sleep(Duration::from_millis(FETCH_INTERVAL_MS)).await;
        }
        rows
    }

    async fn poll_messages<T, F>(&self, expected_count: usize, transform: F) -> Vec<T>
    where
        F: Fn(&[u8]) -> Option<T>,
    {
        let client = self.runtime.create_client().await;
        let mut received = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(polled) = client.poll_messages().await {
                for msg in polled.messages {
                    if let Some(item) = transform(&msg.payload) {
                        received.push(item);
                    }
                }
                if received.len() >= expected_count {
                    break;
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        received
    }
}
