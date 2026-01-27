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
    DEFAULT_POLL_ATTEMPTS, DEFAULT_POLL_INTERVAL_MS, DEFAULT_SINK_TABLE, DEFAULT_TEST_STREAM,
    DEFAULT_TEST_TOPIC, ENV_SINK_CONNECTION_STRING, ENV_SINK_PATH, ENV_SINK_PAYLOAD_FORMAT,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TARGET_TABLE, PostgresContainer, PostgresOps,
    SinkPayloadFormat, SinkSchema,
};
use crate::harness::error::TestBinaryError;
use crate::harness::fixtures::TestFixture;
use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

/// PostgreSQL sink connector fixture.
///
/// Starts a PostgreSQL container and provides environment variables
/// for the sink connector to connect to it.
pub struct PostgresSinkFixture {
    container: PostgresContainer,
    payload_format: SinkPayloadFormat,
    schema: SinkSchema,
}

impl PostgresOps for PostgresSinkFixture {
    fn container(&self) -> &PostgresContainer {
        &self.container
    }
}

impl PostgresSinkFixture {
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
    ///
    /// Returns an error if the expected count is not reached within the poll attempts.
    pub async fn fetch_rows_as<T>(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        expected_count: usize,
    ) -> Result<Vec<T>, TestBinaryError>
    where
        T: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let mut rows = Vec::new();
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            if let Ok(fetched) = sqlx::query_as::<_, T>(query).fetch_all(pool).await {
                rows = fetched;
                if rows.len() >= expected_count {
                    return Ok(rows);
                }
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS / 5)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {} rows but got {} after {} poll attempts",
                expected_count,
                rows.len(),
                DEFAULT_POLL_ATTEMPTS
            ),
        })
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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
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

impl std::ops::Deref for PostgresSinkByteaFixture {
    type Target = PostgresSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// PostgreSQL sink fixture for JSON payload format.
pub struct PostgresSinkJsonFixture {
    inner: PostgresSinkFixture,
}

impl std::ops::Deref for PostgresSinkJsonFixture {
    type Target = PostgresSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
