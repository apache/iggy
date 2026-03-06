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
    CLICKHOUSE_TEST_PASSWORD, CLICKHOUSE_TEST_USER, CREATE_TABLE_SQL, ClickHouseContainer,
    ClickHouseOps, DEFAULT_DATABASE, DEFAULT_SINK_TABLE, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC,
    ENV_SINK_DATABASE, ENV_SINK_INSERT_FORMAT, ENV_SINK_PASSWORD, ENV_SINK_PATH,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_STRING_FORMAT, ENV_SINK_TABLE, ENV_SINK_URL,
    ENV_SINK_USERNAME, HEALTH_CHECK_ATTEMPTS, HEALTH_CHECK_INTERVAL_MS, create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

// ── shared helper ────────────────────────────────────────────────────────────

async fn start_and_init_container(
    table_sql: &str,
    client: &HttpClient,
) -> Result<ClickHouseContainer, TestBinaryError> {
    let container = ClickHouseContainer::start().await?;

    // Wait until ClickHouse accepts HTTP requests.
    let ping_url = format!("{}/ping", container.base_url);
    let mut ready = false;
    for _ in 0..HEALTH_CHECK_ATTEMPTS {
        if let Ok(resp) = client.get(&ping_url).send().await 
            && resp.status().is_success() {
            info!("ClickHouse HTTP interface is ready");
            ready = true;
            break;
        }
        sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS)).await;
    }

    if !ready {
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "ClickHouseSink".to_string(),
            message: format!(
                "ClickHouse did not become healthy after {} attempts",
                HEALTH_CHECK_ATTEMPTS
            ),
        });
    }

    // Create the test table.
    let response = client
        .post(&container.base_url)
        .body(table_sql.to_string())
        .send()
        .await
        .map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "ClickHouseSink".to_string(),
            message: format!("Failed to create test table: {e}"),
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "ClickHouseSink".to_string(),
            message: format!("CREATE TABLE failed: status={status}, body={body}"),
        });
    }

    info!("Created ClickHouse test table");
    Ok(container)
}

fn base_envs(container: &ClickHouseContainer, schema: &str) -> HashMap<String, String> {
    let mut envs = HashMap::new();
    envs.insert(ENV_SINK_URL.to_string(), container.base_url.clone());
    envs.insert(ENV_SINK_DATABASE.to_string(), DEFAULT_DATABASE.to_string());
    envs.insert(ENV_SINK_TABLE.to_string(), DEFAULT_SINK_TABLE.to_string());
    envs.insert(
        ENV_SINK_USERNAME.to_string(),
        CLICKHOUSE_TEST_USER.to_string(),
    );
    envs.insert(
        ENV_SINK_PASSWORD.to_string(),
        CLICKHOUSE_TEST_PASSWORD.to_string(),
    );
    envs.insert(
        ENV_SINK_STREAMS_0_STREAM.to_string(),
        DEFAULT_TEST_STREAM.to_string(),
    );
    envs.insert(
        ENV_SINK_STREAMS_0_TOPICS.to_string(),
        format!("[{}]", DEFAULT_TEST_TOPIC),
    );
    envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), schema.to_string());
    envs.insert(
        ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
        "clickhouse_sink_cg".to_string(),
    );
    envs.insert(
        ENV_SINK_PATH.to_string(),
        "../../target/debug/libiggy_connector_clickhouse_sink".to_string(),
    );
    envs
}

// ── JSONEachRow fixture ───────────────────────────────────────────────────────

/// ClickHouse sink fixture using the default `json_each_row` insert format.
pub struct ClickHouseSinkFixture {
    pub container: ClickHouseContainer,
    pub http_client: HttpClient,
}

impl ClickHouseOps for ClickHouseSinkFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl ClickHouseSinkFixture {
    pub async fn wait_for_rows(&self, expected: usize) -> Result<usize, TestBinaryError> {
        ClickHouseOps::wait_for_rows(self, DEFAULT_SINK_TABLE, expected).await
    }

    pub async fn fetch_rows(&self) -> Result<Vec<serde_json::Value>, TestBinaryError> {
        ClickHouseOps::fetch_rows_as_json(self, DEFAULT_SINK_TABLE).await
    }

    pub async fn count_rows(&self) -> Result<usize, TestBinaryError> {
        ClickHouseOps::count_rows(self, DEFAULT_SINK_TABLE).await
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let http_client = create_http_client();
        let container = start_and_init_container(CREATE_TABLE_SQL, &http_client).await?;
        Ok(Self {
            container,
            http_client,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        // insert_format defaults to json_each_row — no env var needed.
        base_envs(&self.container, "json")
    }
}

// ── RowBinary fixture ─────────────────────────────────────────────────────────

/// ClickHouse sink fixture using `row_binary` insert format.
///
/// The connector fetches the table schema at startup and serialises each JSON
/// message to ClickHouse's RowBinaryWithDefaults wire format.
pub struct ClickHouseSinkRowBinaryFixture {
    inner: ClickHouseSinkFixture,
}

impl std::ops::Deref for ClickHouseSinkRowBinaryFixture {
    type Target = ClickHouseSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkRowBinaryFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let http_client = create_http_client();
        let container = start_and_init_container(CREATE_TABLE_SQL, &http_client).await?;
        Ok(Self {
            inner: ClickHouseSinkFixture {
                container,
                http_client,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = base_envs(&self.inner.container, "json");
        envs.insert(ENV_SINK_INSERT_FORMAT.to_string(), "row_binary".to_string());
        envs
    }
}

// ── String (CSV) passthrough fixture ─────────────────────────────────────────

/// ClickHouse sink fixture using `string` insert format with CSV sub-format.
///
/// Payloads are raw CSV bytes forwarded directly to ClickHouse without any
/// parsing by the connector.
pub struct ClickHouseSinkStringFixture {
    inner: ClickHouseSinkFixture,
}

impl std::ops::Deref for ClickHouseSinkStringFixture {
    type Target = ClickHouseSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkStringFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let http_client = create_http_client();
        let container = start_and_init_container(CREATE_TABLE_SQL, &http_client).await?;
        Ok(Self {
            inner: ClickHouseSinkFixture {
                container,
                http_client,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = base_envs(&self.inner.container, "text");
        envs.insert(ENV_SINK_INSERT_FORMAT.to_string(), "string".to_string());
        envs.insert(ENV_SINK_STRING_FORMAT.to_string(), "csv".to_string());
        envs
    }
}
