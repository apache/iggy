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
    CLICKHOUSE_DEFAULT_PASSWORD, CLICKHOUSE_DEFAULT_USER, ClickHouseContainer, ClickHouseOps,
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_AUTH_TYPE, ENV_SINK_COMPRESSION,
    ENV_SINK_INCLUDE_METADATA, ENV_SINK_INSERT_TYPE, ENV_SINK_PASSWORD, ENV_SINK_PATH,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TABLE, ENV_SINK_URL, ENV_SINK_USERNAME,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;

/// Builds the common env vars shared by all ClickHouse sink fixtures.
fn base_envs(
    url: &str,
    table: &str,
    insert_type: &str,
    include_metadata: bool,
) -> HashMap<String, String> {
    let mut envs = HashMap::new();
    envs.insert(ENV_SINK_URL.to_string(), url.to_string());
    envs.insert(ENV_SINK_TABLE.to_string(), table.to_string());
    envs.insert(ENV_SINK_INSERT_TYPE.to_string(), insert_type.to_string());
    envs.insert(
        ENV_SINK_INCLUDE_METADATA.to_string(),
        include_metadata.to_string(),
    );
    envs.insert(ENV_SINK_COMPRESSION.to_string(), "false".to_string());
    envs.insert(ENV_SINK_AUTH_TYPE.to_string(), "credential".to_string());
    envs.insert(
        ENV_SINK_USERNAME.to_string(),
        CLICKHOUSE_DEFAULT_USER.to_string(),
    );
    envs.insert(
        ENV_SINK_PASSWORD.to_string(),
        CLICKHOUSE_DEFAULT_PASSWORD.to_string(),
    );
    envs.insert(
        ENV_SINK_STREAMS_0_STREAM.to_string(),
        DEFAULT_TEST_STREAM.to_string(),
    );
    envs.insert(
        ENV_SINK_STREAMS_0_TOPICS.to_string(),
        format!("[{}]", DEFAULT_TEST_TOPIC),
    );
    envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
    envs.insert(
        ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
        "test".to_string(),
    );
    envs.insert(
        ENV_SINK_PATH.to_string(),
        "../../target/debug/libiggy_connector_clickhouse_sink".to_string(),
    );
    envs
}

// ── Table DDLs ───────────────────────────────────────────────────────────────

const DDL_JSON: &str = "\
    CREATE TABLE IF NOT EXISTS iggy_sink (\
        id        UInt64,\
        name      String,\
        count     UInt32,\
        amount    Float64,\
        active    Bool,\
        timestamp Int64\
    ) ENGINE = MergeTree() ORDER BY id";

const DDL_JSON_META: &str = "\
    CREATE TABLE IF NOT EXISTS iggy_sink_meta (\
        id                     UInt64,\
        name                   String,\
        count                  UInt32,\
        amount                 Float64,\
        active                 Bool,\
        timestamp              Int64,\
        iggy_stream            String,\
        iggy_topic             String,\
        iggy_partition_id      UInt32,\
        iggy_id                String,\
        iggy_offset            UInt64,\
        iggy_checksum          UInt64,\
        iggy_timestamp         UInt64,\
        iggy_origin_timestamp  UInt64\
    ) ENGINE = MergeTree() ORDER BY iggy_offset";

const DDL_FIELD_MAPPINGS: &str = "\
    CREATE TABLE IF NOT EXISTS iggy_sink_mapped (\
        msg_id     UInt64,\
        msg_name   String,\
        msg_amount Float64\
    ) ENGINE = MergeTree() ORDER BY msg_id";

const DDL_ROWBINARY: &str = "\
    CREATE TABLE IF NOT EXISTS iggy_sink_rb (\
        payload String\
    ) ENGINE = MergeTree() ORDER BY tuple()";

const DDL_ROWBINARY_META: &str = "\
    CREATE TABLE IF NOT EXISTS iggy_sink_rb_meta (\
        iggy_stream            String,\
        iggy_topic             String,\
        iggy_partition_id      UInt32,\
        iggy_id                String,\
        iggy_offset            UInt64,\
        iggy_checksum          UInt64,\
        iggy_timestamp         UInt64,\
        iggy_origin_timestamp  UInt64,\
        payload                String\
    ) ENGINE = MergeTree() ORDER BY iggy_offset";

// ── Fixture A: JSON insert, no metadata ──────────────────────────────────────

pub struct ClickHouseSinkJsonFixture {
    container: ClickHouseContainer,
}

impl ClickHouseOps for ClickHouseSinkJsonFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ClickHouseContainer::start().await?;
        let client = container.create_client();
        container.execute_ddl(&client, DDL_JSON).await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(&self.container.url, "iggy_sink", "json", false)
    }
}

// ── Fixture B: JSON insert, with metadata ────────────────────────────────────

pub struct ClickHouseSinkJsonWithMetadataFixture {
    container: ClickHouseContainer,
}

impl ClickHouseOps for ClickHouseSinkJsonWithMetadataFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkJsonWithMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ClickHouseContainer::start().await?;
        let client = container.create_client();
        container.execute_ddl(&client, DDL_JSON_META).await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(&self.container.url, "iggy_sink_meta", "json", true)
    }
}

// ── Fixture C: JSON insert, field mappings ───────────────────────────────────

pub struct ClickHouseSinkJsonFieldMappingsFixture {
    container: ClickHouseContainer,
}

impl ClickHouseOps for ClickHouseSinkJsonFieldMappingsFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkJsonFieldMappingsFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ClickHouseContainer::start().await?;
        let client = container.create_client();
        container.execute_ddl(&client, DDL_FIELD_MAPPINGS).await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        // field_mappings are baked into the TOML config — only override url/table/path/streams
        base_envs(&self.container.url, "iggy_sink_mapped", "json", false)
    }
}

// ── Fixture D: RowBinary insert, no metadata ─────────────────────────────────

pub struct ClickHouseSinkRowBinaryFixture {
    container: ClickHouseContainer,
}

impl ClickHouseOps for ClickHouseSinkRowBinaryFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkRowBinaryFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ClickHouseContainer::start().await?;
        let client = container.create_client();
        container.execute_ddl(&client, DDL_ROWBINARY).await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(&self.container.url, "iggy_sink_rb", "rowbinary", false)
    }
}

// ── Fixture E: RowBinary insert, with metadata ───────────────────────────────

pub struct ClickHouseSinkRowBinaryWithMetadataFixture {
    container: ClickHouseContainer,
}

impl ClickHouseOps for ClickHouseSinkRowBinaryWithMetadataFixture {
    fn container(&self) -> &ClickHouseContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for ClickHouseSinkRowBinaryWithMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ClickHouseContainer::start().await?;
        let client = container.create_client();
        container.execute_ddl(&client, DDL_ROWBINARY_META).await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(&self.container.url, "iggy_sink_rb_meta", "rowbinary", true)
    }
}
