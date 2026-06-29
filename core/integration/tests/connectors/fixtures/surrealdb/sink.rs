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
    DEFAULT_DATABASE, DEFAULT_NAMESPACE, DEFAULT_POLL_ATTEMPTS, DEFAULT_POLL_INTERVAL_MS,
    DEFAULT_TABLE, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_AUTH_SCOPE,
    ENV_SINK_AUTO_DEFINE_TABLE, ENV_SINK_BATCH_SIZE, ENV_SINK_DATABASE, ENV_SINK_DEFINE_INDEXES,
    ENV_SINK_ENDPOINT, ENV_SINK_NAMESPACE, ENV_SINK_PASSWORD, ENV_SINK_PATH,
    ENV_SINK_PAYLOAD_FORMAT, ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA,
    ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TABLE, ENV_SINK_USERNAME,
    ROOT_PASSWORD, ROOT_USERNAME, SurrealDbClient, SurrealDbContainer, SurrealDbOps,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub trait SurrealDbSinkProfile {
    const SCHEMA: &'static str;
    const BATCH_SIZE: Option<usize>;
}

pub struct SurrealDbSinkJsonProfile;
pub struct SurrealDbSinkRawProfile;
pub struct SurrealDbSinkBatchProfile;

impl SurrealDbSinkProfile for SurrealDbSinkJsonProfile {
    const SCHEMA: &'static str = "json";
    const BATCH_SIZE: Option<usize> = None;
}

impl SurrealDbSinkProfile for SurrealDbSinkRawProfile {
    const SCHEMA: &'static str = "raw";
    const BATCH_SIZE: Option<usize> = None;
}

impl SurrealDbSinkProfile for SurrealDbSinkBatchProfile {
    const SCHEMA: &'static str = "json";
    const BATCH_SIZE: Option<usize> = Some(10);
}

pub type SurrealDbSinkJsonFixture = SurrealDbSinkFixture<SurrealDbSinkJsonProfile>;
pub type SurrealDbSinkRawFixture = SurrealDbSinkFixture<SurrealDbSinkRawProfile>;
pub type SurrealDbSinkBatchFixture = SurrealDbSinkFixture<SurrealDbSinkBatchProfile>;

pub struct SurrealDbSinkFixture<P = SurrealDbSinkJsonProfile> {
    container: SurrealDbContainer,
    profile: PhantomData<P>,
}

impl<P> SurrealDbOps for SurrealDbSinkFixture<P>
where
    P: Sync,
{
    fn container(&self) -> &SurrealDbContainer {
        &self.container
    }
}

impl<P> SurrealDbSinkFixture<P> {
    pub async fn wait_for_records(
        &self,
        client: &SurrealDbClient,
        expected: usize,
    ) -> Result<Vec<Value>, TestBinaryError> {
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            let records = self.select_all_records(client).await?;
            if records.len() >= expected {
                info!(
                    "Found {} records in SurrealDB table '{DEFAULT_TABLE}'",
                    records.len()
                );
                return Ok(records);
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected} SurrealDB records after {DEFAULT_POLL_ATTEMPTS} attempts"
            ),
        })
    }

    pub async fn select_all_records(
        &self,
        client: &SurrealDbClient,
    ) -> Result<Vec<Value>, TestBinaryError> {
        let query = format!("SELECT * FROM {DEFAULT_TABLE};");
        let value = client.query_result(&query).await?;
        decode_records_sorted_by_offset(value)
    }

    pub async fn select_records_by_message_id(
        &self,
        client: &SurrealDbClient,
        message_id: u128,
    ) -> Result<Vec<Value>, TestBinaryError> {
        let message_id = serde_json::to_string(&message_id.to_string()).map_err(|e| {
            TestBinaryError::InvalidState {
                message: format!("Failed to encode SurrealDB message id: {e}"),
            }
        })?;
        let query = format!("SELECT * FROM {DEFAULT_TABLE} WHERE iggy_message_id = {message_id};");
        let value = client.query_result(&query).await?;
        decode_records_sorted_by_offset(value)
    }

    pub async fn insert_preseeded_record(
        &self,
        client: &SurrealDbClient,
        record_id: &str,
        message_id: u128,
    ) -> Result<(), TestBinaryError> {
        let records = serde_json::to_string(&json!([
            {
                "id": record_id,
                "iggy_message_id": message_id.to_string(),
                "seed_marker": "preseed-unchanged",
                "payload": "preseeded"
            }
        ]))
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to encode SurrealDB preseed record: {e}"),
        })?;
        let query = format!("INSERT INTO {DEFAULT_TABLE} {records} RETURN NONE;");
        client.query_result(&query).await.map(|_| ())
    }
}

fn decode_records_sorted_by_offset(value: Value) -> Result<Vec<Value>, TestBinaryError> {
    let mut records: Vec<Value> =
        serde_json::from_value(value).map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to decode SurrealDB records: {e}"),
        })?;
    records.sort_by_key(|record| {
        record
            .get("iggy_offset")
            .and_then(Value::as_str)
            .and_then(|offset| offset.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    });

    Ok(records)
}

#[async_trait]
impl<P> TestFixture for SurrealDbSinkFixture<P>
where
    P: SurrealDbSinkProfile + Send + Sync,
{
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = SurrealDbContainer::start().await?;
        Ok(Self {
            container,
            profile: PhantomData,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_ENDPOINT.to_string(),
            self.container.endpoint.clone(),
        );
        envs.insert(
            ENV_SINK_NAMESPACE.to_string(),
            DEFAULT_NAMESPACE.to_string(),
        );
        envs.insert(ENV_SINK_DATABASE.to_string(), DEFAULT_DATABASE.to_string());
        envs.insert(ENV_SINK_TABLE.to_string(), DEFAULT_TABLE.to_string());
        envs.insert(ENV_SINK_USERNAME.to_string(), ROOT_USERNAME.to_string());
        envs.insert(ENV_SINK_PASSWORD.to_string(), ROOT_PASSWORD.to_string());
        envs.insert(ENV_SINK_AUTH_SCOPE.to_string(), "root".to_string());
        envs.insert(ENV_SINK_AUTO_DEFINE_TABLE.to_string(), "true".to_string());
        envs.insert(ENV_SINK_DEFINE_INDEXES.to_string(), "true".to_string());
        envs.insert(ENV_SINK_PAYLOAD_FORMAT.to_string(), "auto".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), P::SCHEMA.to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            format!("surrealdb_sink_{}_cg", P::SCHEMA),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_surrealdb_sink".to_string(),
        );

        if let Some(batch_size) = P::BATCH_SIZE {
            envs.insert(ENV_SINK_BATCH_SIZE.to_string(), batch_size.to_string());
        }

        envs
    }
}
