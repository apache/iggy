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

use std::collections::HashMap;

use async_trait::async_trait;
use integration::harness::seeds;
use integration::harness::{TestBinaryError, TestFixture};
use s3::Bucket;
use tracing::info;

use crate::connectors::fixtures::{
    self,
    floci::{self, ACCESS_KEY, FlociContainer, REGION, SECRET_KEY},
};

const TEST_BUCKET: &str = "iggy-s3-test";

const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_S3_PATH";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_S3_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_S3_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_S3_STREAMS_0_SCHEMA";
const ENV_SINK_PLUGIN_BUCKET: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_BUCKET";
const ENV_SINK_PLUGIN_REGION: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_REGION";
const ENV_SINK_PLUGIN_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_ENDPOINT";
const ENV_SINK_PLUGIN_PREFIX: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_PREFIX";
const ENV_SINK_PLUGIN_ACCESS_KEY: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_ACCESS_KEY_ID";
const ENV_SINK_PLUGIN_SECRET_KEY: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_SECRET_ACCESS_KEY";
const ENV_SINK_PLUGIN_FILE_ROTATION: &str = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_FILE_ROTATION";
const ENV_SINK_PLUGIN_MAX_MESSAGES: &str =
    "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_MAX_MESSAGES_PER_FILE";

const DEFAULT_MAX_MESSAGES_PER_FILE: usize = 5;
const POLL_ATTEMPTS: usize = 30;
const POLL_INTERVAL_MS: u64 = 500;

pub trait S3SinkOps: Sync {
    fn bucket(&self) -> &Bucket;
    #[allow(dead_code)]
    fn endpoint(&self) -> &str;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>, TestBinaryError>> + Send {
        async move {
            let results = self
                .bucket()
                .list(prefix.to_string(), None)
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to list objects: {e}"),
                })?;

            let keys: Vec<String> = results
                .iter()
                .flat_map(|r| r.contents.iter().map(|o| o.key.clone()))
                .collect();
            Ok(keys)
        }
    }

    fn get_object(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, TestBinaryError>> + Send {
        async move {
            let response =
                self.bucket()
                    .get_object(key)
                    .await
                    .map_err(|e| TestBinaryError::InvalidState {
                        message: format!("Failed to get object '{key}': {e}"),
                    })?;
            Ok(response.to_vec())
        }
    }

    fn wait_for_objects(
        &self,
        prefix: &str,
        min_objects: usize,
    ) -> impl std::future::Future<Output = Result<Vec<String>, TestBinaryError>> + Send {
        async move {
            for _ in 0..POLL_ATTEMPTS {
                let keys = self.list_objects(prefix).await?;
                if keys.len() >= min_objects {
                    info!(
                        "Found {} objects under prefix '{}' (required: {})",
                        keys.len(),
                        prefix,
                        min_objects
                    );
                    return Ok(keys);
                }
                tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }

            let keys = self.list_objects(prefix).await?;
            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected at least {min_objects} objects under '{prefix}', found {} after {POLL_ATTEMPTS} attempts",
                    keys.len()
                ),
            })
        }
    }
}

pub struct S3SinkFixture {
    #[allow(dead_code)]
    floci: FlociContainer,
    bucket: Box<Bucket>,
    endpoint: String,
}

impl S3SinkOps for S3SinkFixture {
    fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl TestFixture for S3SinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let floci =
            FlociContainer::start(None, &fixtures::unique_container_name("floci-s3")).await?;
        let endpoint = floci.endpoint.clone();
        let bucket = floci::create_bucket(&endpoint, TEST_BUCKET).await?;

        Ok(Self {
            floci,
            bucket,
            endpoint,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_s3_sink".to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            seeds::names::STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", seeds::names::TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_SINK_PLUGIN_BUCKET.to_string(), TEST_BUCKET.to_string());
        envs.insert(ENV_SINK_PLUGIN_REGION.to_string(), REGION.to_string());
        envs.insert(ENV_SINK_PLUGIN_ENDPOINT.to_string(), self.endpoint.clone());
        envs.insert(ENV_SINK_PLUGIN_PREFIX.to_string(), String::new());
        envs.insert(
            ENV_SINK_PLUGIN_ACCESS_KEY.to_string(),
            ACCESS_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_PLUGIN_SECRET_KEY.to_string(),
            SECRET_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_PLUGIN_FILE_ROTATION.to_string(),
            "messages".to_string(),
        );
        envs.insert(
            ENV_SINK_PLUGIN_MAX_MESSAGES.to_string(),
            DEFAULT_MAX_MESSAGES_PER_FILE.to_string(),
        );
        envs
    }
}

pub struct S3SinkRotationFixture {
    inner: S3SinkFixture,
}

impl S3SinkOps for S3SinkRotationFixture {
    fn bucket(&self) -> &Bucket {
        self.inner.bucket()
    }

    fn endpoint(&self) -> &str {
        self.inner.endpoint()
    }
}

#[async_trait]
impl TestFixture for S3SinkRotationFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = S3SinkFixture::setup().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SINK_PLUGIN_FILE_ROTATION.to_string(),
            "messages".to_string(),
        );
        envs.insert(ENV_SINK_PLUGIN_MAX_MESSAGES.to_string(), "10".to_string());
        envs
    }
}
