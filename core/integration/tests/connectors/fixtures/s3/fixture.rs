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

use async_trait::async_trait;
use integration::harness::seeds;
use integration::harness::{TestBinaryError, TestFixture};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::collections::HashMap;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;
use uuid::Uuid;

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";
const MINIO_PORT: u16 = 9000;
const MINIO_CONSOLE_PORT: u16 = 9001;

const MINIO_ACCESS_KEY: &str = "admin";
const MINIO_SECRET_KEY: &str = "password";
const MINIO_BUCKET: &str = "iggy-s3-test";

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
    container: ContainerAsync<GenericImage>,
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
        let id = Uuid::new_v4();
        let container_name = format!("minio-s3-{id}");

        let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(MINIO_PORT.tcp())
            .with_exposed_port(MINIO_CONSOLE_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/minio/health/live")
                    .with_port(MINIO_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_container_name(&container_name)
            .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
            .with_mapped_port(0, MINIO_PORT.tcp())
            .with_mapped_port(0, MINIO_CONSOLE_PORT.tcp())
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "S3SinkFixture".to_string(),
                message: format!("Failed to start MinIO container: {error}"),
            })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "S3SinkFixture".to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(MINIO_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "S3SinkFixture".to_string(),
                message: "No mapping for MinIO port".to_string(),
            })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        info!("MinIO container for S3 sink available at {endpoint}");

        let region = Region::Custom {
            region: "us-east-1".to_string(),
            endpoint: endpoint.clone(),
        };
        let credentials = Credentials::new(
            Some(MINIO_ACCESS_KEY),
            Some(MINIO_SECRET_KEY),
            None,
            None,
            None,
        )
        .map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "S3SinkFixture".to_string(),
            message: format!("Failed to create credentials: {e}"),
        })?;

        let config = s3::BucketConfiguration::default();
        let response = Bucket::create_with_path_style(
            MINIO_BUCKET,
            region.clone(),
            credentials.clone(),
            config,
        )
        .await
        .map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "S3SinkFixture".to_string(),
            message: format!("Failed to create bucket: {e}"),
        })?;
        info!(
            "S3 bucket '{}' ready (status: {})",
            MINIO_BUCKET, response.response_code
        );

        let mut bucket = Bucket::new(MINIO_BUCKET, region, credentials).map_err(|e| {
            TestBinaryError::FixtureSetup {
                fixture_type: "S3SinkFixture".to_string(),
                message: format!("Failed to create bucket handle: {e}"),
            }
        })?;
        bucket.set_path_style();

        Ok(Self {
            container,
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
        envs.insert(ENV_SINK_PLUGIN_BUCKET.to_string(), MINIO_BUCKET.to_string());
        envs.insert(ENV_SINK_PLUGIN_REGION.to_string(), "us-east-1".to_string());
        envs.insert(ENV_SINK_PLUGIN_ENDPOINT.to_string(), self.endpoint.clone());
        envs.insert(ENV_SINK_PLUGIN_PREFIX.to_string(), String::new());
        envs.insert(
            ENV_SINK_PLUGIN_ACCESS_KEY.to_string(),
            MINIO_ACCESS_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_PLUGIN_SECRET_KEY.to_string(),
            MINIO_SECRET_KEY.to_string(),
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
