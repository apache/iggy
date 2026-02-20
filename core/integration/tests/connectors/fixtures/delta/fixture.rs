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

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;
use uuid::Uuid;

const ENV_SINK_TABLE_URI: &str = "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_TABLE_URI";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_DELTA_PATH";
const ENV_SINK_STORAGE_BACKEND_TYPE: &str =
    "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_STORAGE_BACKEND_TYPE";
const ENV_SINK_AWS_S3_ACCESS_KEY: &str =
    "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_AWS_S3_ACCESS_KEY";
const ENV_SINK_AWS_S3_SECRET_KEY: &str =
    "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_AWS_S3_SECRET_KEY";
const ENV_SINK_AWS_S3_REGION: &str = "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_AWS_S3_REGION";
const ENV_SINK_AWS_S3_ENDPOINT_URL: &str =
    "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_AWS_S3_ENDPOINT_URL";
const ENV_SINK_AWS_S3_ALLOW_HTTP: &str =
    "IGGY_CONNECTORS_SINK_DELTA_PLUGIN_CONFIG_AWS_S3_ALLOW_HTTP";

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";
const MINIO_PORT: u16 = 9000;
const MINIO_CONSOLE_PORT: u16 = 9001;
const MINIO_ACCESS_KEY: &str = "admin";
const MINIO_SECRET_KEY: &str = "password";
const MINIO_BUCKET: &str = "delta-warehouse";

pub struct DeltaFixture {
    _temp_dir: TempDir,
    table_path: PathBuf,
}

impl DeltaFixture {
    pub async fn wait_for_delta_log(
        &self,
        min_versions: usize,
        max_attempts: usize,
        interval_ms: u64,
    ) -> Result<usize, TestBinaryError> {
        let delta_log_dir = self.table_path.join("_delta_log");

        for _ in 0..max_attempts {
            let count = Self::count_delta_versions(&delta_log_dir);
            if count >= min_versions {
                info!("Found {count} delta log versions (required: {min_versions})");
                return Ok(count);
            }
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        }

        let final_count = Self::count_delta_versions(&delta_log_dir);
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {min_versions} delta log versions, found {final_count} after {max_attempts} attempts"
            ),
        })
    }

    fn count_delta_versions(delta_log_dir: &std::path::Path) -> usize {
        std::fs::read_dir(delta_log_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
                    .count()
            })
            .unwrap_or(0)
    }
}

#[async_trait]
impl TestFixture for DeltaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let temp_dir = TempDir::new().map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "DeltaFixture".to_string(),
            message: format!("Failed to create temp directory: {error}"),
        })?;

        let table_path = temp_dir.path().join("delta_table");
        info!(
            "Delta fixture created with table path: {}",
            table_path.display()
        );

        Ok(Self {
            _temp_dir: temp_dir,
            table_path,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let table_uri = format!("file://{}", self.table_path.display());

        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_TABLE_URI.to_string(), table_uri);
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_delta_sink".to_string(),
        );
        envs
    }
}

pub struct DeltaS3Fixture {
    #[allow(dead_code)]
    minio: ContainerAsync<GenericImage>,
    minio_endpoint: String,
}

impl DeltaS3Fixture {
    async fn start_minio(
        network: &str,
        container_name: &str,
    ) -> Result<(ContainerAsync<GenericImage>, String), TestBinaryError> {
        let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(MINIO_PORT.tcp())
            .with_exposed_port(MINIO_CONSOLE_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_network(network)
            .with_container_name(container_name)
            .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
            .with_mapped_port(0, MINIO_PORT.tcp())
            .with_mapped_port(0, MINIO_CONSOLE_PORT.tcp())
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: format!("Failed to start MinIO container: {error}"),
            })?;

        info!("Started MinIO container for Delta S3 tests");

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(MINIO_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: "No mapping for MinIO port".to_string(),
            })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        info!("MinIO container available at {endpoint}");

        Ok((container, endpoint))
    }

    async fn create_bucket(minio_endpoint: &str) -> Result<(), TestBinaryError> {
        use std::process::Command;

        let host = minio_endpoint.trim_start_matches("http://");
        let mc_host = format!("http://{}:{}@{}", MINIO_ACCESS_KEY, MINIO_SECRET_KEY, host);

        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network=host",
                "-e",
                &format!("MC_HOST_minio={}", mc_host),
                "minio/mc",
                "mb",
                "--ignore-existing",
                &format!("minio/{}", MINIO_BUCKET),
            ])
            .output()
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: format!("Failed to run mc command: {error}"),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: format!("Failed to create bucket: stderr={stderr}, stdout={stdout}"),
            });
        }

        info!("Created MinIO bucket: {MINIO_BUCKET}");
        Ok(())
    }
}

#[async_trait]
impl TestFixture for DeltaS3Fixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let id = Uuid::new_v4();
        let network = format!("iggy-delta-s3-{id}");
        let minio_name = format!("minio-delta-{id}");

        let (minio, minio_endpoint) = Self::start_minio(&network, &minio_name).await?;
        Self::create_bucket(&minio_endpoint).await?;

        info!("Delta S3 fixture ready with MinIO at {minio_endpoint}");

        Ok(Self {
            minio,
            minio_endpoint,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let table_uri = format!("s3://{MINIO_BUCKET}/delta_table");

        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_delta_sink".to_string(),
        );
        envs.insert(ENV_SINK_TABLE_URI.to_string(), table_uri);
        envs.insert(ENV_SINK_STORAGE_BACKEND_TYPE.to_string(), "s3".to_string());
        envs.insert(
            ENV_SINK_AWS_S3_ACCESS_KEY.to_string(),
            MINIO_ACCESS_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_AWS_S3_SECRET_KEY.to_string(),
            MINIO_SECRET_KEY.to_string(),
        );
        envs.insert(ENV_SINK_AWS_S3_REGION.to_string(), "us-east-1".to_string());
        envs.insert(
            ENV_SINK_AWS_S3_ENDPOINT_URL.to_string(),
            self.minio_endpoint.clone(),
        );
        envs.insert(ENV_SINK_AWS_S3_ALLOW_HTTP.to_string(), "true".to_string());
        envs
    }
}
