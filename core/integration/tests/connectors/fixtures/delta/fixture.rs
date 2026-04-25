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
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
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

async fn count_rows(
    table_uri: url::Url,
    storage_options: HashMap<String, String>,
) -> Result<usize, TestBinaryError> {
    use deltalake::arrow::array::Int64Array;

    let table = deltalake::open_table_with_storage_options(table_uri, storage_options)
        .await
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to open delta table: {e}"),
        })?;

    let batch = table
        .snapshot()
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to get table snapshot: {e}"),
        })?
        .add_actions_table(false)
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to get add actions table: {e}"),
        })?;

    let total = batch
        .column_by_name("num_records")
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .map(|arr| arr.iter().flatten().sum::<i64>() as usize)
        .unwrap_or(0);

    Ok(total)
}

async fn wait_for_row_count(
    table_uri: url::Url,
    storage_options: HashMap<String, String>,
    expected_rows: usize,
    max_attempts: usize,
    interval_ms: u64,
) -> Result<usize, TestBinaryError> {
    for _ in 0..max_attempts {
        let count = count_rows(table_uri.clone(), storage_options.clone())
            .await
            .unwrap_or(0);
        if count >= expected_rows {
            info!("Found {count} rows in delta table (required: {expected_rows})");
            return Ok(count);
        }
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
    }

    let final_count = count_rows(table_uri, storage_options).await.unwrap_or(0);
    Err(TestBinaryError::InvalidState {
        message: format!(
            "Expected at least {expected_rows} rows, found {final_count} after {max_attempts} attempts"
        ),
    })
}

fn table_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Long), true),
        StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
        StructField::new("count", DataType::Primitive(PrimitiveType::Integer), true),
        StructField::new("amount", DataType::Primitive(PrimitiveType::Double), true),
        StructField::new("active", DataType::Primitive(PrimitiveType::Boolean), true),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

impl DeltaFixture {
    async fn create_table(table_uri: &str) -> Result<(), TestBinaryError> {
        let columns = table_columns();
        CreateBuilder::new()
            .with_location(table_uri)
            .with_columns(columns)
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaFixture".to_string(),
                message: format!("Failed to create Delta table: {error}"),
            })?;
        Ok(())
    }

    pub async fn wait_for_row_count(
        &self,
        expected_rows: usize,
        max_attempts: usize,
        interval_ms: u64,
    ) -> Result<usize, TestBinaryError> {
        let table_uri =
            url::Url::parse(&format!("file://{}", self.table_path.display())).map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to parse table URI: {e}"),
                }
            })?;
        wait_for_row_count(
            table_uri,
            HashMap::new(),
            expected_rows,
            max_attempts,
            interval_ms,
        )
        .await
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
        let table_uri = format!("file://{}", table_path.display());
        Self::create_table(&table_uri).await?;
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
        use tokio::process::Command;

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
            .await
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

    async fn create_table(minio_endpoint: &str) -> Result<(), TestBinaryError> {
        let table_uri = format!("s3://{MINIO_BUCKET}/delta_table");
        let columns = table_columns();
        let storage_options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), MINIO_ACCESS_KEY.into()),
            ("AWS_SECRET_ACCESS_KEY".into(), MINIO_SECRET_KEY.into()),
            ("AWS_REGION".into(), "us-east-1".into()),
            ("AWS_ENDPOINT_URL".into(), minio_endpoint.into()),
            ("AWS_ALLOW_HTTP".into(), "true".into()),
            ("AWS_S3_ALLOW_HTTP".into(), "true".into()),
        ]);
        CreateBuilder::new()
            .with_location(table_uri)
            .with_storage_options(storage_options)
            .with_columns(columns)
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "DeltaS3Fixture".to_string(),
                message: format!("Failed to create Delta table in MinIO: {error}"),
            })?;
        Ok(())
    }

    pub async fn wait_for_row_count(
        &self,
        expected_rows: usize,
        max_attempts: usize,
        interval_ms: u64,
    ) -> Result<usize, TestBinaryError> {
        let table_uri =
            url::Url::parse(&format!("s3://{MINIO_BUCKET}/delta_table")).map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to parse table URI: {e}"),
                }
            })?;
        let storage_options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), MINIO_ACCESS_KEY.into()),
            ("AWS_SECRET_ACCESS_KEY".into(), MINIO_SECRET_KEY.into()),
            ("AWS_REGION".into(), "us-east-1".into()),
            ("AWS_ENDPOINT_URL".into(), self.minio_endpoint.clone()),
            ("AWS_ALLOW_HTTP".into(), "true".into()),
            ("AWS_S3_ALLOW_HTTP".into(), "true".into()),
        ]);
        wait_for_row_count(
            table_uri,
            storage_options,
            expected_rows,
            max_attempts,
            interval_ms,
        )
        .await
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
        Self::create_table(&minio_endpoint).await?;

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
