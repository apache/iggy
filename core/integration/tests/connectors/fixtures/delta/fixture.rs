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

use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use integration::harness::{TestBinaryError, TestFixture};
use tempfile::TempDir;
use tracing::info;

use crate::connectors::fixtures::{
    self,
    floci::{self, ACCESS_KEY, FlociContainer, REGION, SECRET_KEY},
};

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

const TEST_BUCKET: &str = "delta-warehouse";

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
    floci: FlociContainer,
    floci_endpoint: String,
}

impl DeltaS3Fixture {
    async fn create_table(floci_endpoint: &str) -> Result<(), TestBinaryError> {
        let table_uri = format!("s3://{TEST_BUCKET}/delta_table");
        let columns = table_columns();
        let storage_options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), ACCESS_KEY.into()),
            ("AWS_SECRET_ACCESS_KEY".into(), SECRET_KEY.into()),
            ("AWS_REGION".into(), REGION.into()),
            ("AWS_ENDPOINT_URL".into(), floci_endpoint.into()),
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
                message: format!("Failed to create Delta table in Floci: {error}"),
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
            url::Url::parse(&format!("s3://{TEST_BUCKET}/delta_table")).map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to parse table URI: {e}"),
                }
            })?;
        let storage_options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), ACCESS_KEY.into()),
            ("AWS_SECRET_ACCESS_KEY".into(), SECRET_KEY.into()),
            ("AWS_REGION".into(), REGION.into()),
            ("AWS_ENDPOINT_URL".into(), self.floci_endpoint.clone()),
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
        let floci_name = fixtures::unique_container_name("floci-delta");

        let floci = FlociContainer::start(None, &floci_name).await?;
        let floci_endpoint = floci.endpoint.clone();
        floci::create_bucket(&floci_endpoint, TEST_BUCKET).await?;
        Self::create_table(&floci_endpoint).await?;

        info!("Delta S3 fixture ready with Floci at {floci_endpoint}");

        Ok(Self {
            floci,
            floci_endpoint,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let table_uri = format!("s3://{TEST_BUCKET}/delta_table");

        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_delta_sink".to_string(),
        );
        envs.insert(ENV_SINK_TABLE_URI.to_string(), table_uri);
        envs.insert(ENV_SINK_STORAGE_BACKEND_TYPE.to_string(), "s3".to_string());
        envs.insert(
            ENV_SINK_AWS_S3_ACCESS_KEY.to_string(),
            ACCESS_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_AWS_S3_SECRET_KEY.to_string(),
            SECRET_KEY.to_string(),
        );
        envs.insert(ENV_SINK_AWS_S3_REGION.to_string(), REGION.to_string());
        envs.insert(
            ENV_SINK_AWS_S3_ENDPOINT_URL.to_string(),
            self.floci_endpoint.clone(),
        );
        envs.insert(ENV_SINK_AWS_S3_ALLOW_HTTP.to_string(), "true".to_string());
        envs
    }
}
