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

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::connectors::fixtures::{
    self,
    redshift::{
        MinioContainer, PostgresContainer, RedshiftContainer,
        container::{
            DEFAULT_POLL_ATTEMPTS, DEFAULT_POLL_INTERVAL_MS, DEFAULT_SINK_TABLE,
            DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_ARCHIVE, ENV_SINK_CONNECTION_STRING,
            ENV_SINK_PATH, ENV_SINK_PAYLOAD_FORMAT, ENV_SINK_S3_BUCKET, ENV_SINK_S3_ENDPOINT,
            ENV_SINK_S3_PREFIX, ENV_SINK_STAGING_ACCESS_KEY, ENV_SINK_STAGING_REGION,
            ENV_SINK_STAGING_SECRET, ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA,
            ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TARGET_TABLE,
            MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_SECRET_KEY, STAGING_PREFIX, STAGING_REGION,
            SinkPayloadFormat, SinkSchema,
        },
    },
};

pub struct RedshiftSinkFixture {
    #[allow(dead_code)]
    minio: MinioContainer,
    #[allow(dead_code)]
    redshift: RedshiftContainer,
    postgres: PostgresContainer,
    payload_format: SinkPayloadFormat,
    schema: SinkSchema,
    pub minio_endpoint: String,
}

impl RedshiftSinkFixture {
    /// Assertions read from here — never from `redshift`.
    pub async fn target_pool(&self) -> Result<sqlx::Pool<sqlx::Postgres>, TestBinaryError> {
        self.postgres.create_pool().await
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
            let result = sqlx::query_as::<_, T>(sqlx::AssertSqlSafe(query))
                .fetch_all(pool)
                .await;
            if let Ok(fetched) = result {
                rows = fetched;
                if rows.len() >= expected_count {
                    return Ok(rows);
                }
            } else if let Err(e) = result {
                tracing::error!("{e}");
            }

            tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
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
impl TestFixture for RedshiftSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let postgres = PostgresContainer::start().await?;

        let id = Uuid::now_v7();
        let network = format!("iggy-redshift-{id}");

        let minio_name = fixtures::unique_container_name("minio-redshift");

        let minio = MinioContainer::start(&network, &minio_name).await?;

        create_bucket(&minio.endpoint)?;

        let redshift =
            RedshiftContainer::start(postgres.connection_string.clone(), minio.endpoint.clone())
                .await?;

        Ok(Self {
            minio_endpoint: minio.endpoint.clone(),
            minio,
            redshift,
            postgres,
            payload_format: SinkPayloadFormat::default(),
            schema: SinkSchema::default(),
        })
    }

    fn connectors_runtime_envs(&self) -> std::collections::HashMap<String, String> {
        let mut envs = HashMap::new();

        envs.insert(
            ENV_SINK_CONNECTION_STRING.to_string(),
            self.redshift.connection_string.clone(),
        );
        envs.insert(
            ENV_SINK_TARGET_TABLE.to_string(),
            DEFAULT_SINK_TABLE.to_string(),
        );

        envs.insert(
            ENV_SINK_STAGING_ACCESS_KEY.to_string(),
            MINIO_ACCESS_KEY.to_string(),
        );

        envs.insert(
            ENV_SINK_STAGING_SECRET.to_string(),
            MINIO_SECRET_KEY.to_string(),
        );

        envs.insert(ENV_SINK_S3_BUCKET.to_string(), MINIO_BUCKET.to_string());
        envs.insert(ENV_SINK_S3_PREFIX.to_string(), STAGING_PREFIX.to_string());

        envs.insert(
            ENV_SINK_S3_ENDPOINT.to_string(),
            self.minio_endpoint.clone(),
        );

        envs.insert(
            ENV_SINK_STAGING_REGION.to_string(),
            STAGING_REGION.to_string(),
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
            "../../target/debug/libiggy_connector_redshift_sink".to_string(),
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
            SinkPayloadFormat::Text => "text",
        };
        envs.insert(ENV_SINK_PAYLOAD_FORMAT.to_string(), format_str.to_string());

        envs
    }
}

/// redshift sink fixture for bytea payload format.
pub struct RedshiftSinkByteaFixture {
    inner: RedshiftSinkFixture,
}

impl std::ops::Deref for RedshiftSinkByteaFixture {
    type Target = RedshiftSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RedshiftSinkByteaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let postgres = PostgresContainer::start().await?;

        let id = Uuid::now_v7();
        let network = format!("iggy-redshift-{id}");

        let minio_name = fixtures::unique_container_name("minio-redshift");

        let minio = MinioContainer::start(&network, &minio_name).await?;

        create_bucket(&minio.endpoint)?;

        let redshift =
            RedshiftContainer::start(postgres.connection_string.clone(), minio.endpoint.clone())
                .await?;

        Ok(Self {
            inner: RedshiftSinkFixture {
                minio_endpoint: minio.endpoint.clone(),
                minio,
                redshift,
                postgres,
                payload_format: SinkPayloadFormat::Bytea,
                schema: SinkSchema::Raw,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// redshift sink fixture for bytea payload format.
pub struct RedshiftSinkJsonFixture {
    inner: RedshiftSinkFixture,
}

impl std::ops::Deref for RedshiftSinkJsonFixture {
    type Target = RedshiftSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RedshiftSinkJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let postgres = PostgresContainer::start().await?;

        let id = Uuid::now_v7();
        let network = format!("iggy-redshift-{id}");

        let minio_name = fixtures::unique_container_name("minio-redshift");

        let minio = MinioContainer::start(&network, &minio_name).await?;

        create_bucket(&minio.endpoint)?;

        let redshift =
            RedshiftContainer::start(postgres.connection_string.clone(), minio.endpoint.clone())
                .await?;

        Ok(Self {
            inner: RedshiftSinkFixture {
                minio_endpoint: minio.endpoint.clone(),
                minio,
                redshift,
                postgres,
                payload_format: SinkPayloadFormat::Text,
                schema: SinkSchema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// redshift sink fixture for bytea payload format.
pub struct RedshiftSinkNoArchiveFixture {
    inner: RedshiftSinkFixture,
}

impl RedshiftSinkNoArchiveFixture {
    pub fn confirm_empty_bucket(&self) -> Result<bool, TestBinaryError> {
        bucket_empty(&self.minio_endpoint)
    }
}

impl std::ops::Deref for RedshiftSinkNoArchiveFixture {
    type Target = RedshiftSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RedshiftSinkNoArchiveFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let postgres = PostgresContainer::start().await?;

        let id = Uuid::now_v7();
        let network = format!("iggy-redshift-{id}");

        let minio_name = fixtures::unique_container_name("minio-redshift");

        let minio = MinioContainer::start(&network, &minio_name).await?;

        create_bucket(&minio.endpoint)?;

        let redshift =
            RedshiftContainer::start(postgres.connection_string.clone(), minio.endpoint.clone())
                .await?;

        Ok(Self {
            inner: RedshiftSinkFixture {
                minio_endpoint: minio.endpoint.clone(),
                minio,
                redshift,
                postgres,
                payload_format: SinkPayloadFormat::Text,
                schema: SinkSchema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();

        let schema_str = match self.schema {
            SinkSchema::Json => "json",
            SinkSchema::Raw => "raw",
        };
        envs.insert(
            ENV_SINK_STREAMS_0_SCHEMA.to_string(),
            schema_str.to_string(),
        );

        envs.insert(ENV_SINK_ARCHIVE.to_string(), false.to_string());

        let format_str = match self.payload_format {
            SinkPayloadFormat::Bytea => "bytea",
            SinkPayloadFormat::Text => "text",
        };

        envs.insert(ENV_SINK_PAYLOAD_FORMAT.to_string(), format_str.to_string());

        envs
    }
}

fn create_bucket(minio_endpoint: &str) -> Result<(), TestBinaryError> {
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
            fixture_type: "RedshiftFixture".to_string(),
            message: format!("Failed to run mc command: {error}"),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "IcebergFixture".to_string(),
            message: format!("Failed to create bucket: stderr={stderr}, stdout={stdout}"),
        });
    }

    tracing::info!("Created MinIO bucket: {MINIO_BUCKET}");
    Ok(())
}

fn bucket_empty(minio_endpoint: &str) -> Result<bool, TestBinaryError> {
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
            "ls",
            &format!("minio/{}", MINIO_BUCKET),
        ])
        .output()
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "RedshiftFixture".to_string(),
            message: format!("Failed to run mc command: {error}"),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "IcebergFixture".to_string(),
            message: format!("Failed to create bucket: stderr={stderr}, stdout={stdout}"),
        });
    }

    tracing::info!("Created MinIO bucket: {MINIO_BUCKET}");
    Ok(output.stdout.is_empty())
}
