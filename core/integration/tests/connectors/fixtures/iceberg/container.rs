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
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use std::collections::HashMap;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;
use uuid::Uuid;

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";
const MINIO_PORT: u16 = 9000;
const MINIO_CONSOLE_PORT: u16 = 9001;
const ICEBERG_REST_IMAGE: &str = "apache/iceberg-rest-fixture";
const ICEBERG_REST_TAG: &str = "latest";
const ICEBERG_REST_PORT: u16 = 8181;

pub const MINIO_ACCESS_KEY: &str = "admin";
pub const MINIO_SECRET_KEY: &str = "password";
pub const MINIO_BUCKET: &str = "warehouse";

pub const ENV_SINK_URI: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_URI";
pub const ENV_SINK_WAREHOUSE: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_WAREHOUSE";
pub const ENV_SINK_STORE_URL: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_URL";
pub const ENV_SINK_STORE_ACCESS_KEY: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_ACCESS_KEY_ID";
pub const ENV_SINK_STORE_SECRET: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_SECRET_ACCESS_KEY";
pub const ENV_SINK_STORE_REGION: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_REGION";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PATH";

pub struct MinioContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub internal_endpoint: String,
}

impl MinioContainer {
    pub async fn start(network: &str, container_name: &str) -> Result<Self, TestBinaryError> {
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
                fixture_type: "MinioContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        info!("Started MinIO container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MinioContainer".to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(MINIO_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MinioContainer".to_string(),
                message: "No mapping for MinIO port".to_string(),
            })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        let internal_endpoint = format!("http://{container_name}:{MINIO_PORT}");
        info!("MinIO container available at {endpoint} (internal: {internal_endpoint})");

        Ok(Self {
            container,
            endpoint,
            internal_endpoint,
        })
    }
}

pub struct IcebergRestContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub catalog_url: String,
}

impl IcebergRestContainer {
    pub async fn start(
        network: &str,
        minio_internal_endpoint: &str,
    ) -> Result<Self, TestBinaryError> {
        let warehouse_path = format!("s3://{MINIO_BUCKET}/");

        let container = GenericImage::new(ICEBERG_REST_IMAGE, ICEBERG_REST_TAG)
            .with_exposed_port(ICEBERG_REST_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Started Server@"))
            .with_startup_timeout(std::time::Duration::from_secs(30))
            .with_network(network)
            .with_env_var(
                "CATALOG_CATALOG__IMPL",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .with_env_var("CATALOG_URI", "jdbc:sqlite:/tmp/iceberg_catalog.db")
            .with_env_var("CATALOG_WAREHOUSE", &warehouse_path)
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", minio_internal_endpoint)
            .with_env_var("CATALOG_S3_ACCESS__KEY__ID", MINIO_ACCESS_KEY)
            .with_env_var("CATALOG_S3_SECRET__ACCESS__KEY", MINIO_SECRET_KEY)
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .with_env_var("AWS_REGION", "us-east-1")
            .with_env_var("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
            .with_mapped_port(0, ICEBERG_REST_PORT.tcp())
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        info!("Started Iceberg REST catalog container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(ICEBERG_REST_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: "No mapping for Iceberg REST port".to_string(),
            })?;

        let catalog_url = format!("http://localhost:{mapped_port}");
        info!("Iceberg REST catalog available at {catalog_url}");

        Ok(Self {
            container,
            catalog_url,
        })
    }
}

pub struct IcebergFixture {
    #[allow(dead_code)]
    minio: MinioContainer,
    #[allow(dead_code)]
    iceberg_rest: IcebergRestContainer,
    http_client: HttpClient,
    pub catalog_url: String,
    pub minio_endpoint: String,
}

impl IcebergFixture {
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
                fixture_type: "IcebergFixture".to_string(),
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

        info!("Created MinIO bucket: {MINIO_BUCKET}");
        Ok(())
    }
}

pub trait IcebergOps: Sync {
    fn catalog_url(&self) -> &str;
    fn http_client(&self) -> &HttpClient;

    fn create_namespace(
        &self,
        namespace: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/v1/namespaces", self.catalog_url());
            let body = serde_json::json!({
                "namespace": [namespace]
            });

            let response = self
                .http_client()
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: "IcebergOps".to_string(),
                    message: format!("Failed to create namespace: {error}"),
                })?;

            if !response.status().is_success() && response.status().as_u16() != 409 {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "IcebergOps".to_string(),
                    message: format!("Failed to create namespace: status={status}, body={body}"),
                });
            }

            info!("Created Iceberg namespace: {namespace}");
            Ok(())
        }
    }

    fn create_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/v1/namespaces/{namespace}/tables", self.catalog_url());
            let body = serde_json::json!({
                "name": table,
                "schema": {
                    "type": "struct",
                    "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": true},
                        {"id": 2, "name": "name", "type": "string", "required": true},
                        {"id": 3, "name": "count", "type": "int", "required": false},
                        {"id": 4, "name": "amount", "type": "double", "required": false},
                        {"id": 5, "name": "active", "type": "boolean", "required": false},
                        {"id": 6, "name": "timestamp", "type": "long", "required": false}
                    ]
                }
            });

            let response = self
                .http_client()
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: "IcebergOps".to_string(),
                    message: format!("Failed to create table: {error}"),
                })?;

            if !response.status().is_success() && response.status().as_u16() != 409 {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "IcebergOps".to_string(),
                    message: format!("Failed to create table: status={status}, body={body}"),
                });
            }

            info!("Created Iceberg table: {namespace}.{table}");
            Ok(())
        }
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> impl std::future::Future<Output = Result<TableMetadata, TestBinaryError>> + Send {
        async move {
            let url = format!(
                "{}/v1/namespaces/{namespace}/tables/{table}",
                self.catalog_url()
            );

            let response = self.http_client().get(&url).send().await.map_err(|error| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to get table metadata: {error}"),
                }
            })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to get table metadata: status={status}, body={body}"),
                });
            }

            response
                .json::<TableMetadata>()
                .await
                .map_err(|error| TestBinaryError::InvalidState {
                    message: format!("Failed to parse table metadata: {error}"),
                })
        }
    }

    fn snapshot_count(
        &self,
        namespace: &str,
        table: &str,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            let metadata = self.get_table_metadata(namespace, table).await?;
            Ok(metadata.metadata.snapshots.map(|s| s.len()).unwrap_or(0))
        }
    }

    fn wait_for_snapshots(
        &self,
        namespace: &str,
        table: &str,
        min_snapshots: usize,
        max_attempts: usize,
        interval_ms: u64,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            for _ in 0..max_attempts {
                let count = self.snapshot_count(namespace, table).await?;
                if count >= min_snapshots {
                    info!(
                        "Found {count} snapshots in table {namespace}.{table} (required: {min_snapshots})"
                    );
                    return Ok(count);
                }
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            }

            let final_count = self.snapshot_count(namespace, table).await?;
            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected at least {min_snapshots} snapshots in {namespace}.{table}, found {final_count} after {max_attempts} attempts"
                ),
            })
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct TableMetadata {
    pub metadata: IcebergMetadata,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct IcebergMetadata {
    #[serde(rename = "format-version")]
    pub format_version: i32,
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    pub location: String,
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Option<Vec<IcebergSnapshot>>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct IcebergSnapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    pub summary: Option<SnapshotSummary>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct SnapshotSummary {
    pub operation: String,
    #[serde(rename = "added-data-files")]
    pub added_data_files: Option<String>,
    #[serde(rename = "added-records")]
    pub added_records: Option<String>,
}

impl IcebergOps for IcebergFixture {
    fn catalog_url(&self) -> &str {
        &self.catalog_url
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

#[async_trait]
impl TestFixture for IcebergFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let id = Uuid::new_v4();
        let network = format!("iggy-iceberg-{id}");
        let minio_name = format!("minio-{id}");

        let minio = MinioContainer::start(&network, &minio_name).await?;

        Self::create_bucket(&minio.endpoint).await?;

        let iceberg_rest = IcebergRestContainer::start(&network, &minio.internal_endpoint).await?;

        let http_client = create_http_client();

        Ok(Self {
            catalog_url: iceberg_rest.catalog_url.clone(),
            minio_endpoint: minio.endpoint.clone(),
            minio,
            iceberg_rest,
            http_client,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_URI.to_string(), self.catalog_url.clone());
        envs.insert(ENV_SINK_WAREHOUSE.to_string(), MINIO_BUCKET.to_string());
        envs.insert(ENV_SINK_STORE_URL.to_string(), self.minio_endpoint.clone());
        envs.insert(
            ENV_SINK_STORE_ACCESS_KEY.to_string(),
            MINIO_ACCESS_KEY.to_string(),
        );
        envs.insert(
            ENV_SINK_STORE_SECRET.to_string(),
            MINIO_SECRET_KEY.to_string(),
        );
        envs.insert(ENV_SINK_STORE_REGION.to_string(), "us-east-1".to_string());
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_iceberg_sink".to_string(),
        );
        envs
    }
}

fn create_http_client() -> HttpClient {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    reqwest_middleware::ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

pub const DEFAULT_NAMESPACE: &str = "test";
pub const DEFAULT_TABLE: &str = "messages";

pub struct IcebergPreCreatedFixture {
    inner: IcebergFixture,
}

impl std::ops::Deref for IcebergPreCreatedFixture {
    type Target = IcebergFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl IcebergOps for IcebergPreCreatedFixture {
    fn catalog_url(&self) -> &str {
        &self.inner.catalog_url
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for IcebergPreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = IcebergFixture::setup().await?;

        inner.create_namespace(DEFAULT_NAMESPACE).await?;
        inner.create_table(DEFAULT_NAMESPACE, DEFAULT_TABLE).await?;

        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
