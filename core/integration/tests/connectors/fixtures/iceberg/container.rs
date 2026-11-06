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
use integration::harness::{SinglePortReserver, TestBinaryError, TestFixture, seeds};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;
use uuid::Uuid;

use crate::connectors::fixtures::{
    self,
    floci::{self, ACCESS_KEY, FlociContainer, REGION, SECRET_KEY},
};

const ICEBERG_REST_IMAGE: &str = "docker.io/apache/iceberg-rest-fixture";
const ICEBERG_REST_TAG: &str = "latest";
const ICEBERG_REST_PORT: u16 = 8181;

pub const TEST_BUCKET: &str = "warehouse";

pub const ENV_SINK_URI: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_URI";
pub const ENV_SINK_WAREHOUSE: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_WAREHOUSE";
pub const ENV_SINK_STORE_URL: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_URL";
pub const ENV_SINK_STORE_ACCESS_KEY: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_ACCESS_KEY_ID";
pub const ENV_SINK_STORE_SECRET: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_SECRET_ACCESS_KEY";
pub const ENV_SINK_STORE_REGION: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_STORE_REGION";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PATH";
pub const ENV_SINK_INVALID_ROWS: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_INVALID_ROWS";
pub const ENV_SINK_MAX_RETRIES: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_MAX_RETRIES";
pub const ENV_SINK_RETRY_DELAY: &str = "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_RETRY_DELAY";
pub const ENV_SINK_MAX_RETRY_DELAY: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_PLUGIN_CONFIG_MAX_RETRY_DELAY";
pub const ENV_SINK_DELIVERY_GUARANTEE: &str = "IGGY_CONNECTORS_SINK_ICEBERG_DELIVERY_GUARANTEE";
pub const ENV_SINK_DELIVERY_FLUSH_INTERVAL: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_DELIVERY_FLUSH_INTERVAL";
pub const ENV_SINK_DELIVERY_RETRY_INITIAL_BACKOFF: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_DELIVERY_RETRY_INITIAL_BACKOFF";
pub const ENV_SINK_DELIVERY_RETRY_MAX_BACKOFF: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_DELIVERY_RETRY_MAX_BACKOFF";
// Setting one field of a `[[streams]]` entry from the environment replaces the whole entry.
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_ICEBERG_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_ICEBERG_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_ICEBERG_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_BATCH_LENGTH: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_STREAMS_0_BATCH_LENGTH";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_ICEBERG_STREAMS_0_CONSUMER_GROUP";
/// The messages of one batch of `IcebergDeferredCheckpointFixture`.
pub const DEFERRED_CHECKPOINT_BATCH_LENGTH: u32 = 2;

pub const ENV_AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
pub const ENV_AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";

pub struct IcebergRestContainer {
    container: ContainerAsync<GenericImage>,
    // A fixed host port survives a restart, where Docker would map a new random one.
    _host_port: SinglePortReserver,
    pub catalog_url: String,
}

impl IcebergRestContainer {
    pub async fn start(
        network: &str,
        floci_internal_endpoint: &str,
    ) -> Result<Self, TestBinaryError> {
        let warehouse_path = format!("s3://{TEST_BUCKET}/");
        let host_port = SinglePortReserver::new()?;
        let mapped_port = host_port.address().port();

        let container = GenericImage::new(ICEBERG_REST_IMAGE, ICEBERG_REST_TAG)
            .with_exposed_port(ICEBERG_REST_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/v1/config")
                    .with_port(ICEBERG_REST_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_startup_timeout(std::time::Duration::from_secs(30))
            .with_network(network)
            .with_env_var(
                "CATALOG_CATALOG__IMPL",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .with_env_var("CATALOG_URI", "jdbc:sqlite:/tmp/iceberg_catalog.db")
            .with_env_var("CATALOG_WAREHOUSE", &warehouse_path)
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", floci_internal_endpoint)
            .with_env_var("CATALOG_S3_ACCESS__KEY__ID", ACCESS_KEY)
            .with_env_var("CATALOG_S3_SECRET__ACCESS__KEY", SECRET_KEY)
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .with_env_var("AWS_REGION", REGION)
            .with_env_var("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .with_mapped_port(mapped_port, ICEBERG_REST_PORT.tcp())
            .with_container_name(fixtures::unique_container_name("iceberg-rest"))
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        info!("Started Iceberg REST catalog container");

        let catalog_url = format!("http://localhost:{mapped_port}");
        info!("Iceberg REST catalog available at {catalog_url}");

        Ok(Self {
            container,
            _host_port: host_port,
            catalog_url,
        })
    }

    /// Kills the catalog, as a crash would. Its tables survive a later `start_again`.
    pub async fn stop(&self) -> Result<(), TestBinaryError> {
        self.container
            .stop_with_timeout(Some(0))
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: format!("Failed to stop container: {error}"),
            })
    }

    /// Starts a stopped catalog on the same host port, without waiting for it to serve.
    pub async fn start_again(&self) -> Result<(), TestBinaryError> {
        self.container
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "IcebergRestContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })
    }
}

pub struct IcebergFixture {
    #[allow(dead_code)]
    floci: FlociContainer,
    iceberg_rest: IcebergRestContainer,
    http_client: HttpClient,
    pub catalog_url: String,
    pub floci_endpoint: String,
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
        partition_spec: Option<serde_json::Value>,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/v1/namespaces/{namespace}/tables", self.catalog_url());
            let mut body = serde_json::json!({
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
            if let Some(partition_spec) = partition_spec {
                body["partition-spec"] = partition_spec;
            }

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
    #[serde(rename = "changed-partition-count")]
    pub changed_partition_count: Option<String>,
    /// The last message that an at-least-once append holds.
    #[serde(rename = "iggy.last-offset")]
    pub last_offset: Option<String>,
}

impl IcebergFixture {
    pub async fn stop_catalog(&self) -> Result<(), TestBinaryError> {
        self.iceberg_rest.stop().await
    }

    pub async fn start_catalog(&self) -> Result<(), TestBinaryError> {
        self.iceberg_rest.start_again().await
    }
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
        let floci_name = fixtures::unique_container_name("floci-iceberg");

        let floci = FlociContainer::start(Some(&network), &floci_name).await?;

        floci::create_bucket(&floci.endpoint, TEST_BUCKET).await?;

        let iceberg_rest = IcebergRestContainer::start(&network, &floci.internal_endpoint).await?;

        let http_client = create_http_client();

        Ok(Self {
            catalog_url: iceberg_rest.catalog_url.clone(),
            floci_endpoint: floci.endpoint.clone(),
            floci,
            iceberg_rest,
            http_client,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_URI.to_string(), self.catalog_url.clone());
        envs.insert(ENV_SINK_WAREHOUSE.to_string(), TEST_BUCKET.to_string());
        envs.insert(ENV_SINK_STORE_URL.to_string(), self.floci_endpoint.clone());
        envs.insert(
            ENV_SINK_STORE_ACCESS_KEY.to_string(),
            ACCESS_KEY.to_string(),
        );
        envs.insert(ENV_SINK_STORE_SECRET.to_string(), SECRET_KEY.to_string());
        envs.insert(ENV_SINK_STORE_REGION.to_string(), REGION.to_string());
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
/// Field id of the `active` column in the schema created by `IcebergOps::create_table`.
const ACTIVE_FIELD_ID: i32 = 5;

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

impl IcebergPreCreatedFixture {
    async fn setup_with_partition_spec(
        partition_spec: Option<serde_json::Value>,
    ) -> Result<Self, TestBinaryError> {
        let inner = IcebergFixture::setup().await?;

        inner.create_namespace(DEFAULT_NAMESPACE).await?;
        inner
            .create_table(DEFAULT_NAMESPACE, DEFAULT_TABLE, partition_spec)
            .await?;

        Ok(Self { inner })
    }
}

#[async_trait]
impl TestFixture for IcebergPreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup_with_partition_spec(None).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// Same table as `IcebergPreCreatedFixture`, with a sink that skips rows that do not match it.
pub struct IcebergSkipInvalidRowsFixture {
    inner: IcebergPreCreatedFixture,
}

impl IcebergOps for IcebergSkipInvalidRowsFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergSkipInvalidRowsFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: IcebergPreCreatedFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_INVALID_ROWS.to_string(), "skip".to_string());
        envs
    }
}

/// Same table as `IcebergPreCreatedFixture`, with commit retries that outlast a catalog restart.
pub struct IcebergCatalogRestartFixture {
    inner: IcebergPreCreatedFixture,
}

impl std::ops::Deref for IcebergCatalogRestartFixture {
    type Target = IcebergFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl IcebergOps for IcebergCatalogRestartFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergCatalogRestartFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: IcebergPreCreatedFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "30".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "200ms".to_string());
        envs.insert(ENV_SINK_MAX_RETRY_DELAY.to_string(), "1s".to_string());
        envs
    }
}

/// Same table as `IcebergPreCreatedFixture`, with a sink that delivers at least once. Its own
/// commit retries run out within a second, so a longer catalog outage reaches the runtime, which
/// hands the batch over again.
pub struct IcebergAtLeastOnceFixture {
    inner: IcebergPreCreatedFixture,
}

impl std::ops::Deref for IcebergAtLeastOnceFixture {
    type Target = IcebergFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl IcebergOps for IcebergAtLeastOnceFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergAtLeastOnceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: IcebergPreCreatedFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SINK_DELIVERY_GUARANTEE.to_string(),
            "at_least_once".to_string(),
        );
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "2".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "200ms".to_string());
        envs.insert(ENV_SINK_MAX_RETRY_DELAY.to_string(), "200ms".to_string());
        envs.insert(
            ENV_SINK_DELIVERY_RETRY_INITIAL_BACKOFF.to_string(),
            "200ms".to_string(),
        );
        envs.insert(
            ENV_SINK_DELIVERY_RETRY_MAX_BACKOFF.to_string(),
            "1s".to_string(),
        );
        envs
    }
}

/// Same table as `IcebergPreCreatedFixture`, with a sink that delivers at least once and stores
/// no offset within a test, so every message it committed is above the stored offset. Its
/// batches hold [`DEFERRED_CHECKPOINT_BATCH_LENGTH`] messages, so that a few messages already
/// make a backlog of several batches.
pub struct IcebergDeferredCheckpointFixture {
    inner: IcebergPreCreatedFixture,
}

impl std::ops::Deref for IcebergDeferredCheckpointFixture {
    type Target = IcebergFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl IcebergOps for IcebergDeferredCheckpointFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergDeferredCheckpointFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: IcebergPreCreatedFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SINK_DELIVERY_GUARANTEE.to_string(),
            "at_least_once".to_string(),
        );
        envs.insert(
            ENV_SINK_DELIVERY_FLUSH_INTERVAL.to_string(),
            "1h".to_string(),
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
        envs.insert(
            ENV_SINK_STREAMS_0_BATCH_LENGTH.to_string(),
            DEFERRED_CHECKPOINT_BATCH_LENGTH.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "iceberg_sink_connector".to_string(),
        );
        envs
    }
}

/// Same table as `IcebergPreCreatedFixture`, partitioned by identity(`active`).
pub struct IcebergPartitionedTableFixture {
    inner: IcebergPreCreatedFixture,
}

impl IcebergOps for IcebergPartitionedTableFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergPartitionedTableFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let partition_spec = serde_json::json!({
            "spec-id": 0,
            "fields": [{
                "source-id": ACTIVE_FIELD_ID,
                "field-id": 1000,
                "name": "active",
                "transform": "identity"
            }]
        });
        let inner =
            IcebergPreCreatedFixture::setup_with_partition_spec(Some(partition_spec)).await?;

        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct IcebergEnvAuthFixture {
    inner: IcebergPreCreatedFixture,
}

impl IcebergOps for IcebergEnvAuthFixture {
    fn catalog_url(&self) -> &str {
        self.inner.catalog_url()
    }

    fn http_client(&self) -> &HttpClient {
        self.inner.http_client()
    }
}

#[async_trait]
impl TestFixture for IcebergEnvAuthFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = IcebergPreCreatedFixture::setup().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        // Remove the explicit credentials injected by the underlying fixture.
        // This forces the Iceberg Sink to use the default credential provider chain instead of explicit config.
        envs.remove(ENV_SINK_STORE_ACCESS_KEY);
        envs.remove(ENV_SINK_STORE_SECRET);
        // Inject standard AWS env vars to test the default credential provider chain.
        envs.insert(ENV_AWS_ACCESS_KEY_ID.to_string(), ACCESS_KEY.to_string());
        envs.insert(
            ENV_AWS_SECRET_ACCESS_KEY.to_string(),
            SECRET_KEY.to_string(),
        );
        envs
    }
}
