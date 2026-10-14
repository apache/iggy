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

use crate::connectors::fixtures;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use integration::harness::{TestBinaryError, TestFixture, seeds};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const DYNAMODB_IMAGE: &str = "amazon/dynamodb-local";
const DYNAMODB_TAG: &str = "2.5.2";
const DYNAMODB_PORT: u16 = 8000;
const DYNAMODB_READY_LOG: &str = "Initializing DynamoDB Local";
const DYNAMODB_REGION: &str = "us-east-1";
const DYNAMODB_ACCESS_KEY: &str = "test";
const DYNAMODB_SECRET_KEY: &str = "test";
const CREDENTIALS_PROVIDER_NAME: &str = "iggy-dynamodb-test";
pub const TEST_TABLE: &str = "iggy_messages";
const PARTITION_KEY: &str = "iggy_id";
const SORT_KEY: &str = "iggy_offset";
/// Bounds the wait for the DynamoDB Local API to answer after its startup log.
const TABLE_CREATE_ATTEMPTS: usize = 30;
const TABLE_CREATE_RETRY_DELAY: Duration = Duration::from_secs(1);
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PATH";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_STREAMS_0_SCHEMA";
const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_TABLE";
const ENV_SINK_REGION: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_REGION";
const ENV_SINK_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_ENDPOINT";
const ENV_SINK_ACCESS_KEY_ID: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_ACCESS_KEY_ID";
const ENV_SINK_SECRET_ACCESS_KEY: &str =
    "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_SECRET_ACCESS_KEY";
const ENV_SINK_SORT_KEY_FIELD: &str = "IGGY_CONNECTORS_SINK_DYNAMODB_PLUGIN_CONFIG_SORT_KEY_FIELD";

pub trait DynamoDbOps: Sync {
    fn client(&self) -> &Client;

    fn scan_items(
        &self,
    ) -> impl std::future::Future<
        Output = Result<Vec<HashMap<String, AttributeValue>>, TestBinaryError>,
    > + Send {
        async move {
            let output = self
                .client()
                .scan()
                .table_name(TEST_TABLE)
                .send()
                .await
                .map_err(|error| TestBinaryError::InvalidState {
                    message: format!("Failed to scan DynamoDB table: {error}"),
                })?;
            Ok(output.items.unwrap_or_default())
        }
    }

    fn wait_for_items(
        &self,
        expected_count: usize,
    ) -> impl std::future::Future<
        Output = Result<Vec<HashMap<String, AttributeValue>>, TestBinaryError>,
    > + Send {
        async move {
            let mut last_count = 0usize;
            for _ in 0..POLL_ATTEMPTS {
                if let Ok(items) = self.scan_items().await {
                    last_count = items.len();
                    if last_count >= expected_count {
                        return Ok(items);
                    }
                }
                sleep(POLL_INTERVAL).await;
            }

            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected {expected_count} DynamoDB items, found {last_count} after {POLL_ATTEMPTS} attempts"
                ),
            })
        }
    }
}

pub struct DynamoDbSinkFixture {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    client: Client,
    endpoint: String,
}

impl DynamoDbOps for DynamoDbSinkFixture {
    fn client(&self) -> &Client {
        &self.client
    }
}

impl DynamoDbSinkFixture {
    async fn start(fixture_type: &str, with_sort_key: bool) -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(DYNAMODB_IMAGE, DYNAMODB_TAG)
            .with_exposed_port(DYNAMODB_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(DYNAMODB_READY_LOG))
            .with_container_name(fixtures::unique_container_name("dynamodb"))
            .with_mapped_port(0, DYNAMODB_PORT.tcp())
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: fixture_type.to_string(),
                message: format!("Failed to start DynamoDB Local container: {error}"),
            })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: fixture_type.to_string(),
                message: format!("Failed to get ports: {error}"),
            })?
            .map_to_host_port_ipv4(DYNAMODB_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: fixture_type.to_string(),
                message: "No mapping for DynamoDB port".to_string(),
            })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        info!("DynamoDB Local container available at {endpoint}");

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(DYNAMODB_REGION))
            .endpoint_url(&endpoint)
            .credentials_provider(Credentials::new(
                DYNAMODB_ACCESS_KEY,
                DYNAMODB_SECRET_KEY,
                None,
                None,
                CREDENTIALS_PROVIDER_NAME,
            ))
            .load()
            .await;
        let client = Client::new(&config);

        create_table(&client, fixture_type, with_sort_key).await?;

        Ok(Self {
            container,
            client,
            endpoint,
        })
    }

    fn base_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_dynamodb_sink".to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{}]", seeds::names::TOPIC),
            ),
            (ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (ENV_SINK_TABLE.to_string(), TEST_TABLE.to_string()),
            (ENV_SINK_REGION.to_string(), DYNAMODB_REGION.to_string()),
            (ENV_SINK_ENDPOINT.to_string(), self.endpoint.clone()),
            (
                ENV_SINK_ACCESS_KEY_ID.to_string(),
                DYNAMODB_ACCESS_KEY.to_string(),
            ),
            (
                ENV_SINK_SECRET_ACCESS_KEY.to_string(),
                DYNAMODB_SECRET_KEY.to_string(),
            ),
        ])
    }
}

#[async_trait]
impl TestFixture for DynamoDbSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::start("DynamoDbSinkFixture", false).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.base_envs()
    }
}

/// Same container, but the table carries a sort key so the sink has to fill it
/// from the message offset.
pub struct DynamoDbSinkSortKeyFixture {
    inner: DynamoDbSinkFixture,
}

impl DynamoDbOps for DynamoDbSinkSortKeyFixture {
    fn client(&self) -> &Client {
        self.inner.client()
    }
}

#[async_trait]
impl TestFixture for DynamoDbSinkSortKeyFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = DynamoDbSinkFixture::start("DynamoDbSinkSortKeyFixture", true).await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.base_envs();
        envs.insert(ENV_SINK_SORT_KEY_FIELD.to_string(), SORT_KEY.to_string());
        envs
    }
}

async fn create_table(
    client: &Client,
    fixture_type: &str,
    with_sort_key: bool,
) -> Result<(), TestBinaryError> {
    let partition_key_definition = AttributeDefinition::builder()
        .attribute_name(PARTITION_KEY)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: fixture_type.to_string(),
            message: format!("Failed to build attribute definition: {error}"),
        })?;
    let partition_key_schema = KeySchemaElement::builder()
        .attribute_name(PARTITION_KEY)
        .key_type(KeyType::Hash)
        .build()
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: fixture_type.to_string(),
            message: format!("Failed to build key schema: {error}"),
        })?;

    let mut last_error = String::new();
    for _ in 0..TABLE_CREATE_ATTEMPTS {
        let mut request = client
            .create_table()
            .table_name(TEST_TABLE)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(partition_key_definition.clone())
            .key_schema(partition_key_schema.clone());

        if with_sort_key {
            let sort_key_definition = AttributeDefinition::builder()
                .attribute_name(SORT_KEY)
                .attribute_type(ScalarAttributeType::N)
                .build()
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: fixture_type.to_string(),
                    message: format!("Failed to build attribute definition: {error}"),
                })?;
            let sort_key_schema = KeySchemaElement::builder()
                .attribute_name(SORT_KEY)
                .key_type(KeyType::Range)
                .build()
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: fixture_type.to_string(),
                    message: format!("Failed to build key schema: {error}"),
                })?;
            request = request
                .attribute_definitions(sort_key_definition)
                .key_schema(sort_key_schema);
        }

        match request.send().await {
            Ok(_) => {
                info!("DynamoDB table '{TEST_TABLE}' created");
                return Ok(());
            }
            Err(error) => {
                last_error = error.to_string();
                sleep(TABLE_CREATE_RETRY_DELAY).await;
            }
        }
    }

    Err(TestBinaryError::FixtureSetup {
        fixture_type: fixture_type.to_string(),
        message: format!(
            "Table '{TEST_TABLE}' not creatable after {TABLE_CREATE_ATTEMPTS} attempts, last error: {last_error}"
        ),
    })
}
