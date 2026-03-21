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

use super::container::{
    DEFAULT_POLL_ATTEMPTS, DEFAULT_POLL_INTERVAL_MS, DEFAULT_SINK_COLLECTION,
    DEFAULT_TEST_DATABASE, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC,
    ENV_SINK_AUTO_CREATE_COLLECTION, ENV_SINK_BATCH_SIZE, ENV_SINK_COLLECTION,
    ENV_SINK_CONNECTION_URI, ENV_SINK_DATABASE, ENV_SINK_INCLUDE_METADATA, ENV_SINK_MAX_RETRIES,
    ENV_SINK_PATH, ENV_SINK_PAYLOAD_FORMAT, ENV_SINK_RETRY_DELAY,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, MongoDbContainer, MongoDbOps,
};
use async_trait::async_trait;
use futures::TryStreamExt;
use integration::harness::{TestBinaryError, TestFixture};
use mongodb::{Client, bson::Document};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

/// MongoDB sink connector fixture with binary payload format (default).
pub struct MongoDbSinkFixture {
    container: MongoDbContainer,
    payload_format: &'static str,
    auto_create: bool,
}

impl MongoDbOps for MongoDbSinkFixture {
    fn container(&self) -> &MongoDbContainer {
        &self.container
    }
}

impl MongoDbSinkFixture {
    /// Wait for documents to appear in a MongoDB collection, polling until expected count reached.
    ///
    /// Returns an error if the expected count is not reached within the poll attempts.
    pub async fn wait_for_documents(
        &self,
        client: &Client,
        collection_name: &str,
        expected: usize,
    ) -> Result<Vec<Document>, TestBinaryError> {
        let db = client.database(DEFAULT_TEST_DATABASE);
        let collection = db.collection::<Document>(collection_name);

        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            let cursor = collection
                .find(mongodb::bson::doc! {})
                .sort(mongodb::bson::doc! { "iggy_offset": 1 })
                .await;

            if let Ok(c) = cursor
                && let Ok(docs) = c.try_collect::<Vec<_>>().await
                && docs.len() >= expected
            {
                info!(
                    "Found {} documents in MongoDB collection '{collection_name}'",
                    docs.len()
                );
                return Ok(docs);
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected} documents in '{collection_name}' after {} attempts",
                DEFAULT_POLL_ATTEMPTS
            ),
        })
    }

    /// Count all documents in a collection.
    pub async fn count_documents_in_collection(
        &self,
        client: &Client,
        collection_name: &str,
    ) -> Result<u64, TestBinaryError> {
        let db = client.database(DEFAULT_TEST_DATABASE);
        let collection = db.collection::<Document>(collection_name);
        collection
            .count_documents(mongodb::bson::doc! {})
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to count documents: {e}"),
            })
    }

    /// Check whether a named collection exists in the test database.
    pub async fn collection_exists(
        &self,
        client: &Client,
        collection_name: &str,
    ) -> Result<bool, TestBinaryError> {
        let db = client.database(DEFAULT_TEST_DATABASE);
        let names =
            db.list_collection_names()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to list collections: {e}"),
                })?;
        Ok(names.contains(&collection_name.to_string()))
    }

    /// Enable `failCommand` failpoint once with caller-supplied failpoint data.
    pub async fn configure_fail_command_once(
        &self,
        client: &Client,
        data: Document,
    ) -> Result<(), TestBinaryError> {
        client
            .database("admin")
            .run_command(mongodb::bson::doc! {
                "configureFailPoint": "failCommand",
                "mode": { "times": 1 },
                "data": data,
            })
            .await
            .map(|_| ())
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbSinkFixture".to_string(),
                message: format!("Failed to configure failCommand failpoint: {e}"),
            })
    }

    /// Disable `failCommand` failpoint explicitly.
    pub async fn disable_fail_command(&self, client: &Client) -> Result<(), TestBinaryError> {
        client
            .database("admin")
            .run_command(mongodb::bson::doc! {
                "configureFailPoint": "failCommand",
                "mode": "off",
            })
            .await
            .map(|_| ())
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MongoDbSinkFixture".to_string(),
                message: format!("Failed to disable failCommand failpoint: {e}"),
            })
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start().await?;
        Ok(Self {
            container,
            payload_format: "binary",
            auto_create: false,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_CONNECTION_URI.to_string(),
            self.container.connection_uri.clone(),
        );
        envs.insert(
            ENV_SINK_DATABASE.to_string(),
            DEFAULT_TEST_DATABASE.to_string(),
        );
        envs.insert(
            ENV_SINK_COLLECTION.to_string(),
            DEFAULT_SINK_COLLECTION.to_string(),
        );
        envs.insert(
            ENV_SINK_PAYLOAD_FORMAT.to_string(),
            self.payload_format.to_string(),
        );
        envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "raw".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "mongodb_sink_cg".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_mongodb_sink".to_string(),
        );
        if self.auto_create {
            envs.insert(
                ENV_SINK_AUTO_CREATE_COLLECTION.to_string(),
                "true".to_string(),
            );
        }
        envs
    }
}

/// MongoDB sink fixture with JSON payload format.
pub struct MongoDbSinkJsonFixture {
    inner: MongoDbSinkFixture,
}

impl std::ops::Deref for MongoDbSinkJsonFixture {
    type Target = MongoDbSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkJsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start().await?;
        Ok(Self {
            inner: MongoDbSinkFixture {
                container,
                payload_format: "json",
                auto_create: false,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        // Schema must be "json" for the runtime to route messages correctly.
        // Already set in base, but override STREAMS_0_SCHEMA explicitly.
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs
    }
}

/// MongoDB sink fixture with auto_create_collection enabled.
pub struct MongoDbSinkAutoCreateFixture {
    inner: MongoDbSinkFixture,
}

impl std::ops::Deref for MongoDbSinkAutoCreateFixture {
    type Target = MongoDbSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkAutoCreateFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start().await?;
        Ok(Self {
            inner: MongoDbSinkFixture {
                container,
                payload_format: "binary",
                auto_create: true,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        // auto_create flag is set in the base connectors_runtime_envs()
        self.inner.connectors_runtime_envs()
    }
}

/// MongoDB sink fixture with batch_size set to 10 for large-batch tests.
pub struct MongoDbSinkBatchFixture {
    inner: MongoDbSinkFixture,
}

impl std::ops::Deref for MongoDbSinkBatchFixture {
    type Target = MongoDbSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkBatchFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start().await?;
        Ok(Self {
            inner: MongoDbSinkFixture {
                container,
                payload_format: "binary",
                auto_create: false,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_BATCH_SIZE.to_string(), "10".to_string());
        envs
    }
}

/// MongoDB sink fixture backed by single-node replica set and write concern timeout URI.
pub struct MongoDbSinkWriteConcernFixture {
    inner: MongoDbSinkFixture,
    connection_uri: String,
}

impl std::ops::Deref for MongoDbSinkWriteConcernFixture {
    type Target = MongoDbSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkWriteConcernFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start_single_node_replica_set(false).await?;
        let connection_uri = format!(
            "{}&w=2&wtimeoutMS=200&retryWrites=false",
            container.connection_uri
        );
        Ok(Self {
            inner: MongoDbSinkFixture {
                container,
                payload_format: "binary",
                auto_create: false,
            },
            connection_uri,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SINK_CONNECTION_URI.to_string(),
            self.connection_uri.clone(),
        );
        // Keep the signal clean: fail once and report failure instead of retrying.
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "1".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "50ms".to_string());
        envs
    }
}

/// MongoDB sink fixture backed by single-node replica set with test commands enabled.
pub struct MongoDbSinkFailpointFixture {
    inner: MongoDbSinkFixture,
}

impl std::ops::Deref for MongoDbSinkFailpointFixture {
    type Target = MongoDbSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MongoDbSinkFailpointFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start_single_node_replica_set(true).await?;
        Ok(Self {
            inner: MongoDbSinkFixture {
                container,
                payload_format: "binary",
                auto_create: false,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "2".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "50ms".to_string());
        envs
    }
}
