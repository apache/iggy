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
    DEFAULT_SOURCE_COLLECTION, DEFAULT_SOURCE_DATABASE, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC,
    ENV_SOURCE_COLLECTION, ENV_SOURCE_CONNECTION_URI, ENV_SOURCE_DATABASE, ENV_SOURCE_LIMIT,
    ENV_SOURCE_PATH, ENV_SOURCE_POLLING_INTERVAL, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TIMESTAMP_FIELD,
    MongoDbContainer, MongoDbOps,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use mongodb::bson::{DateTime as BsonDateTime, Document, doc};
use std::collections::HashMap;

/// MongoDB source fixture for basic document polling.
pub struct MongodbSourceFixture {
    container: MongoDbContainer,
}

impl MongoDbOps for MongodbSourceFixture {
    fn container(&self) -> &MongoDbContainer {
        &self.container
    }
}

impl MongodbSourceFixture {
    #[allow(dead_code)]
    pub fn database_name(&self) -> &str {
        DEFAULT_SOURCE_DATABASE
    }

    #[allow(dead_code)]
    pub fn collection_name(&self) -> &str {
        DEFAULT_SOURCE_COLLECTION
    }

    pub async fn insert_document_at(
        &self,
        doc_id: i32,
        name: &str,
        value: i32,
        timestamp_millis: i64,
    ) -> Result<(), TestBinaryError> {
        let client = self.create_client().await?;
        let coll = client
            .database(DEFAULT_SOURCE_DATABASE)
            .collection::<Document>(DEFAULT_SOURCE_COLLECTION);
        coll.insert_one(doc! {
            "id": doc_id,
            "name": name,
            "value": value,
            "timestamp": BsonDateTime::from_millis(timestamp_millis),
        })
        .await
        .map(|_| ())
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to insert document: {e}"),
        })
    }

    pub async fn insert_documents(&self, count: usize) -> Result<(), TestBinaryError> {
        // Use a fixed base timestamp + monotonic offsets so the source's
        // `sort({ timestamp: 1 })` yields deterministic order.
        let base = BsonDateTime::now().timestamp_millis();
        for i in 1..=count {
            self.insert_document_at(
                i as i32,
                &format!("doc_{i}"),
                (i * 10) as i32,
                base + i as i64,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn get_document_count(&self) -> Result<usize, TestBinaryError> {
        let client = self.create_client().await?;
        let coll = client
            .database(DEFAULT_SOURCE_DATABASE)
            .collection::<Document>(DEFAULT_SOURCE_COLLECTION);
        coll.count_documents(doc! {})
            .await
            .map(|c| c as usize)
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to count documents: {e}"),
            })
    }

    async fn create_collection(&self) -> Result<(), TestBinaryError> {
        let client = self.create_client().await?;
        let db = client.database(DEFAULT_SOURCE_DATABASE);
        let names =
            db.list_collection_names()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "MongodbSourceFixture".to_string(),
                    message: format!("Failed to list collections: {e}"),
                })?;
        if !names.contains(&DEFAULT_SOURCE_COLLECTION.to_string()) {
            db.create_collection(DEFAULT_SOURCE_COLLECTION)
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "MongodbSourceFixture".to_string(),
                    message: format!("Failed to create collection: {e}"),
                })?;
        }
        Ok(())
    }
}

#[async_trait]
impl TestFixture for MongodbSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MongoDbContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SOURCE_CONNECTION_URI.to_string(),
            self.container.connection_uri.clone(),
        );
        envs.insert(
            ENV_SOURCE_DATABASE.to_string(),
            DEFAULT_SOURCE_DATABASE.to_string(),
        );
        envs.insert(
            ENV_SOURCE_COLLECTION.to_string(),
            DEFAULT_SOURCE_COLLECTION.to_string(),
        );
        envs.insert(ENV_SOURCE_POLLING_INTERVAL.to_string(), "100ms".to_string());
        envs.insert(
            ENV_SOURCE_TIMESTAMP_FIELD.to_string(),
            "timestamp".to_string(),
        );
        envs.insert(ENV_SOURCE_LIMIT.to_string(), "100".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_mongodb_source".to_string(),
        );
        envs
    }
}

/// MongoDB source fixture with pre-created collection.
pub struct MongodbSourcePreCreatedFixture {
    inner: MongodbSourceFixture,
}

impl std::ops::Deref for MongodbSourcePreCreatedFixture {
    type Target = MongodbSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl MongoDbOps for MongodbSourcePreCreatedFixture {
    fn container(&self) -> &MongoDbContainer {
        &self.inner.container
    }
}

#[async_trait]
impl TestFixture for MongodbSourcePreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = MongodbSourceFixture::setup().await?;
        inner.create_collection().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
