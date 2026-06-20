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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_BATCH_SIZE, ENV_SOURCE_INDEX,
    ENV_SOURCE_PATH, ENV_SOURCE_POLLING_INTERVAL, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TIMESTAMP_FIELD,
    ENV_SOURCE_URL, OpenSearchContainer, OpenSearchOps, create_http_client,
};
use async_trait::async_trait;
use iggy_common::IggyTimestamp;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use uuid::Uuid;

const TEST_INDEX_PREFIX: &str = "test_documents";

/// OpenSearch source fixture for basic document polling.
pub struct OpenSearchSourceFixture {
    container: OpenSearchContainer,
    http_client: HttpClient,
    // Unique per fixture so parallel tests never collide on the same index.
    index: String,
}

impl OpenSearchOps for OpenSearchSourceFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl OpenSearchSourceFixture {
    #[allow(dead_code)]
    pub fn index_name(&self) -> &str {
        &self.index
    }

    pub async fn setup_index(&self) -> Result<(), TestBinaryError> {
        self.create_index(&self.index).await
    }

    pub async fn insert_document(
        &self,
        doc_id: i32,
        name: &str,
        value: i32,
    ) -> Result<(), TestBinaryError> {
        let timestamp = IggyTimestamp::from(
            IggyTimestamp::now().as_micros() + u64::from(doc_id as u32) * 1_000,
        )
        .to_rfc3339_string();
        let document = serde_json::json!({
            "id": doc_id,
            "name": name,
            "value": value,
            "timestamp": timestamp
        });
        self.index_document(&self.index, &doc_id.to_string(), &document)
            .await
    }

    pub async fn insert_documents(&self, count: usize) -> Result<(), TestBinaryError> {
        for i in 1..=count {
            self.insert_document(i as i32, &format!("doc_{i}"), (i * 10) as i32)
                .await?;
        }
        self.refresh_index().await?;
        Ok(())
    }

    pub async fn get_document_count(&self) -> Result<usize, TestBinaryError> {
        self.count_documents(&self.index).await
    }

    pub async fn refresh_index(&self) -> Result<(), TestBinaryError> {
        OpenSearchOps::refresh_index(self, &self.index).await
    }

    pub async fn insert_typed_sample_document(&self) -> Result<(), TestBinaryError> {
        let timestamp = IggyTimestamp::now().to_rfc3339_string();
        let document = serde_json::json!({
            "id": 1,
            "title": "OpenSearch typed field coverage",
            "status": "active",
            "count": 9_223_372_036_854_775_807_i64,
            "score": 98.6_f32,
            "ratio": 0.125_f64,
            "active": true,
            "timestamp": timestamp,
            "client_ip": "192.168.1.42",
            "location": { "lat": 40.12, "lon": -71.34 },
            "tags": ["integration", "opensearch"],
            "optional_note": null
        });
        self.index_document(&self.index, "typed-1", &document)
            .await?;
        self.refresh_index().await
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = OpenSearchContainer::start().await?;
        let http_client = create_http_client();
        let index = format!("{TEST_INDEX_PREFIX}_{}", Uuid::new_v4().simple());

        // Container startup already waits for /_cluster/health to return 200
        // via HttpWaitStrategy, so no additional health check is needed.
        Ok(Self {
            container,
            http_client,
            index,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SOURCE_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SOURCE_INDEX.to_string(), self.index.clone());
        envs.insert(ENV_SOURCE_POLLING_INTERVAL.to_string(), "100ms".to_string());
        envs.insert(ENV_SOURCE_BATCH_SIZE.to_string(), "100".to_string());
        envs.insert(
            ENV_SOURCE_TIMESTAMP_FIELD.to_string(),
            "timestamp".to_string(),
        );
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
            "../../target/debug/libiggy_connector_opensearch_source".to_string(),
        );
        envs
    }
}

/// OpenSearch source fixture with typed-field index mapping.
pub struct OpenSearchSourceTypedFieldsFixture {
    inner: OpenSearchSourceFixture,
}

impl std::ops::Deref for OpenSearchSourceTypedFieldsFixture {
    type Target = OpenSearchSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl OpenSearchOps for OpenSearchSourceTypedFieldsFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.inner.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourceTypedFieldsFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = OpenSearchSourceFixture::setup().await?;
        inner.create_typed_fields_index(&inner.index).await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// OpenSearch source fixture with pre-created index.
pub struct OpenSearchSourcePreCreatedFixture {
    inner: OpenSearchSourceFixture,
}

impl std::ops::Deref for OpenSearchSourcePreCreatedFixture {
    type Target = OpenSearchSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl OpenSearchOps for OpenSearchSourcePreCreatedFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.inner.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourcePreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = OpenSearchSourceFixture::setup().await?;

        inner.setup_index().await?;

        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// OpenSearch source fixture with pre-created index and a small `batch_size` for
/// pagination integration tests.
pub struct OpenSearchSourceSmallBatchFixture {
    inner: OpenSearchSourceFixture,
}

impl std::ops::Deref for OpenSearchSourceSmallBatchFixture {
    type Target = OpenSearchSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl OpenSearchOps for OpenSearchSourceSmallBatchFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.inner.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for OpenSearchSourceSmallBatchFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = OpenSearchSourceFixture::setup().await?;
        inner.setup_index().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SOURCE_BATCH_SIZE.to_string(), "2".to_string());
        envs
    }
}

/// OpenSearch source fixture pointing at an index that is never created,
/// for exercising the connector's "missing index" failure path.
pub struct OpenSearchSourceMissingIndexFixture {
    inner: OpenSearchSourceFixture,
}

#[async_trait]
impl TestFixture for OpenSearchSourceMissingIndexFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = OpenSearchSourceFixture::setup().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
