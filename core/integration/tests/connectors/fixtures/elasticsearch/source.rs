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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_BATCH_SIZE, ENV_SOURCE_INDEX,
    ENV_SOURCE_PATH, ENV_SOURCE_POLLING_INTERVAL, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TIMESTAMP_FIELD,
    ENV_SOURCE_URL, ElasticsearchContainer, ElasticsearchOps, HEALTH_CHECK_ATTEMPTS,
    HEALTH_CHECK_INTERVAL_MS, create_http_client,
};
use async_trait::async_trait;
use iggy_common::IggyTimestamp;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const TEST_INDEX: &str = "test_documents";

/// Elasticsearch source fixture for basic document polling.
pub struct ElasticsearchSourceFixture {
    container: ElasticsearchContainer,
    http_client: HttpClient,
}

impl ElasticsearchOps for ElasticsearchSourceFixture {
    fn container(&self) -> &ElasticsearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl ElasticsearchSourceFixture {
    #[allow(dead_code)]
    pub fn index_name(&self) -> &str {
        TEST_INDEX
    }

    pub async fn setup_index(&self) -> Result<(), TestBinaryError> {
        self.create_index(TEST_INDEX).await
    }

    pub async fn insert_document(
        &self,
        doc_id: i32,
        name: &str,
        value: i32,
    ) -> Result<(), TestBinaryError> {
        let timestamp = IggyTimestamp::now().to_rfc3339_string();
        let document = serde_json::json!({
            "id": doc_id,
            "name": name,
            "value": value,
            "timestamp": timestamp
        });
        self.index_document(TEST_INDEX, &doc_id.to_string(), &document)
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
        self.count_documents(TEST_INDEX).await
    }

    pub async fn refresh_index(&self) -> Result<(), TestBinaryError> {
        ElasticsearchOps::refresh_index(self, TEST_INDEX).await
    }
}

#[async_trait]
impl TestFixture for ElasticsearchSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ElasticsearchContainer::start().await?;
        let http_client = create_http_client();

        let fixture = Self {
            container,
            http_client,
        };

        for _ in 0..HEALTH_CHECK_ATTEMPTS {
            let url = format!("{}/_cluster/health", fixture.container.base_url);
            if let Ok(response) = fixture.http_client.get(&url).send().await
                && response.status().is_success()
            {
                info!("Elasticsearch cluster is healthy");
                return Ok(fixture);
            }
            sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "ElasticsearchSource".to_string(),
            message: format!(
                "Failed to confirm Elasticsearch cluster health after {} attempts",
                HEALTH_CHECK_ATTEMPTS
            ),
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SOURCE_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SOURCE_INDEX.to_string(), TEST_INDEX.to_string());
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
            "../../target/debug/libiggy_connector_elasticsearch_source".to_string(),
        );
        envs
    }
}

/// Elasticsearch source fixture with pre-created index.
pub struct ElasticsearchSourcePreCreatedFixture {
    inner: ElasticsearchSourceFixture,
}

impl std::ops::Deref for ElasticsearchSourcePreCreatedFixture {
    type Target = ElasticsearchSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ElasticsearchOps for ElasticsearchSourcePreCreatedFixture {
    fn container(&self) -> &ElasticsearchContainer {
        &self.inner.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for ElasticsearchSourcePreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = ElasticsearchSourceFixture::setup().await?;

        inner.setup_index().await?;

        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
