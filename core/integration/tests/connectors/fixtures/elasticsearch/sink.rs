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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_INDEX, ENV_SINK_PATH,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_URL, ElasticsearchContainer, ElasticsearchOps,
    ElasticsearchSearchResponse, HEALTH_CHECK_ATTEMPTS, HEALTH_CHECK_INTERVAL_MS,
    create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const SINK_INDEX: &str = "iggy_messages";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;

pub struct ElasticsearchSinkFixture {
    container: ElasticsearchContainer,
    http_client: HttpClient,
}

impl ElasticsearchOps for ElasticsearchSinkFixture {
    fn container(&self) -> &ElasticsearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl ElasticsearchSinkFixture {
    pub async fn get_document_count(&self) -> Result<usize, TestBinaryError> {
        self.count_documents(SINK_INDEX).await
    }

    pub async fn wait_for_documents(
        &self,
        expected_count: usize,
    ) -> Result<usize, TestBinaryError> {
        for _ in 0..POLL_ATTEMPTS {
            match self.count_documents(SINK_INDEX).await {
                Ok(count) if count >= expected_count => {
                    info!("Found {count} documents in Elasticsearch (expected {expected_count})");
                    return Ok(count);
                }
                Ok(_) => {}
                Err(_) => {}
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        let final_count = self.count_documents(SINK_INDEX).await.unwrap_or(0);
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected_count} documents, found {final_count} after {POLL_ATTEMPTS} attempts"
            ),
        })
    }

    pub async fn search_documents(&self) -> Result<ElasticsearchSearchResponse, TestBinaryError> {
        self.search_all(SINK_INDEX).await
    }

    pub async fn refresh_index(&self) -> Result<(), TestBinaryError> {
        ElasticsearchOps::refresh_index(self, SINK_INDEX).await
    }
}

#[async_trait]
impl TestFixture for ElasticsearchSinkFixture {
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
            fixture_type: "ElasticsearchSink".to_string(),
            message: format!(
                "Failed to confirm Elasticsearch cluster health after {} attempts",
                HEALTH_CHECK_ATTEMPTS
            ),
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SINK_INDEX.to_string(), SINK_INDEX.to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "elasticsearch_sink_cg".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_elasticsearch_sink".to_string(),
        );
        envs
    }
}
