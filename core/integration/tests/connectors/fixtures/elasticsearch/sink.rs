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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_INDEX, ENV_SINK_PATH,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_URL, ElasticsearchContainer, ElasticsearchOps,
    ElasticsearchSearchResponse, create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const SINK_INDEX_PREFIX: &str = "iggy_messages";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const PROBE_REQUEST_TIMEOUT_MS: u64 = 2_000;

pub struct ElasticsearchSinkFixture {
    container: ElasticsearchContainer,
    http_client: HttpClient,
    // Unique per fixture so tests sharing one container never collide on the
    // same index. The connector writes here via ENV_SINK_INDEX.
    index: String,
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
        self.count_documents(&self.index).await
    }

    pub async fn wait_for_documents(
        &self,
        expected_count: usize,
    ) -> Result<usize, TestBinaryError> {
        // Built once, reused across every probe below: a fresh reqwest::Client
        // re-parses the system CA store via rustls-platform-verifier on every
        // build() (even for plain http) and loses keep-alive between probes.
        let client = probe_client()?;
        let mut last_error: Option<TestBinaryError> = None;

        for _ in 0..POLL_ATTEMPTS {
            // Short-timeout probes: create_http_client() is 30s + 3 retries and
            // would inflate failure-path wait_for_documents up to minutes.
            if let Err(error) = self.refresh_index_probe(&client).await {
                last_error = Some(error);
                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                continue;
            }

            match self.count_documents_probe(&client).await {
                Ok(count) if count >= expected_count => {
                    info!("Found {count} documents in Elasticsearch (expected {expected_count})");
                    return Ok(count);
                }
                Ok(_) => {
                    last_error = None;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        let final_count = self.count_documents_probe(&client).await.unwrap_or(0);
        let detail = last_error
            .map(|error| format!("; last error: {error}"))
            .unwrap_or_default();
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected_count} documents, found {final_count} after {POLL_ATTEMPTS} attempts{detail}"
            ),
        })
    }

    pub async fn search_documents(&self) -> Result<ElasticsearchSearchResponse, TestBinaryError> {
        self.search_all(&self.index).await
    }

    pub async fn refresh_index(&self) -> Result<(), TestBinaryError> {
        ElasticsearchOps::refresh_index(self, &self.index).await
    }

    /// Refresh with the short-timeout probe client shared across one poll loop.
    async fn refresh_index_probe(&self, client: &reqwest::Client) -> Result<(), TestBinaryError> {
        let url = format!("{}/{}/_refresh", self.container.base_url, self.index);
        let response =
            client
                .post(&url)
                .send()
                .await
                .map_err(|error| TestBinaryError::InvalidState {
                    message: format!("Failed to refresh index: {error}"),
                })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(TestBinaryError::InvalidState {
                message: format!("Failed to refresh index: status={status}, body={body}"),
            });
        }
        Ok(())
    }

    /// Count with the short-timeout probe client shared across one poll loop.
    ///
    /// `count_documents` (via `ElasticsearchOps`) goes through the shared
    /// `create_http_client()` - 30s timeout plus 3 retries - so a degraded
    /// `_count` endpoint could block this poll for minutes even though
    /// `refresh_index_probe` already uses a short timeout.
    async fn count_documents_probe(
        &self,
        client: &reqwest::Client,
    ) -> Result<usize, TestBinaryError> {
        let url = format!("{}/{}/_count", self.container.base_url, self.index);
        let response =
            client
                .get(&url)
                .send()
                .await
                .map_err(|error| TestBinaryError::InvalidState {
                    message: format!("Failed to count documents: {error}"),
                })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(TestBinaryError::InvalidState {
                message: format!("Failed to count documents: status={status}, body={body}"),
            });
        }

        #[derive(serde::Deserialize)]
        struct CountResponse {
            count: usize,
        }
        let count_response = response.json::<CountResponse>().await.map_err(|error| {
            TestBinaryError::InvalidState {
                message: format!("Failed to parse count response: {error}"),
            }
        })?;
        Ok(count_response.count)
    }
}

fn probe_client() -> Result<reqwest::Client, TestBinaryError> {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(PROBE_REQUEST_TIMEOUT_MS))
        .build()
        .map_err(|error| TestBinaryError::InvalidState {
            message: format!("Failed to build probe client: {error}"),
        })
}

#[async_trait]
impl TestFixture for ElasticsearchSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ElasticsearchContainer::start().await?;
        let http_client = create_http_client();
        let index = format!("{SINK_INDEX_PREFIX}_{}", Uuid::new_v4().simple());

        Ok(Self {
            container,
            http_client,
            index,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SINK_INDEX.to_string(), self.index.clone());
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
