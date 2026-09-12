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

use integration::harness::TestBinaryError;
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::Deserialize;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, ReuseDirective,
};
use tokio::time::sleep;
use tracing::info;

const OPENSEARCH_IMAGE: &str = "opensearchproject/opensearch";
// Pinned rather than a floating major tag: verified against this exact tag
// that DISABLE_SECURITY_PLUGIN=true still boots green over plain HTTP with no
// OPENSEARCH_INITIAL_ADMIN_PASSWORD (required since 2.12 unless the security
// plugin is genuinely disabled).
const OPENSEARCH_TAG: &str = "3.8.0";
const OPENSEARCH_PORT: u16 = 9200;
const OPENSEARCH_HEALTH_ENDPOINT: &str = "/_cluster/health";
// Fixed name + ReuseDirective::Always shares one container across nextest's
// per-test processes, mirroring the Elasticsearch fixture: the first test
// creates it, later test processes attach by name. Per-test isolation comes
// from a unique index per fixture, not a fresh container.
const OPENSEARCH_CONTAINER_NAME: &str = "iggy-test-opensearch";

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub const DEFAULT_TEST_TOPIC_2: &str = "test_topic_2";

#[derive(Debug, Deserialize)]
pub struct OpenSearchCountResponse {
    pub count: u64,
}

pub struct OpenSearchContainer {
    // Held so testcontainers' Drop runs on test exit; ReuseDirective::Always
    // makes that Drop leave the container running for the next test to attach.
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

/// See [`start_shared_container`]: same create-or-attach race as the
/// Elasticsearch fixture, same bound.
const CONTAINER_START_ATTEMPTS: u32 = 30;
const CONTAINER_START_RETRY_DELAY: Duration = Duration::from_secs(1);

async fn start_shared_container() -> Result<ContainerAsync<GenericImage>, TestBinaryError> {
    let mut conflict = String::new();
    for attempt in 1..=CONTAINER_START_ATTEMPTS {
        // Rebuilt per attempt because `start` consumes the request.
        let result = GenericImage::new(OPENSEARCH_IMAGE, OPENSEARCH_TAG)
            .with_exposed_port(OPENSEARCH_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new(OPENSEARCH_HEALTH_ENDPOINT)
                    .with_port(OPENSEARCH_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_startup_timeout(Duration::from_secs(120))
            .with_env_var("discovery.type", "single-node")
            .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
            .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
            .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_mapped_port(0, OPENSEARCH_PORT.tcp())
            .with_container_name(OPENSEARCH_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await;

        match result {
            Ok(container) => return Ok(container),
            Err(error) => {
                let message = error.to_string();
                if !message.contains("is already in use") {
                    return Err(TestBinaryError::FixtureSetup {
                        fixture_type: "OpenSearchContainer".to_string(),
                        message: format!("Failed to start container: {message}"),
                    });
                }
                info!(
                    "OpenSearch container name taken by another test (attempt {attempt}), retrying to attach"
                );
                conflict = message;
                sleep(CONTAINER_START_RETRY_DELAY).await;
            }
        }
    }

    Err(TestBinaryError::FixtureSetup {
        fixture_type: "OpenSearchContainer".to_string(),
        message: format!(
            "Failed to attach to container '{OPENSEARCH_CONTAINER_NAME}' after \
             {CONTAINER_START_ATTEMPTS} attempts: {conflict}"
        ),
    })
}

impl OpenSearchContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = start_shared_container().await?;

        info!("Started OpenSearch container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "OpenSearchContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(OPENSEARCH_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "OpenSearchContainer".to_string(),
                message: "No mapping for OpenSearch port".to_string(),
            })?;

        let base_url = format!("http://localhost:{mapped_port}");
        info!("OpenSearch container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

pub fn create_http_client() -> HttpClient {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    reqwest_middleware::ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

pub trait OpenSearchOps: Sync {
    fn container(&self) -> &OpenSearchContainer;
    fn http_client(&self) -> &HttpClient;

    fn count_documents(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<u64, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}/_count", self.container().base_url, index_name);

            let response = self.http_client().get(&url).send().await.map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to count OpenSearch documents: {e}"),
                }
            })?;

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                // Index not created yet: treat as zero documents rather than
                // an error, since polling starts before the sink has opened.
                return Ok(0);
            }
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!(
                        "Failed to count OpenSearch documents: status={status}, body={body}"
                    ),
                });
            }

            response
                .json::<OpenSearchCountResponse>()
                .await
                .map(|response| response.count)
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse OpenSearch count response: {e}"),
                })
        }
    }

    fn search_all(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}/_search", self.container().base_url, index_name);
            let query = serde_json::json!({ "query": { "match_all": {} }, "size": 1000 });

            let response = self
                .http_client()
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&query)
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to search OpenSearch index: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!(
                        "Failed to search OpenSearch index: status={status}, body={body}"
                    ),
                });
            }

            response
                .json::<serde_json::Value>()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse OpenSearch search response: {e}"),
                })
        }
    }
}
