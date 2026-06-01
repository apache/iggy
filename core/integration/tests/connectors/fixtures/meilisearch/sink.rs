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
use integration::harness::{TestBinaryError, TestFixture, seeds};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const MEILISEARCH_IMAGE: &str = "getmeili/meilisearch";
const MEILISEARCH_TAG: &str = "v1.13";
const MEILISEARCH_PORT: u16 = 7700;
const MEILISEARCH_HEALTH_ENDPOINT: &str = "/health";
const SINK_INDEX: &str = "iggy_messages";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;

const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_PLUGIN_CONFIG_URL";
const ENV_SINK_INDEX: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_PLUGIN_CONFIG_INDEX";
const ENV_SINK_PRIMARY_KEY: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_PLUGIN_CONFIG_PRIMARY_KEY";
const ENV_SINK_TASK_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SINK_MEILISEARCH_PLUGIN_CONFIG_TASK_POLL_INTERVAL";
const ENV_SINK_TASK_TIMEOUT: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_PLUGIN_CONFIG_TASK_TIMEOUT";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_MEILISEARCH_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_MEILISEARCH_PATH";

#[derive(Debug, Deserialize)]
pub struct MeilisearchDocumentsResponse {
    pub results: Vec<serde_json::Value>,
}

pub struct MeilisearchContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl MeilisearchContainer {
    async fn start() -> Result<Self, TestBinaryError> {
        let unique_network = format!("iggy-meilisearch-sink-{}", Uuid::new_v4());

        let container = GenericImage::new(MEILISEARCH_IMAGE, MEILISEARCH_TAG)
            .with_exposed_port(MEILISEARCH_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new(MEILISEARCH_HEALTH_ENDPOINT)
                    .with_port(MEILISEARCH_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_network(unique_network)
            .with_env_var("MEILI_ENV", "development")
            .with_mapped_port(0, MEILISEARCH_PORT.tcp())
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MeilisearchContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        info!("Started Meilisearch container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MeilisearchContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(MEILISEARCH_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MeilisearchContainer".to_string(),
                message: "No mapping for Meilisearch port".to_string(),
            })?;

        let base_url = format!("http://localhost:{mapped_port}");
        info!("Meilisearch container available at {base_url}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

pub fn create_http_client() -> HttpClient {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    reqwest_middleware::ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

pub trait MeilisearchOps: Sync {
    fn container(&self) -> &MeilisearchContainer;
    fn http_client(&self) -> &HttpClient;

    fn list_documents(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<Vec<serde_json::Value>, TestBinaryError>> + Send
    {
        async move {
            let url = format!(
                "{}/indexes/{}/documents",
                self.container().base_url,
                index_name
            );
            let response = self
                .http_client()
                .get(&url)
                .query(&[("limit", "100")])
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to list Meilisearch documents: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!(
                        "Failed to list Meilisearch documents: status={status}, body={body}"
                    ),
                });
            }

            response
                .json::<MeilisearchDocumentsResponse>()
                .await
                .map(|documents| documents.results)
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse Meilisearch documents response: {e}"),
                })
        }
    }

    fn wait_for_documents(
        &self,
        expected_count: usize,
    ) -> impl std::future::Future<Output = Result<Vec<serde_json::Value>, TestBinaryError>> + Send
    {
        async move {
            let mut last_count = 0usize;
            for _ in 0..POLL_ATTEMPTS {
                if let Ok(documents) = self.list_documents(SINK_INDEX).await {
                    last_count = documents.len();
                    if documents.len() >= expected_count {
                        return Ok(documents);
                    }
                }
                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }

            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected {expected_count} Meilisearch documents, found {last_count} after {POLL_ATTEMPTS} attempts"
                ),
            })
        }
    }
}

pub struct MeilisearchSinkFixture {
    container: MeilisearchContainer,
    http_client: HttpClient,
}

impl MeilisearchOps for MeilisearchSinkFixture {
    fn container(&self) -> &MeilisearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

#[async_trait]
impl TestFixture for MeilisearchSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MeilisearchContainer::start().await?;
        let http_client = create_http_client();

        Ok(Self {
            container,
            http_client,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_SINK_URL.to_string(), self.container.base_url.clone()),
            (ENV_SINK_INDEX.to_string(), SINK_INDEX.to_string()),
            (ENV_SINK_PRIMARY_KEY.to_string(), "iggy_id".to_string()),
            (ENV_SINK_TASK_TIMEOUT.to_string(), "10s".to_string()),
            (ENV_SINK_TASK_POLL_INTERVAL.to_string(), "25ms".to_string()),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{}]", seeds::names::TOPIC),
            ),
            (ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
                "meilisearch_sink_cg".to_string(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_meilisearch_sink".to_string(),
            ),
        ])
    }
}
