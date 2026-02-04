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
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const POLL_INTERVAL_MS: u64 = 200;
const MAX_POLL_ATTEMPTS: usize = 100;

const ELASTICSEARCH_IMAGE: &str = "elasticsearch";
const ELASTICSEARCH_TAG: &str = "8.17.0";
const ELASTICSEARCH_PORT: u16 = 9200;
const ELASTICSEARCH_READY_MSG: &str = "started";

const ENV_PLUGIN_URL: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PLUGIN_CONFIG_URL";
const ENV_PLUGIN_INDEX: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PLUGIN_CONFIG_INDEX";
const ENV_PLUGIN_CREATE_INDEX: &str =
    "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PLUGIN_CONFIG_CREATE_INDEX_IF_NOT_EXISTS";
const ENV_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_STREAM";
const ENV_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_TOPICS";
const ENV_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_SCHEMA";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PATH";

#[derive(Deserialize)]
pub struct ElasticsearchSearchResponse {
    pub hits: ElasticsearchHits,
}

#[derive(Deserialize)]
pub struct ElasticsearchHits {
    pub hits: Vec<ElasticsearchHit>,
}

#[derive(Deserialize)]
pub struct ElasticsearchHit {
    #[serde(rename = "_source")]
    pub source: serde_json::Value,
}

pub struct ElasticsearchContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    mapped_port: u16,
}

impl ElasticsearchContainer {
    async fn start() -> Result<Self, TestBinaryError> {
        let unique_network = format!("iggy-elasticsearch-sink-{}", Uuid::new_v4());

        let container = GenericImage::new(ELASTICSEARCH_IMAGE, ELASTICSEARCH_TAG)
            .with_exposed_port(0.tcp())
            .with_wait_for(WaitFor::message_on_stdout(ELASTICSEARCH_READY_MSG))
            .with_network(unique_network)
            .with_env_var("discovery.type", "single-node")
            .with_env_var("xpack.security.enabled", "false")
            .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_mapped_port(0, ELASTICSEARCH_PORT.tcp())
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ElasticsearchContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        info!("Started elasticsearch container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "ElasticsearchContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(ELASTICSEARCH_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "ElasticsearchContainer".to_string(),
                message: "No mapping for Elasticsearch port".to_string(),
            })?;

        info!("Elasticsearch container mapped to port {mapped_port}");

        Ok(Self {
            container,
            mapped_port,
        })
    }

    fn base_url(&self) -> String {
        format!("http://localhost:{}", self.mapped_port)
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

pub trait ElasticsearchOps: Sync {
    fn container(&self) -> &ElasticsearchContainer;
    fn http_client(&self) -> &HttpClient;

    fn refresh_index(
        &self,
        index: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let response = self
                .http_client()
                .post(format!(
                    "{}/{}/_refresh",
                    self.container().base_url(),
                    index
                ))
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to refresh index: {e}"),
                })?;

            info!("Received index refresh response");

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to refresh index: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn search_all(
        &self,
        index: &str,
    ) -> impl std::future::Future<Output = Result<ElasticsearchSearchResponse, TestBinaryError>> + Send
    {
        async move {
            let search_url = format!("{}/{}/_search", self.container().base_url(), index);

            let response = self
                .http_client()
                .get(&search_url)
                .json(&serde_json::json!({
                    "query": { "match_all": {} },
                    "size": 10000
                }))
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to search index: {e}"),
                })?;

            info!("Received search index response");

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to search index: status={status}, body={body}"),
                });
            }

            response
                .json::<ElasticsearchSearchResponse>()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse search response: {e}"),
                })
        }
    }

    fn fetch_documents_with_count(
        &self,
        index: &str,
        expected_count: usize,
    ) -> impl std::future::Future<Output = Result<Vec<serde_json::Value>, TestBinaryError>> + Send
    {
        async move {
            for attempt in 0..MAX_POLL_ATTEMPTS {
                self.refresh_index(index).await?;
                let response = self.search_all(index).await?;
                let docs: Vec<serde_json::Value> =
                    response.hits.hits.into_iter().map(|h| h.source).collect();

                if docs.len() >= expected_count {
                    info!(
                        "Found {} documents after {} attempts",
                        docs.len(),
                        attempt + 1
                    );
                    return Ok(docs);
                }

                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }

            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Timeout waiting for {} documents in index {}",
                    expected_count, index
                ),
            })
        }
    }
}

fn build_connector_envs(base_url: &str) -> HashMap<String, String> {
    HashMap::from([
        (ENV_PLUGIN_URL.to_string(), base_url.to_string()),
        (
            ENV_PLUGIN_INDEX.to_string(),
            seeds::names::TOPIC.to_string(),
        ),
        (ENV_PLUGIN_CREATE_INDEX.to_string(), "true".to_string()),
        (
            ENV_STREAMS_0_STREAM.to_string(),
            seeds::names::STREAM.to_string(),
        ),
        (
            ENV_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", seeds::names::TOPIC),
        ),
        (ENV_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
        (
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_elasticsearch_sink".to_string(),
        ),
    ])
}

pub struct ElasticsearchFixture {
    container: ElasticsearchContainer,
    http_client: HttpClient,
}

impl ElasticsearchOps for ElasticsearchFixture {
    fn container(&self) -> &ElasticsearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

#[async_trait]
impl TestFixture for ElasticsearchFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = ElasticsearchContainer::start().await?;
        let http_client = create_http_client();

        Ok(Self {
            container,
            http_client,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        build_connector_envs(&self.container.base_url())
    }
}
