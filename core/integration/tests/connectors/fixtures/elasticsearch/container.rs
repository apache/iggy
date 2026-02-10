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

use integration::harness::TestBinaryError;
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::Deserialize;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tracing::info;
use uuid::Uuid;

const ELASTICSEARCH_IMAGE: &str = "elasticsearch";
const ELASTICSEARCH_TAG: &str = "9.3.0";
const ELASTICSEARCH_PORT: u16 = 9200;
const ELASTICSEARCH_READY_MSG: &str = "started";

pub const HEALTH_CHECK_ATTEMPTS: usize = 30;
pub const HEALTH_CHECK_INTERVAL_MS: u64 = 500;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

pub const ENV_SOURCE_URL: &str = "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PLUGIN_CONFIG_URL";
pub const ENV_SOURCE_INDEX: &str = "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PLUGIN_CONFIG_INDEX";
pub const ENV_SOURCE_POLLING_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PLUGIN_CONFIG_POLLING_INTERVAL";
pub const ENV_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_SOURCE_TIMESTAMP_FIELD: &str =
    "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PLUGIN_CONFIG_TIMESTAMP_FIELD";
pub const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_STREAMS_0_STREAM";
pub const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_STREAMS_0_TOPIC";
pub const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_STREAMS_0_SCHEMA";
pub const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_ELASTICSEARCH_PATH";

pub const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PLUGIN_CONFIG_URL";
pub const ENV_SINK_INDEX: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PLUGIN_CONFIG_INDEX";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_ELASTICSEARCH_STREAMS_0_CONSUMER_GROUP";
pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_ELASTICSEARCH_PATH";

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ElasticsearchSearchResponse {
    pub hits: ElasticsearchHits,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ElasticsearchHits {
    pub total: ElasticsearchTotal,
    pub hits: Vec<ElasticsearchHit>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ElasticsearchTotal {
    pub value: usize,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ElasticsearchHit {
    #[serde(rename = "_source")]
    pub source: serde_json::Value,
}

pub struct ElasticsearchContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl ElasticsearchContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let unique_network = format!("iggy-elasticsearch-source-{}", Uuid::new_v4());

        let container = GenericImage::new(ELASTICSEARCH_IMAGE, ELASTICSEARCH_TAG)
            .with_exposed_port(ELASTICSEARCH_PORT.tcp())
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

        info!("Started Elasticsearch container");

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

        let base_url = format!("http://localhost:{mapped_port}");
        info!("Elasticsearch container available at {base_url}");

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

pub trait ElasticsearchOps: Sync {
    fn container(&self) -> &ElasticsearchContainer;
    fn http_client(&self) -> &HttpClient;

    fn create_index(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}", self.container().base_url, index_name);
            let mapping = serde_json::json!({
                "mappings": {
                    "properties": {
                        "id": { "type": "integer" },
                        "name": { "type": "keyword" },
                        "value": { "type": "integer" },
                        "timestamp": { "type": "date" }
                    }
                }
            });

            let response = self
                .http_client()
                .put(&url)
                .header("Content-Type", "application/json")
                .json(&mapping)
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "ElasticsearchOps".to_string(),
                    message: format!("Failed to create index: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "ElasticsearchOps".to_string(),
                    message: format!("Failed to create index: status={status}, body={body}"),
                });
            }

            info!("Created Elasticsearch index: {index_name}");
            Ok(())
        }
    }

    fn index_document(
        &self,
        index_name: &str,
        doc_id: &str,
        document: &serde_json::Value,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!(
                "{}/{}/_doc/{}",
                self.container().base_url,
                index_name,
                doc_id
            );

            let response = self
                .http_client()
                .put(&url)
                .header("Content-Type", "application/json")
                .json(document)
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "ElasticsearchOps".to_string(),
                    message: format!("Failed to index document: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "ElasticsearchOps".to_string(),
                    message: format!("Failed to index document: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn refresh_index(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}/_refresh", self.container().base_url, index_name);

            let response = self.http_client().post(&url).send().await.map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to refresh index: {e}"),
                }
            })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to refresh index: status={status}, body={body}"),
                });
            }

            info!("Refreshed Elasticsearch index: {index_name}");
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn search_all(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<ElasticsearchSearchResponse, TestBinaryError>> + Send
    {
        async move {
            let url = format!("{}/{}/_search", self.container().base_url, index_name);
            let query = serde_json::json!({
                "query": { "match_all": {} },
                "size": 1000,
                "_source": true
            });

            let response = self
                .http_client()
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&query)
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to search index: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to search index: status={status}, body={body}"),
                });
            }

            let text = response
                .text()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to get response text: {e}"),
                })?;

            info!("Elasticsearch search response: {text}");

            serde_json::from_str::<ElasticsearchSearchResponse>(&text).map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to parse search response: {e}, body: {text}"),
                }
            })
        }
    }

    fn count_documents(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<usize, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}/_count", self.container().base_url, index_name);

            let response = self.http_client().get(&url).send().await.map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to count documents: {e}"),
                }
            })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to count documents: status={status}, body={body}"),
                });
            }

            #[derive(Deserialize)]
            struct CountResponse {
                count: usize,
            }

            let count_response = response.json::<CountResponse>().await.map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to parse count response: {e}"),
                }
            })?;

            Ok(count_response.count)
        }
    }
}
