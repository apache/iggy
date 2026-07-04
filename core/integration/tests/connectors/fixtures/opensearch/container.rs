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
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, ReuseDirective,
};
use tracing::info;

const OPENSEARCH_IMAGE: &str = "docker.io/opensearchproject/opensearch";
const OPENSEARCH_TAG: &str = "2.19.1";
const OPENSEARCH_PORT: u16 = 9200;
const OPENSEARCH_HEALTH_ENDPOINT: &str = "/_cluster/health";
// Fixed name + ReuseDirective::Always shares one container across nextest's
// per-test processes: the first test creates it, every later test attaches by
// name. Per-test isolation comes from a unique index per fixture (see
// source.rs), not a fresh container; create_index/create_typed_fields_index
// still delete any leftover index of the same name first as a defensive
// cleanup against stale data in the shared instance.
const OPENSEARCH_CONTAINER_NAME: &str = "iggy-test-opensearch";

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

pub const ENV_SOURCE_URL: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_URL";
pub const ENV_SOURCE_INDEX: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_INDEX";
pub const ENV_SOURCE_POLLING_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_POLLING_INTERVAL";
pub const ENV_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_SOURCE_TIMESTAMP_FIELD: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_TIMESTAMP_FIELD";
pub const ENV_SOURCE_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_STREAMS_0_STREAM";
pub const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_STREAMS_0_TOPIC";
pub const ENV_SOURCE_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_STREAMS_0_SCHEMA";
pub const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PATH";
pub const ENV_SOURCE_MAX_RETRIES: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_MAX_RETRIES";
pub const ENV_SOURCE_RETRY_DELAY: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_RETRY_DELAY";
pub const ENV_SOURCE_RETRY_MAX_DELAY: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_RETRY_MAX_DELAY";
pub const ENV_SOURCE_MAX_OPEN_RETRIES: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_MAX_OPEN_RETRIES";
pub const ENV_SOURCE_OPEN_RETRY_MAX_DELAY: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_OPEN_RETRY_MAX_DELAY";
pub const ENV_SOURCE_CIRCUIT_BREAKER_THRESHOLD: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_CIRCUIT_BREAKER_THRESHOLD";
pub const ENV_SOURCE_CIRCUIT_BREAKER_COOL_DOWN: &str =
    "IGGY_CONNECTORS_SOURCE_OPENSEARCH_PLUGIN_CONFIG_CIRCUIT_BREAKER_COOL_DOWN";

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OpenSearchSearchResponse {
    pub hits: OpenSearchHits,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OpenSearchHits {
    pub total: OpenSearchTotal,
    pub hits: Vec<OpenSearchHit>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OpenSearchTotal {
    pub value: usize,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OpenSearchHit {
    #[serde(rename = "_source")]
    pub source: serde_json::Value,
}

pub struct OpenSearchContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl OpenSearchContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(OPENSEARCH_IMAGE, OPENSEARCH_TAG)
            .with_exposed_port(OPENSEARCH_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new(OPENSEARCH_HEALTH_ENDPOINT)
                    .with_port(OPENSEARCH_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_startup_timeout(std::time::Duration::from_secs(120))
            .with_env_var("discovery.type", "single-node")
            .with_env_var("plugins.security.disabled", "true")
            .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
            .with_env_var("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "iggy-test-password1!")
            .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_mapped_port(0, OPENSEARCH_PORT.tcp())
            .with_container_name(OPENSEARCH_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "OpenSearchContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

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

    /// Deletes `index_name` if it exists, guarding against stale data left
    /// behind under the same name in the shared, reused container.
    fn delete_index_if_exists(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let url = format!("{}/{}", self.container().base_url, index_name);
            let response = self.http_client().delete(&url).send().await.map_err(|e| {
                TestBinaryError::FixtureSetup {
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to delete stale index: {e}"),
                }
            })?;

            if !response.status().is_success()
                && response.status() != reqwest::StatusCode::NOT_FOUND
            {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to delete stale index: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn create_index(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            self.delete_index_if_exists(index_name).await?;

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
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to create index: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to create index: status={status}, body={body}"),
                });
            }

            info!("Created OpenSearch index: {index_name}");
            Ok(())
        }
    }

    fn create_typed_fields_index(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            self.delete_index_if_exists(index_name).await?;

            let url = format!("{}/{}", self.container().base_url, index_name);
            let mapping = serde_json::json!({
                "mappings": {
                    "properties": {
                        "id": { "type": "integer" },
                        "title": { "type": "text" },
                        "status": { "type": "keyword" },
                        "count": { "type": "long" },
                        "score": { "type": "float" },
                        "ratio": { "type": "double" },
                        "active": { "type": "boolean" },
                        "timestamp": { "type": "date" },
                        "client_ip": { "type": "ip" },
                        "location": { "type": "geo_point" },
                        "tags": { "type": "keyword" },
                        "optional_note": { "type": "keyword" }
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
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to create typed index: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to create typed index: status={status}, body={body}"),
                });
            }

            info!("Created typed OpenSearch index: {index_name}");
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
                    fixture_type: "OpenSearchOps".to_string(),
                    message: format!("Failed to index document: {e}"),
                })?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "OpenSearchOps".to_string(),
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

            info!("Refreshed OpenSearch index: {index_name}");
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn search_all(
        &self,
        index_name: &str,
    ) -> impl std::future::Future<Output = Result<OpenSearchSearchResponse, TestBinaryError>> + Send
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

            info!("OpenSearch search response: {text}");

            serde_json::from_str::<OpenSearchSearchResponse>(&text).map_err(|e| {
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
