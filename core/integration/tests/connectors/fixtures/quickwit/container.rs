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

const DEFAULT_POLL_ATTEMPTS: usize = 100;
const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

const QUICKWIT_IMAGE: &str = "quickwit/quickwit";
const QUICKWIT_TAG: &str = "0.8.2";
const QUICKWIT_PORT: u16 = 7280;
const QUICKWIT_READY_MSG: &str = "REST server is ready";
const QUICKWIT_LISTEN_ADDRESS: &str = "0.0.0.0";

const ENV_PLUGIN_URL: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG_URL";
const ENV_PLUGIN_INDEX: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG_INDEX";
const ENV_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_STREAM";
const ENV_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_TOPICS";
const ENV_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_SCHEMA";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PATH";

const INDEXES_API_PATH: &str = "api/v1/indexes";
const INDEX_TIMESTAMP_FIELD: &str = "timestamp";

#[derive(Deserialize)]
pub struct QuickwitSearchResponse {
    pub hits: Vec<serde_json::Value>,
    pub num_hits: usize,
}

pub struct QuickwitContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    mapped_port: u16,
}

impl QuickwitContainer {
    async fn start() -> Result<Self, TestBinaryError> {
        let unique_network = format!("iggy-quickwit-sink-{}", Uuid::new_v4());

        let container = GenericImage::new(QUICKWIT_IMAGE, QUICKWIT_TAG)
            .with_exposed_port(0.tcp())
            .with_wait_for(WaitFor::message_on_stdout(QUICKWIT_READY_MSG))
            .with_network(unique_network)
            .with_cmd(["run"])
            .with_env_var("QW_LISTEN_ADDRESS", QUICKWIT_LISTEN_ADDRESS)
            .with_mapped_port(0, QUICKWIT_PORT.tcp())
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "QuickwitContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        info!("Started quickwit container");

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "QuickwitContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(QUICKWIT_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "QuickwitContainer".to_string(),
                message: "No mapping for Quickwit port".to_string(),
            })?;

        info!("Quickwit container mapped to port {mapped_port}");

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

/// Trait for Quickwit fixtures with common HTTP operations.
pub trait QuickwitOps: Sync {
    fn container(&self) -> &QuickwitContainer;
    fn http_client(&self) -> &HttpClient;

    fn create_index(
        &self,
        index_config: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let response = self
                .http_client()
                .post(format!(
                    "{}/{}",
                    self.container().base_url(),
                    INDEXES_API_PATH
                ))
                .header("Content-Type", "application/yaml")
                .body(index_config.to_string())
                .send()
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "QuickwitOps".to_string(),
                    message: format!("Failed to create index: {e}"),
                })?;

            info!("Received create index response");

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::FixtureSetup {
                    fixture_type: "QuickwitOps".to_string(),
                    message: format!("Failed to create index: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn flush_index(
        &self,
        index_id: &str,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send {
        async move {
            let ingest_url = format!("{}/api/v1/{}/ingest", self.container().base_url(), index_id);

            let response = self
                .http_client()
                .post(&ingest_url)
                .query(&[("commit", "force")])
                .json("{}")
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to flush index: {e}"),
                })?;

            info!("Received index ingest/flush response");

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(TestBinaryError::InvalidState {
                    message: format!("Failed to flush index: status={status}, body={body}"),
                });
            }

            Ok(())
        }
    }

    fn search_all(
        &self,
        index_id: &str,
    ) -> impl std::future::Future<Output = Result<QuickwitSearchResponse, TestBinaryError>> + Send
    {
        async move {
            let search_url = format!("{}/api/v1/{}/search", self.container().base_url(), index_id);
            let descending = "-";

            let response = self
                .http_client()
                .get(&search_url)
                .query(&[("query", "")])
                .query(&[("sort_by", format!("{descending}{INDEX_TIMESTAMP_FIELD}"))])
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
                .json::<QuickwitSearchResponse>()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse search response: {e}"),
                })
        }
    }

    fn wait_for_documents(
        &self,
        index_id: &str,
        expected_count: usize,
    ) -> impl std::future::Future<Output = Result<QuickwitSearchResponse, TestBinaryError>> + Send
    {
        async move {
            let mut last_count = 0;
            for _ in 0..DEFAULT_POLL_ATTEMPTS {
                if self.flush_index(index_id).await.is_ok()
                    && let Ok(search) = self.search_all(index_id).await
                {
                    last_count = search.num_hits;
                    if search.num_hits >= expected_count {
                        return Ok(search);
                    }
                }
                sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS / 5)).await;
            }
            Err(TestBinaryError::InvalidState {
                message: format!(
                    "Expected {} documents but got {} after {} poll attempts",
                    expected_count, last_count, DEFAULT_POLL_ATTEMPTS
                ),
            })
        }
    }
}

fn get_index_config(index_id: &str) -> String {
    format!(
        r#"
version: 0.8
index_id: {index_id}
doc_mapping:
  mode: strict
  field_mappings:
    - name: id
      type: u64
    - name: name
      type: text
      tokenizer: raw
    - name: count
      type: u64
    - name: amount
      type: f64
    - name: active
      type: bool
    - name: {INDEX_TIMESTAMP_FIELD}
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_micros
      fast_precision: microseconds
      fast: true
  timestamp_field: timestamp
retention:
  period: 7 days
  schedule: daily
"#
    )
}

fn build_connector_envs(base_url: &str) -> HashMap<String, String> {
    HashMap::from([
        (ENV_PLUGIN_URL.to_string(), base_url.to_string()),
        (
            ENV_PLUGIN_INDEX.to_string(),
            get_index_config(seeds::names::TOPIC),
        ),
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
            "../../target/debug/libiggy_connector_quickwit_sink".to_string(),
        ),
    ])
}

/// Quickwit fixture where the connector auto-creates the index.
pub struct QuickwitFixture {
    container: QuickwitContainer,
    http_client: HttpClient,
}

impl QuickwitOps for QuickwitFixture {
    fn container(&self) -> &QuickwitContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

#[async_trait]
impl TestFixture for QuickwitFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = QuickwitContainer::start().await?;
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

/// Quickwit fixture where the index is pre-created before the test.
pub struct QuickwitPreCreatedFixture {
    inner: QuickwitFixture,
}

impl std::ops::Deref for QuickwitPreCreatedFixture {
    type Target = QuickwitFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl QuickwitOps for QuickwitPreCreatedFixture {
    fn container(&self) -> &QuickwitContainer {
        &self.inner.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}

#[async_trait]
impl TestFixture for QuickwitPreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = QuickwitContainer::start().await?;
        let http_client = create_http_client();

        let inner = QuickwitFixture {
            container,
            http_client,
        };

        let index_config = get_index_config(seeds::names::TOPIC);
        inner.create_index(&index_config).await?;

        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
