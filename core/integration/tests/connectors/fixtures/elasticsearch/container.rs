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
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, ReuseDirective,
};
use tracing::{info, warn};

const ELASTICSEARCH_IMAGE: &str = "docker.io/library/elasticsearch";
const ELASTICSEARCH_TAG: &str = "9.3.0";
const ELASTICSEARCH_PORT: u16 = 9200;
const ELASTICSEARCH_HEALTH_ENDPOINT: &str = "/_cluster/health";
// Fixed name + ReuseDirective::Always shares one container across nextest's
// per-test processes: the first test creates it, every later test attaches by
// name. Per-test isolation comes from a unique index per fixture, not a fresh
// container.
const ELASTICSEARCH_CONTAINER_NAME: &str = "iggy-test-elasticsearch";
// Short probe timeouts: create_http_client() uses 30s + retries and must not
// be used for readiness. One hung attempt there looks like a 60s+ test hang.
const CLUSTER_READY_ATTEMPTS: usize = 40;
const CLUSTER_READY_INTERVAL_MS: u64 = 250;
const CLUSTER_READY_REQUEST_TIMEOUT_MS: u64 = 2_000;
const DOCKER_RM_TIMEOUT_SECS: u64 = 15;
// Indices from prior runs older than this are leftovers: a live concurrent
// test's index is seconds old, so age-based sweeping never races other tests
// sharing the reused container.
const STALE_INDEX_MAX_AGE_MS: u128 = 30 * 60 * 1000;
const STALE_INDEX_PATTERNS: &str = "iggy_messages_*,test_documents_*";

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
    // Held so testcontainers' Drop runs on test exit; ReuseDirective::Always
    // makes that Drop leave the container running for the next test to attach.
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl ElasticsearchContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        match Self::try_start().await {
            Ok(started) => Ok(started),
            Err(first_error) => {
                // A reused container can be wedged (running but unhealthy:
                // OOM-killed JVM, corrupted data dir, dead port mapping).
                // Remove it and retry once with a fresh container instead of
                // failing every test until someone cleans up manually.
                warn!(
                    "Elasticsearch container unusable, removing '{ELASTICSEARCH_CONTAINER_NAME}' and retrying once: {first_error}"
                );
                force_remove_container();
                Self::try_start().await
            }
        }
    }

    async fn try_start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(ELASTICSEARCH_IMAGE, ELASTICSEARCH_TAG)
            .with_exposed_port(ELASTICSEARCH_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new(ELASTICSEARCH_HEALTH_ENDPOINT)
                    .with_port(ELASTICSEARCH_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_startup_timeout(std::time::Duration::from_secs(120))
            .with_env_var("discovery.type", "single-node")
            .with_env_var("xpack.security.enabled", "false")
            .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_mapped_port(0, ELASTICSEARCH_PORT.tcp())
            .with_container_name(ELASTICSEARCH_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
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

        // Prefer IPv4 loopback: Docker publishes 0.0.0.0:HOST→9200. `localhost`
        // can resolve to ::1 first on macOS and black-hole the elasticsearch-rs
        // client while the fixture's reqwest client still looks healthy.
        let base_url = format!("http://127.0.0.1:{mapped_port}");
        info!("Elasticsearch container available at {base_url}");

        let started = Self {
            container,
            base_url,
        };
        // ReuseDirective::Always can attach to a days-old container without
        // re-running HttpWaitStrategy; verify cluster health on every setup.
        started.wait_until_ready().await?;
        started.sweep_stale_indices().await;
        Ok(started)
    }

    async fn wait_until_ready(&self) -> Result<(), TestBinaryError> {
        // Dedicated probe client: short timeout, no retry middleware. The shared
        // create_http_client() (30s + 3 retries) turns one black-holed request
        // into a multi-minute hang that looks like the test is stuck.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(
                CLUSTER_READY_REQUEST_TIMEOUT_MS,
            ))
            .build()
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "ElasticsearchContainer".to_string(),
                message: format!("Failed to build readiness HTTP client: {error}"),
            })?;
        // timeout=1s keeps ES from holding the request when the cluster is slow.
        let health_url = format!(
            "{}{ELASTICSEARCH_HEALTH_ENDPOINT}?timeout=1s",
            self.base_url
        );
        let mut last_error = String::from("no attempts made");

        for attempt in 1..=CLUSTER_READY_ATTEMPTS {
            match client.get(&health_url).send().await {
                Ok(response) if response.status().is_success() => {
                    let body = response.text().await.unwrap_or_default();
                    if body.contains("\"timed_out\":true") {
                        last_error = format!(
                            "cluster health timed out on attempt {attempt}/{CLUSTER_READY_ATTEMPTS}: {body}"
                        );
                    } else {
                        info!("Elasticsearch cluster ready at {}", self.base_url);
                        return Ok(());
                    }
                }
                Ok(response) => {
                    last_error = format!(
                        "cluster health status {} on attempt {attempt}/{CLUSTER_READY_ATTEMPTS}",
                        response.status()
                    );
                }
                Err(error) => {
                    last_error = format!(
                        "cluster health request failed on attempt {attempt}/{CLUSTER_READY_ATTEMPTS}: {error}"
                    );
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(CLUSTER_READY_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "ElasticsearchContainer".to_string(),
            message: format!(
                "Elasticsearch at {} not ready after {CLUSTER_READY_ATTEMPTS} attempts: {last_error}",
                self.base_url
            ),
        })
    }

    /// Delete leftover test indices (empty or partially filled) from previous
    /// runs so accumulated shards do not degrade the reused container. Only
    /// indices older than [`STALE_INDEX_MAX_AGE_MS`] are removed, which keeps
    /// the sweep safe against tests running concurrently in other processes.
    /// Best-effort: failures are logged, never fail the fixture.
    async fn sweep_stale_indices(&self) {
        #[derive(Deserialize)]
        struct CatIndexEntry {
            index: String,
            #[serde(rename = "creation.date")]
            creation_date: Option<String>,
        }

        // Short timeout, no retries: sweep is best-effort and must not stall setup.
        let Ok(client) = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
        else {
            warn!("Skipping stale index sweep, failed to build HTTP client");
            return;
        };
        let cat_url = format!(
            "{}/_cat/indices/{STALE_INDEX_PATTERNS}?format=json&h=index,creation.date",
            self.base_url
        );

        let entries = match client.get(&cat_url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<Vec<CatIndexEntry>>().await {
                    Ok(entries) => entries,
                    Err(error) => {
                        warn!("Skipping stale index sweep, unparsable _cat response: {error}");
                        return;
                    }
                }
            }
            Ok(response) => {
                warn!(
                    "Skipping stale index sweep, _cat/indices returned {}",
                    response.status()
                );
                return;
            }
            Err(error) => {
                warn!("Skipping stale index sweep, _cat/indices failed: {error}");
                return;
            }
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);

        let stale: Vec<String> = entries
            .into_iter()
            .filter_map(|entry| {
                let created_ms = entry.creation_date.as_deref()?.parse::<u128>().ok()?;
                (now_ms.saturating_sub(created_ms) > STALE_INDEX_MAX_AGE_MS).then_some(entry.index)
            })
            .collect();

        if stale.is_empty() {
            return;
        }

        // Chunked so the URL stays well under limits with many leftovers.
        for chunk in stale.chunks(20) {
            let delete_url = format!("{}/{}", self.base_url, chunk.join(","));
            match client.delete(&delete_url).send().await {
                Ok(response) if response.status().is_success() => {
                    info!("Deleted {} stale Elasticsearch test indices", chunk.len());
                }
                Ok(response) => {
                    warn!(
                        "Failed to delete stale Elasticsearch indices, status {}",
                        response.status()
                    );
                }
                Err(error) => {
                    warn!("Failed to delete stale Elasticsearch indices: {error}");
                }
            }
        }
    }
}

/// Remove the shared reuse container so the next start creates a fresh one.
/// Uses the Docker CLI directly: testcontainers offers no "remove by name" API
/// and the wedged container was created by an earlier process anyway.
fn force_remove_container() {
    let mut child = match Command::new("docker")
        .args(["rm", "-f", ELASTICSEARCH_CONTAINER_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            warn!("Failed to invoke docker rm for '{ELASTICSEARCH_CONTAINER_NAME}': {error}");
            return;
        }
    };

    let deadline = Instant::now() + Duration::from_secs(DOCKER_RM_TIMEOUT_SECS);
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                info!("Removed wedged Elasticsearch container '{ELASTICSEARCH_CONTAINER_NAME}'");
                return;
            }
            Ok(Some(status)) => {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                warn!(
                    "docker rm -f {ELASTICSEARCH_CONTAINER_NAME} failed (exit {status}): {stderr}"
                );
                return;
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                warn!(
                    "docker rm -f {ELASTICSEARCH_CONTAINER_NAME} timed out after {DOCKER_RM_TIMEOUT_SECS}s"
                );
                return;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(error) => {
                warn!("Failed waiting for docker rm '{ELASTICSEARCH_CONTAINER_NAME}': {error}");
                return;
            }
        }
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
