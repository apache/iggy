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

use std::path::PathBuf;

use crate::connectors::fixtures;
use integration::harness::TestBinaryError;
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt, ReuseDirective,
};
use tracing::info;

const QUESTDB_IMAGE: &str = "docker.io/questdb/questdb";
/// Fixed name plus `ReuseDirective::Always` shares one server across every
/// QuestDB test process on the Docker daemon, as the Elasticsearch and Doris
/// fixtures do. Tests stay isolated through a table per fixture. The container
/// outlives the run: `docker rm -f iggy-test-questdb` forces a fresh one.
const QUESTDB_SHARED_CONTAINER_NAME: &str = "iggy-test-questdb";
/// Bounds the create-or-attach race in [`start_shared_container`], with the
/// same values as the Doris and Elasticsearch fixtures.
const CONTAINER_START_ATTEMPTS: u32 = 30;
const CONTAINER_START_RETRY_DELAY_MS: u64 = 1_000;
/// QWP over WebSocket requires QuestDB 10.0 or newer, so this tag cannot be
/// rolled back to a 9.x line without disabling the whole suite.
const QUESTDB_TAG: &str = "10.0.1";
/// QuestDB serves the REST API and the QWP WebSocket upgrade on the same port.
const QUESTDB_HTTP_PORT: u16 = 9000;

/// Emitted once the server is accepting connections.
const QUESTDB_READY_LOG: &str = "server-main enjoy";

/// Attempts to resolve the published host port before giving up.
const PORT_LOOKUP_ATTEMPTS: usize = 30;
const PORT_LOOKUP_INTERVAL_MS: u64 = 200;

pub const HEALTH_CHECK_ATTEMPTS: usize = 60;
pub const HEALTH_CHECK_INTERVAL_MS: u64 = 1_000;

pub const DEFAULT_TEST_STREAM: &str = "test_stream";
pub const DEFAULT_TEST_TOPIC: &str = "test_topic";

// ── env-var keys injected into the connectors runtime ────────────────────────

pub const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PATH";

/// Where the connectors runtime looks for the plugin, relative to its working
/// directory. Cargo runs the test with the same working directory, so the same
/// relative path resolves here.
pub const SINK_PLUGIN_PATH: &str = "../../target/debug/libiggy_connector_questdb_sink";
pub const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_CONNECTION_STRING";
pub const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TABLE";
pub const ENV_SINK_TIMESTAMP_SOURCE: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_SOURCE";
pub const ENV_SINK_TIMESTAMP_FIELD: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_FIELD";
pub const ENV_SINK_TIMESTAMP_UNIT: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_TIMESTAMP_UNIT";
pub const ENV_SINK_SYMBOL_COLUMNS: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_SYMBOL_COLUMNS";
pub const ENV_SINK_UUID_COLUMNS: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_UUID_COLUMNS";
pub const ENV_SINK_INTEGER_COLUMNS: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INTEGER_COLUMNS";
pub const ENV_SINK_INCLUDE_STREAM_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_STREAM_COLUMN";
pub const ENV_SINK_INCLUDE_TOPIC_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_TOPIC_COLUMN";
pub const ENV_SINK_INCLUDE_PARTITION_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_PARTITION_COLUMN";
pub const ENV_SINK_INCLUDE_OFFSET_COLUMN: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_OFFSET_COLUMN";
pub const ENV_SINK_INCLUDE_HEADERS: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_INCLUDE_HEADERS";
pub const ENV_SINK_NUMBERS_AS_DOUBLE: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_NUMBERS_AS_DOUBLE";
pub const ENV_SINK_LOG_REJECTED_PAYLOAD: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_LOG_REJECTED_PAYLOAD";
pub const ENV_SINK_BATCH_SIZE: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_BATCH_SIZE";
pub const ENV_SINK_FLUSH_TIMEOUT: &str = "IGGY_CONNECTORS_SINK_QUESTDB_PLUGIN_CONFIG_FLUSH_TIMEOUT";
pub const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_STREAM";
pub const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_TOPICS";
pub const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_SCHEMA";
pub const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_QUESTDB_STREAMS_0_CONSUMER_GROUP";

/// Fails with an actionable message when the plugin has not been built.
///
/// Cargo has no dependency edge to a plugin: the runtime `dlopen`s it at a
/// path, so `cargo test` never builds it. Without this check the runtime
/// starts, fails to load the connector, and the test dies somewhere far away
/// from the cause, typically as a timeout waiting for rows that were never
/// going to arrive.
pub fn ensure_plugin_built() -> Result<(), TestBinaryError> {
    let setup_error = |message: String| TestBinaryError::FixtureSetup {
        fixture_type: "QuestDbContainer".to_string(),
        message,
    };
    // The runtime appends the platform's suffix, so the configured path has
    // none and every candidate has to be tried.
    let built = ["dylib", "so", "dll"]
        .iter()
        .map(|extension| PathBuf::from(format!("{SINK_PLUGIN_PATH}.{extension}")))
        .any(|candidate| candidate.exists());
    if !built {
        return Err(setup_error(format!(
            "the QuestDB sink plugin is not built, so the connectors runtime would have \
             nothing to load. Run `cargo build -p iggy_connector_questdb_sink` first. \
             Looked for {SINK_PLUGIN_PATH}.{{dylib,so,dll}}"
        )));
    }
    Ok(())
}

// ── Container ────────────────────────────────────────────────────────────────

pub struct QuestDbContainer {
    // With `ReuseDirective::Always`, dropping this handle leaves the shared
    // container running for the next test to attach to.
    container: ContainerAsync<GenericImage>,
    shared: bool,
    pub base_url: String,
    pub host_port: u16,
}

/// The container request both start paths use.
///
/// It waits on the startup log rather than an HTTP probe. An HTTP wait has to
/// resolve the mapped host port while the container is still starting, and
/// Docker occasionally has not published the mapping yet, which fails the whole
/// fixture with "does not expose port 9000/tcp". The log line needs no port,
/// and readiness is confirmed by the `/ping` poll in the sink fixture anyway.
/// That poll also covers an attach to the shared container, which skips this
/// wait.
fn container_request(name: &str) -> ContainerRequest<GenericImage> {
    GenericImage::new(QUESTDB_IMAGE, QUESTDB_TAG)
        .with_exposed_port(QUESTDB_HTTP_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(QUESTDB_READY_LOG))
        .with_mapped_port(0, QUESTDB_HTTP_PORT.tcp())
        .with_container_name(name)
}

/// Creates the shared QuestDB container, or attaches to it if another test won
/// the race.
///
/// `ReuseDirective::Always` inspects the daemon and then creates when nothing
/// matches, with no lock across the two steps. On a cold daemon with several
/// QuestDB tests in flight, one creates the container and the rest get
/// `409 Conflict: name already in use`. Retrying turns those into attaches.
async fn start_shared_container() -> Result<ContainerAsync<GenericImage>, TestBinaryError> {
    let mut conflict = String::new();
    for attempt in 1..=CONTAINER_START_ATTEMPTS {
        // Rebuilt per attempt because `start` consumes the request.
        let result = container_request(QUESTDB_SHARED_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await;
        match result {
            Ok(container) => return Ok(container),
            Err(error) => {
                let message = error.to_string();
                if !message.contains("is already in use") {
                    return Err(TestBinaryError::FixtureSetup {
                        fixture_type: "QuestDbContainer".to_string(),
                        message: format!("Failed to start container: {message}"),
                    });
                }
                info!(
                    "QuestDB container name taken by another test (attempt {attempt}), retrying to attach"
                );
                conflict = message;
                tokio::time::sleep(std::time::Duration::from_millis(
                    CONTAINER_START_RETRY_DELAY_MS,
                ))
                .await;
            }
        }
    }
    Err(TestBinaryError::FixtureSetup {
        fixture_type: "QuestDbContainer".to_string(),
        message: format!(
            "Failed to attach to container '{QUESTDB_SHARED_CONTAINER_NAME}' after \
             {CONTAINER_START_ATTEMPTS} attempts: {conflict}"
        ),
    })
}

impl QuestDbContainer {
    /// Attaches to the QuestDB server that every test shares.
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = start_shared_container().await?;
        Self::from_container(container, true).await
    }

    /// Starts a server of its own, for a test that pauses QuestDB and would
    /// otherwise freeze every other test on the shared one.
    pub async fn start_dedicated() -> Result<Self, TestBinaryError> {
        let container = container_request(&fixtures::unique_container_name("questdb"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "QuestDbContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;
        Self::from_container(container, false).await
    }

    async fn from_container(
        container: ContainerAsync<GenericImage>,
        shared: bool,
    ) -> Result<Self, TestBinaryError> {
        // The mapping can lag the container being up, so this retries rather
        // than failing the fixture on the first miss.
        let mut host_port = None;
        for _ in 0..PORT_LOOKUP_ATTEMPTS {
            if let Ok(ports) = container.ports().await
                && let Some(port) = ports
                    .map_to_host_port_ipv4(QUESTDB_HTTP_PORT)
                    .or_else(|| ports.map_to_host_port_ipv6(QUESTDB_HTTP_PORT))
            {
                host_port = Some(port);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(PORT_LOOKUP_INTERVAL_MS)).await;
        }
        let host_port = host_port.ok_or_else(|| TestBinaryError::FixtureSetup {
            fixture_type: "QuestDbContainer".to_string(),
            message: format!(
                "No host mapping for port {QUESTDB_HTTP_PORT} after {PORT_LOOKUP_ATTEMPTS} attempts"
            ),
        })?;

        let base_url = format!("http://localhost:{host_port}");
        info!("QuestDB container available at {base_url}");

        Ok(Self {
            container,
            shared,
            base_url,
            host_port,
        })
    }

    /// Connect string handed to the sink. QWP shares the REST port.
    pub fn connection_string(&self) -> String {
        format!("ws::addr=localhost:{};", self.host_port)
    }

    /// Freeze the server to simulate an outage.
    ///
    /// Pause rather than stop: the container was published with an ephemeral
    /// host port (`-p 0:9000`), and Docker re-resolves that to a *different*
    /// port when a stopped container is started again, which would strand both
    /// the sink and this fixture on a dead address. Pausing leaves networking
    /// untouched, so the port survives and the sink reconnects to the same
    /// endpoint.
    pub async fn simulate_outage(&self) -> Result<(), TestBinaryError> {
        if self.shared {
            return Err(TestBinaryError::InvalidState {
                message: "refusing to pause the shared QuestDB container, which other tests \
                          use concurrently; start the fixture with a dedicated container"
                    .to_string(),
            });
        }
        self.container
            .pause()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to pause QuestDB container: {e}"),
            })
    }

    pub async fn resume(&self) -> Result<(), TestBinaryError> {
        self.container
            .unpause()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to unpause QuestDB container: {e}"),
            })
    }
}

// ── HTTP client ───────────────────────────────────────────────────────────────

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

// ── Shared QuestDB operations ─────────────────────────────────────────────────

pub trait QuestDbOps: Sync {
    fn container(&self) -> &QuestDbContainer;
    fn http_client(&self) -> &HttpClient;

    /// Run SQL through the REST `/exec` endpoint and return the parsed JSON.
    fn exec(
        &self,
        query: &str,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, TestBinaryError>> + Send {
        async move {
            let url = format!("{}/exec", self.container().base_url);
            let response = self
                .http_client()
                .get(&url)
                .query(&[("query", query)])
                .send()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to query QuestDB: {e}"),
                })?;

            let text = response.text().await.unwrap_or_default();
            serde_json::from_str(&text).map_err(|e| TestBinaryError::InvalidState {
                message: format!("QuestDB returned non-JSON for `{query}`: {e}. Body: {text}"),
            })
        }
    }

    /// Row count for `table`, or `None` while the table does not exist yet.
    /// QWP auto-creates on first write, so "missing" is a normal early state
    /// rather than a failure.
    fn count_rows(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<Option<usize>, TestBinaryError>> + Send {
        async move {
            let value = self.exec(&format!("select count() from '{table}'")).await?;
            if value.get("error").is_some() {
                return Ok(None);
            }
            let count = value
                .get("dataset")
                .and_then(|rows| rows.get(0))
                .and_then(|row| row.get(0))
                .and_then(serde_json::Value::as_u64);
            Ok(count.map(|count| count as usize))
        }
    }
}
