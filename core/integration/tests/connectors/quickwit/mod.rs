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

mod quickwit_sink;

use crate::connectors::{ConnectorsRuntime, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, IggySetup, setup_runtime};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;

// Test configuration constants
const TEST_INDEX: &str = "iggy_test";
const TEST_STREAM: &str = DEFAULT_TEST_STREAM;
const TEST_TOPIC: &str = DEFAULT_TEST_TOPIC;
const QUICKWIT_PORT: u16 = 7280;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 100;

// Environment variable names for Quickwit sink connector configuration
const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG_URL";
const ENV_SINK_INDEX: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG_INDEX";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PATH";

/// Minimal Quickwit index configuration for testing.
/// Uses dynamic mode to accept any JSON structure.
const TEST_INDEX_CONFIG: &str = r#"
version: 0.9
index_id: iggy_test

doc_mapping:
  mode: dynamic
  field_mappings:
    - name: message
      type: text
      tokenizer: default

indexing_settings:
  commit_timeout_secs: 5
"#;

/// Test setup containing the runtime, Quickwit URL, and container reference.
pub struct QuickwitTestSetup {
    pub runtime: ConnectorsRuntime,
    pub url: String,
    http_client: reqwest::Client,
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
}

/// Starts a Quickwit container and returns the container and its base URL.
async fn start_container() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("quickwit/quickwit", "latest")
        .with_exposed_port(QUICKWIT_PORT.tcp())
        .with_cmd(["run"])
        .with_wait_for(WaitFor::message_on_stdout("REST server listening"))
        .start()
        .await
        .expect("Failed to start Quickwit container");

    let host = container
        .get_host()
        .await
        .expect("Quickwit container should have a host");
    let host_port = container
        .get_host_port_ipv4(QUICKWIT_PORT)
        .await
        .expect("Failed to get Quickwit port");

    let url = format!("http://{host}:{host_port}");
    (container, url)
}

/// Sets up the Quickwit sink test with full container and connector runtime.
pub async fn setup_quickwit_sink() -> QuickwitTestSetup {
    let (container, url) = start_container().await;

    let mut envs = HashMap::new();
    envs.insert(ENV_SINK_URL.to_owned(), url.clone());
    envs.insert(ENV_SINK_INDEX.to_owned(), TEST_INDEX_CONFIG.to_owned());
    envs.insert(ENV_SINK_STREAMS_0_STREAM.to_owned(), TEST_STREAM.to_owned());
    envs.insert(
        ENV_SINK_STREAMS_0_TOPICS.to_owned(),
        format!("[{TEST_TOPIC}]"),
    );
    envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_owned(), "json".to_owned());
    envs.insert(
        ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_owned(),
        "test".to_owned(),
    );
    envs.insert(
        ENV_SINK_PATH.to_owned(),
        "../../target/debug/libiggy_connector_quickwit_sink".to_owned(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init("quickwit/sink.toml", Some(envs), IggySetup::default())
        .await;

    QuickwitTestSetup {
        runtime,
        url,
        http_client: reqwest::Client::new(),
        container,
    }
}

impl QuickwitTestSetup {
    /// Searches for documents in the Quickwit index.
    /// Returns the list of document hits.
    pub async fn search_documents(&self, query: &str) -> Vec<serde_json::Value> {
        let search_url = format!("{}/api/v1/{}/search?query={}", self.url, TEST_INDEX, query);

        let response = self
            .http_client
            .get(&search_url)
            .send()
            .await
            .expect("Failed to send search request to Quickwit");

        if !response.status().is_success() {
            return Vec::new();
        }

        let body: serde_json::Value = response
            .json()
            .await
            .expect("Failed to parse Quickwit search response");

        body.get("hits")
            .and_then(|h| h.as_array())
            .cloned()
            .unwrap_or_default()
    }

    /// Waits for documents to appear in the Quickwit index.
    /// Polls until the expected count is reached or attempts are exhausted.
    pub async fn wait_for_documents(&self, expected_count: usize) -> Vec<serde_json::Value> {
        for _ in 0..POLL_ATTEMPTS {
            let docs = self.search_documents("*").await;
            if docs.len() >= expected_count {
                return docs;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        // Return whatever we have, test will assert on count
        self.search_documents("*").await
    }
}
