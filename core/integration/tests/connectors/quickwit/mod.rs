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

use anyhow::{Result, anyhow};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::connectors::{
    ConnectorsRuntime, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, IggySetup, setup_runtime,
};

mod quickwit_container {
    pub static REPOSITORY: &str = "quickwit/quickwit";
    pub static VERSION: &str = "0.8.1";
    pub static LISTENING_IPV4: &str = "0.0.0.0";
    pub static LISTENING_PORT: u16 = 7280;
    pub static READY_MESSAGE: &str = "REST server is ready";
}

mod quickwit_paths {
    pub static INDEXES: &str = "api/v1/indexes";

    pub fn index_ingest(index_id: &str) -> String {
        format!("api/v1/{index_id}/ingest")
    }

    pub fn index_search(index_id: &str) -> String {
        format!("api/v1/{index_id}/search")
    }
}

mod quickwit_responses {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct IndexSearch {
        pub hits: Vec<serde_json::Value>,
        pub num_hits: usize,
        #[allow(dead_code)]
        pub elapsed_time_micros: usize,
    }
}

struct QuickwitTestSetup {
    quickwit: ContainerAsync<GenericImage>,
    runtime: ConnectorsRuntime,
    reqwest: HttpClient,
}

impl QuickwitTestSetup {
    async fn try_new() -> Result<Self> {
        let quickwit_container = start_quickwit_container().await;
        let connectors_runtime = start_iggy_with_quickwit_connector(&quickwit_container).await?;
        Ok(Self {
            quickwit: quickwit_container,
            runtime: connectors_runtime,
            reqwest: get_http_client_with_retries(),
        })
    }

    async fn try_new_with_precreate_index(
        quickwit_container: ContainerAsync<GenericImage>,
    ) -> Result<Self> {
        let http_client = get_http_client_with_retries();
        create_quickwit_test_index(&quickwit_container, &http_client).await?;
        let connectors_runtime = start_iggy_with_quickwit_connector(&quickwit_container).await?;
        Ok(Self {
            quickwit: quickwit_container,
            runtime: connectors_runtime,
            reqwest: http_client,
        })
    }

    async fn get_mapped_quickwit_port(&self) -> Result<u16> {
        get_mapped_quickwit_port(&self.quickwit).await
    }

    async fn get_quickwit_test_index_all_search(&self) -> Result<quickwit_responses::IndexSearch> {
        let match_all = "";
        let descending = "-";
        let search_response = self
            .reqwest
            .get(format!(
                "http://localhost:{}/{}",
                self.get_mapped_quickwit_port().await?,
                quickwit_paths::index_search(DEFAULT_TEST_TOPIC)
            ))
            .query(&[("query", match_all)])
            .query(&[("sort_by", format!(r#"{descending}{INDEX_TIMESTAMP_FIELD}"#))])
            .send()
            .await?;
        info!("Received search index response.");
        assert!(search_response.status().is_success());
        search_response
            .json::<quickwit_responses::IndexSearch>()
            .await
            .map_err(Into::into)
    }

    async fn flush_quickwit_test_index(&self) -> Result<()> {
        let ingest_response = self
            .reqwest
            .post(format!(
                "http://localhost:{}/{}",
                self.get_mapped_quickwit_port().await?,
                quickwit_paths::index_ingest(DEFAULT_TEST_TOPIC)
            ))
            .query(&[("commit", "force")])
            .json("{}")
            .send()
            .await?;
        info!("Received index ingest response.");
        assert!(ingest_response.status().is_success());
        Ok(())
    }
}

fn get_http_client_with_retries() -> HttpClient {
    let max_retries = 3;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(max_retries);
    reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

async fn get_mapped_quickwit_port(
    quickwit_container: &ContainerAsync<GenericImage>,
) -> Result<u16> {
    let mapped_port = quickwit_container
        .ports()
        .await?
        .map_to_host_port_ipv4(quickwit_container::LISTENING_PORT)
        .ok_or(anyhow!("No mapping for Quickwit port."))?;
    info!("Got ports details from container.");
    Ok(mapped_port)
}

static INDEX_TIMESTAMP_FIELD: &str = "timestamp";

fn get_quickwit_index_config() -> String {
    format!(
        r#"
    version: 0.8
    index_id: {DEFAULT_TEST_TOPIC}
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

const STREAMS_KEY_PREFIX: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_STREAMS";

fn get_stream_0_overrides() -> HashMap<String, String> {
    HashMap::from([
        (
            format!("{STREAMS_KEY_PREFIX}_0_STREAM"),
            DEFAULT_TEST_STREAM.to_string(),
        ),
        (
            format!("{STREAMS_KEY_PREFIX}_0_TOPICS"),
            format!("[{DEFAULT_TEST_TOPIC}]"),
        ),
        (format!("{STREAMS_KEY_PREFIX}_0_SCHEMA"), "json".to_string()),
    ])
}

async fn create_quickwit_test_index(
    quickwit_container: &ContainerAsync<GenericImage>,
    http_client: &HttpClient,
) -> Result<()> {
    let create_response = http_client
        .post(format!(
            "http://localhost:{}/{}",
            get_mapped_quickwit_port(quickwit_container).await?,
            quickwit_paths::INDEXES
        ))
        .header("Content-Type", "application/yaml")
        .body(get_quickwit_index_config())
        .send()
        .await?;
    info!("Received create index response.");
    assert!(create_response.status().is_success());
    Ok(())
}

static NETWORK_NAME_PREFIX: &str = "iggy-quickwit-sink";

async fn start_quickwit_container() -> ContainerAsync<GenericImage> {
    let unique_network = format!("{NETWORK_NAME_PREFIX}-{}", Uuid::new_v4());
    let random_host = 0;
    let quickwit_container =
        GenericImage::new(quickwit_container::REPOSITORY, quickwit_container::VERSION)
            .with_exposed_port(random_host.tcp())
            .with_wait_for(WaitFor::message_on_stdout(
                quickwit_container::READY_MESSAGE,
            ))
            .with_network(unique_network)
            .with_cmd(["run"])
            .with_env_var("QW_LISTEN_ADDRESS", quickwit_container::LISTENING_IPV4)
            .with_mapped_port(random_host, quickwit_container::LISTENING_PORT.tcp())
            .start()
            .await
            .expect("Quickwit started.");
    info!("Started quickwit container.");
    quickwit_container
}

const PLUGIN_KEY_PREFIX: &str = "IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG";

async fn start_iggy_with_quickwit_connector(
    quickwit_container: &ContainerAsync<GenericImage>,
) -> Result<ConnectorsRuntime> {
    let mapped_port = get_mapped_quickwit_port(quickwit_container).await?;

    let plugin_overrides = HashMap::from([
        (
            format!("{PLUGIN_KEY_PREFIX}_URL"),
            format!("http://localhost:{mapped_port}"),
        ),
        (
            format!("{PLUGIN_KEY_PREFIX}_INDEX"),
            get_quickwit_index_config(),
        ),
    ]);
    let mut extra_envs = HashMap::new();
    extra_envs.extend(plugin_overrides);
    extra_envs.extend(get_stream_0_overrides());

    let mut connectors_runtime = setup_runtime();
    let runtime_config = "quickwit/config.toml";
    connectors_runtime
        .init(runtime_config, Some(extra_envs), IggySetup::default())
        .await;

    Ok(connectors_runtime)
}
