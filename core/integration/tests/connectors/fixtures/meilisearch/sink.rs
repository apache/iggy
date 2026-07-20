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

use super::container::{MeilisearchContainer, MeilisearchOps, TEST_INDEX, create_http_client};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture, seeds};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;

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
            (ENV_SINK_INDEX.to_string(), TEST_INDEX.to_string()),
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
                "meilisearch_sink".to_string(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_meilisearch_sink".to_string(),
            ),
        ])
    }
}
