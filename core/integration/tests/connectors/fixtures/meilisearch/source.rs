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

const ENV_SOURCE_URL: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_PLUGIN_CONFIG_URL";
const ENV_SOURCE_INDEX: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_PLUGIN_CONFIG_INDEX";
const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_MEILISEARCH_PLUGIN_CONFIG_INCLUDE_METADATA";
const ENV_SOURCE_POLLING_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_MEILISEARCH_PLUGIN_CONFIG_POLLING_INTERVAL";
const ENV_SOURCE_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_STREAMS_0_STREAM";
const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_STREAMS_0_TOPIC";
const ENV_SOURCE_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_STREAMS_0_SCHEMA";
const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_MEILISEARCH_PATH";

pub struct MeilisearchSourceFixture {
    container: MeilisearchContainer,
    http_client: HttpClient,
}

impl MeilisearchOps for MeilisearchSourceFixture {
    fn container(&self) -> &MeilisearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

#[async_trait]
impl TestFixture for MeilisearchSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = MeilisearchContainer::start().await?;
        let http_client = create_http_client();
        let fixture = Self {
            container,
            http_client,
        };
        fixture.create_source_index().await?;

        Ok(fixture)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_SOURCE_URL.to_string(), self.container.base_url.clone()),
            (ENV_SOURCE_INDEX.to_string(), TEST_INDEX.to_string()),
            (ENV_SOURCE_INCLUDE_METADATA.to_string(), "false".to_string()),
            (ENV_SOURCE_POLLING_INTERVAL.to_string(), "25ms".to_string()),
            (
                ENV_SOURCE_STREAMS_0_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
                seeds::names::TOPIC.to_string(),
            ),
            (ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SOURCE_PATH.to_string(),
                "../../target/debug/libiggy_connector_meilisearch_source".to_string(),
            ),
        ])
    }
}
