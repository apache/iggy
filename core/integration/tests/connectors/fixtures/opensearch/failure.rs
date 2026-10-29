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

use super::container::{OpenSearchContainer, OpenSearchOps, create_http_client};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use uuid::Uuid;

const PLUGIN_PATH: &str = "../../target/debug/libiggy_connector_opensearch_sink";

const ENV_MISSING_INDEX_URL: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_MISSING_INDEX_PLUGIN_CONFIG_URL";
const ENV_MISSING_INDEX_INDEX: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_MISSING_INDEX_PLUGIN_CONFIG_INDEX";
const ENV_MISSING_INDEX_PATH: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_MISSING_INDEX_PATH";

const ENV_MAPPING_CONFLICT_URL: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_MAPPING_CONFLICT_PLUGIN_CONFIG_URL";
const ENV_MAPPING_CONFLICT_INDEX: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_MAPPING_CONFLICT_PLUGIN_CONFIG_INDEX";
const ENV_MAPPING_CONFLICT_PATH: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_MAPPING_CONFLICT_PATH";

const ENV_HEALTHY_URL: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_HEALTHY_PLUGIN_CONFIG_URL";
const ENV_HEALTHY_INDEX: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_HEALTHY_PLUGIN_CONFIG_INDEX";
const ENV_HEALTHY_PATH: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_HEALTHY_PATH";

/// Three OpenSearch sink connectors sharing one container, wired to prove the
/// failure-state assertions in `opensearch_sink_failures.rs`:
///   * `opensearch_missing_index` targets an index that will never exist, with
///     `create_index_if_not_exists = false` (static in its own TOML), so it
///     fails during `open()` and never starts consuming.
///   * `opensearch_mapping_conflict` has an explicit integer mapping for
///     `count` (static in its own TOML, so the type conflict is deterministic
///     rather than dependent on dynamic-mapping inference order) and
///     subscribes to `TOPIC_2`, receiving a batch containing a real
///     `mapper_parsing_exception`-triggering document partway through the
///     test.
///   * `opensearch_healthy` subscribes to `TOPIC` (a distinct topic in the
///     same stream, so it never sees the conflicting message) and stays
///     `Running` throughout, proving the runtime isolates one failing
///     connector from its siblings.
pub struct OpenSearchFailureFixture {
    container: OpenSearchContainer,
    http_client: HttpClient,
    missing_index: String,
    mapping_conflict_index: String,
    healthy_index: String,
}

impl OpenSearchOps for OpenSearchFailureFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl OpenSearchFailureFixture {
    pub fn mapping_conflict_index(&self) -> &str {
        &self.mapping_conflict_index
    }

    pub fn healthy_index(&self) -> &str {
        &self.healthy_index
    }
}

#[async_trait]
impl TestFixture for OpenSearchFailureFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = OpenSearchContainer::start().await?;
        let http_client = create_http_client();

        Ok(Self {
            container,
            http_client,
            missing_index: format!("iggy_missing_{}", Uuid::new_v4().simple()),
            mapping_conflict_index: format!("iggy_conflict_{}", Uuid::new_v4().simple()),
            healthy_index: format!("iggy_healthy_{}", Uuid::new_v4().simple()),
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_MISSING_INDEX_URL.to_string(),
                self.container.base_url.clone(),
            ),
            (
                ENV_MISSING_INDEX_INDEX.to_string(),
                self.missing_index.clone(),
            ),
            (ENV_MISSING_INDEX_PATH.to_string(), PLUGIN_PATH.to_string()),
            (
                ENV_MAPPING_CONFLICT_URL.to_string(),
                self.container.base_url.clone(),
            ),
            (
                ENV_MAPPING_CONFLICT_INDEX.to_string(),
                self.mapping_conflict_index.clone(),
            ),
            (
                ENV_MAPPING_CONFLICT_PATH.to_string(),
                PLUGIN_PATH.to_string(),
            ),
            (ENV_HEALTHY_URL.to_string(), self.container.base_url.clone()),
            (ENV_HEALTHY_INDEX.to_string(), self.healthy_index.clone()),
            (ENV_HEALTHY_PATH.to_string(), PLUGIN_PATH.to_string()),
        ])
    }
}
