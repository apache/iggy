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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, DEFAULT_TEST_TOPIC_2, OpenSearchContainer,
    OpenSearchOps, create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_PLUGIN_CONFIG_URL";
const ENV_SINK_INDEX: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_PLUGIN_CONFIG_INDEX";
const ENV_SINK_DOCUMENT_ID_FIELD: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_PLUGIN_CONFIG_DOCUMENT_ID_FIELD";
// Without this, a write is not visible to `_search`/`_count` until the next
// index refresh (default ~1s), which makes read-after-write assertions
// flaky. `wait_for` blocks the bulk response until the write is visible,
// exercising the `refresh` config option end-to-end.
const ENV_SINK_REFRESH: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_PLUGIN_CONFIG_REFRESH";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_0_CONSUMER_GROUP";
// A second stream entry, subscribed to a distinct topic under the `raw`
// schema, so the same live-server test can prove `document_from_raw`'s two
// branches (valid-JSON-as-raw-bytes, and the base64 fallback for non-JSON
// bytes) alongside the `json`-schema coverage on stream 0.
const ENV_SINK_STREAMS_1_STREAM: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_1_STREAM";
const ENV_SINK_STREAMS_1_TOPICS: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_1_TOPICS";
const ENV_SINK_STREAMS_1_SCHEMA: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_1_SCHEMA";
const ENV_SINK_STREAMS_1_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_OPENSEARCH_STREAMS_1_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_OPENSEARCH_PATH";

const SINK_INDEX_PREFIX: &str = "iggy_messages";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;

pub struct OpenSearchSinkFixture {
    container: OpenSearchContainer,
    http_client: HttpClient,
    // Unique per fixture so tests sharing the reused container never collide
    // on the same index. The connector writes here via ENV_SINK_INDEX.
    index: String,
}

impl OpenSearchOps for OpenSearchSinkFixture {
    fn container(&self) -> &OpenSearchContainer {
        &self.container
    }

    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl OpenSearchSinkFixture {
    pub fn index(&self) -> &str {
        &self.index
    }

    pub async fn wait_for_document_count(
        &self,
        expected_count: u64,
    ) -> Result<u64, TestBinaryError> {
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(count) = self.count_documents(&self.index).await
                && count >= expected_count
            {
                info!("Found {count} documents in OpenSearch (expected {expected_count})");
                return Ok(count);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        let final_count = self.count_documents(&self.index).await.unwrap_or(0);
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected_count} documents, found {final_count} after {POLL_ATTEMPTS} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for OpenSearchSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = OpenSearchContainer::start().await?;
        let http_client = create_http_client();
        let index = format!("{SINK_INDEX_PREFIX}_{}", Uuid::new_v4().simple());

        Ok(Self {
            container,
            http_client,
            index,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_SINK_URL.to_string(), self.container.base_url.clone()),
            (ENV_SINK_INDEX.to_string(), self.index.clone()),
            (
                ENV_SINK_DOCUMENT_ID_FIELD.to_string(),
                "order_id".to_string(),
            ),
            (ENV_SINK_REFRESH.to_string(), "wait_for".to_string()),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                DEFAULT_TEST_STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{DEFAULT_TEST_TOPIC}]"),
            ),
            (ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
                "opensearch_sink".to_string(),
            ),
            (
                ENV_SINK_STREAMS_1_STREAM.to_string(),
                DEFAULT_TEST_STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_1_TOPICS.to_string(),
                format!("[{DEFAULT_TEST_TOPIC_2}]"),
            ),
            (ENV_SINK_STREAMS_1_SCHEMA.to_string(), "raw".to_string()),
            (
                ENV_SINK_STREAMS_1_CONSUMER_GROUP.to_string(),
                "opensearch_sink_raw".to_string(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_opensearch_sink".to_string(),
            ),
        ])
    }
}
