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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, DEFAULT_TEST_TOPIC_2, ENV_SINK_BATCH_MODE,
    ENV_SINK_INCLUDE_METADATA, ENV_SINK_MAX_RETRIES, ENV_SINK_METHOD, ENV_SINK_PATH,
    ENV_SINK_RETRY_DELAY, ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA,
    ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TIMEOUT, ENV_SINK_URL,
    ENV_SINK_VERBOSE_LOGGING, HttpSinkWireMockContainer,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;

/// Base HTTP sink fixture — individual batch mode with metadata enabled.
pub struct HttpSinkIndividualFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkIndividualFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }

    fn base_envs(container: &HttpSinkWireMockContainer) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_URL.to_string(),
            format!("{}/ingest", container.base_url),
        );
        envs.insert(ENV_SINK_METHOD.to_string(), "POST".to_string());
        envs.insert(ENV_SINK_BATCH_MODE.to_string(), "individual".to_string());
        envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(ENV_SINK_TIMEOUT.to_string(), "10s".to_string());
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "1".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "100ms".to_string());
        envs.insert(ENV_SINK_VERBOSE_LOGGING.to_string(), "true".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "http_sink_cg".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_http_sink".to_string(),
        );
        envs
    }
}

#[async_trait]
impl TestFixture for HttpSinkIndividualFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        Self::base_envs(&self.container)
    }
}

/// HTTP sink fixture with NDJSON batch mode.
pub struct HttpSinkNdjsonFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkNdjsonFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for HttpSinkNdjsonFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HttpSinkIndividualFixture::base_envs(&self.container);
        envs.insert(ENV_SINK_BATCH_MODE.to_string(), "ndjson".to_string());
        envs
    }
}

/// HTTP sink fixture with JSON array batch mode.
pub struct HttpSinkJsonArrayFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkJsonArrayFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for HttpSinkJsonArrayFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HttpSinkIndividualFixture::base_envs(&self.container);
        envs.insert(ENV_SINK_BATCH_MODE.to_string(), "json_array".to_string());
        envs
    }
}

/// HTTP sink fixture with raw batch mode (binary payloads).
pub struct HttpSinkRawFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkRawFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for HttpSinkRawFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HttpSinkIndividualFixture::base_envs(&self.container);
        envs.insert(ENV_SINK_BATCH_MODE.to_string(), "raw".to_string());
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "raw".to_string());
        envs
    }
}

/// HTTP sink fixture with metadata disabled.
pub struct HttpSinkNoMetadataFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkNoMetadataFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for HttpSinkNoMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HttpSinkIndividualFixture::base_envs(&self.container);
        envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), "false".to_string());
        envs
    }
}

/// HTTP sink fixture subscribed to two topics on the same stream.
/// Demonstrates the multi-topic single-connector deployment pattern.
pub struct HttpSinkMultiTopicFixture {
    container: HttpSinkWireMockContainer,
}

impl HttpSinkMultiTopicFixture {
    pub fn container(&self) -> &HttpSinkWireMockContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for HttpSinkMultiTopicFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = HttpSinkWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HttpSinkIndividualFixture::base_envs(&self.container);
        // Subscribe to both topics — runtime spawns one task per topic
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{},{}]", DEFAULT_TEST_TOPIC, DEFAULT_TEST_TOPIC_2),
        );
        envs
    }
}
