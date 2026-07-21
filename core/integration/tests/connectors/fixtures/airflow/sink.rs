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
    AirflowWireMockContainer, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_AUTH,
    ENV_SINK_BASE_URL, ENV_SINK_DAG_ID, ENV_SINK_HEALTH_CHECK, ENV_SINK_INCLUDE_IGGY_META,
    ENV_SINK_MAX_RETRIES, ENV_SINK_PATH, ENV_SINK_RETRY_DELAY, ENV_SINK_STREAMS_0_CONSUMER_GROUP,
    ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS,
    ENV_SINK_TIMEOUT, ENV_SINK_VERBOSE_LOGGING,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;

/// Default Airflow sink fixture: unauthenticated WireMock backend, fixed DAG id.
pub struct AirflowSinkFixture {
    container: AirflowWireMockContainer,
}

impl AirflowSinkFixture {
    pub fn container(&self) -> &AirflowWireMockContainer {
        &self.container
    }

    fn base_envs(container: &AirflowWireMockContainer) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_BASE_URL.to_string(), container.base_url.clone());
        envs.insert(ENV_SINK_DAG_ID.to_string(), "example_dag".to_string());
        envs.insert(ENV_SINK_AUTH.to_string(), "none".to_string());
        envs.insert(ENV_SINK_TIMEOUT.to_string(), "10s".to_string());
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "1".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "100ms".to_string());
        envs.insert(ENV_SINK_HEALTH_CHECK.to_string(), "true".to_string());
        envs.insert(ENV_SINK_VERBOSE_LOGGING.to_string(), "true".to_string());
        envs.insert(ENV_SINK_INCLUDE_IGGY_META.to_string(), "true".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{DEFAULT_TEST_TOPIC}]"),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "airflow_sink_cg".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_airflow_sink".to_string(),
        );
        envs
    }
}

#[async_trait]
impl TestFixture for AirflowSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = AirflowWireMockContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        Self::base_envs(&self.container)
    }
}
