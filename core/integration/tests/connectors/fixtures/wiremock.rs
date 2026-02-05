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

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use testcontainers_modules::testcontainers::core::WaitFor::Healthcheck;
use testcontainers_modules::testcontainers::core::wait::HealthWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const WIREMOCK_IMAGE: &str = "wiremock/wiremock";
const WIREMOCK_TAG: &str = "3.13.2";
const WIREMOCK_PORT: u16 = 8080;

struct WireMockContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    base_url: String,
}

impl WireMockContainer {
    async fn start(mode: &str) -> Result<Self, TestBinaryError> {
        let current_dir = std::env::current_dir().map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "WireMockContainer".to_string(),
            message: format!("Failed to get current dir: {e}"),
        })?;

        let container = GenericImage::new(WIREMOCK_IMAGE, WIREMOCK_TAG)
            .with_exposed_port(WIREMOCK_PORT.tcp())
            .with_wait_for(Healthcheck(HealthWaitStrategy::default()))
            .with_mount(Mount::bind_mount(
                current_dir
                    .join(format!(
                        "tests/connectors/http_config_provider/wiremock/mappings/{mode}"
                    ))
                    .to_string_lossy()
                    .to_string(),
                "/home/wiremock/mappings",
            ))
            .with_mount(Mount::bind_mount(
                current_dir
                    .join("tests/connectors/http_config_provider/wiremock/__files")
                    .to_string_lossy()
                    .to_string(),
                "/home/wiremock/__files",
            ))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "WireMockContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host = container
            .get_host()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "WireMockContainer".to_string(),
                message: format!("Failed to get host: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(WIREMOCK_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "WireMockContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let base_url = format!("http://{host}:{host_port}");

        Ok(Self {
            container,
            base_url,
        })
    }
}

/// WireMock fixture for "direct" response format tests.
pub struct WireMockDirectFixture {
    container: WireMockContainer,
}

#[async_trait]
impl TestFixture for WireMockDirectFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = WireMockContainer::start("direct").await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            "IGGY_CONNECTORS_CONNECTORS_BASE_URL".to_string(),
            self.container.base_url.clone(),
        );
        envs
    }
}

/// WireMock fixture for "wrapped" response format tests.
pub struct WireMockWrappedFixture {
    container: WireMockContainer,
}

#[async_trait]
impl TestFixture for WireMockWrappedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = WireMockContainer::start("wrapped").await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            "IGGY_CONNECTORS_CONNECTORS_BASE_URL".to_string(),
            self.container.base_url.clone(),
        );
        envs
    }
}
