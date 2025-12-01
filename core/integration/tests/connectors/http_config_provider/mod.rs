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

mod direct_responses;
mod wrapped_responses;

use crate::connectors::{ConnectorsRuntime, IggySetup, setup_runtime};
use std::collections::HashMap;
use strum_macros::Display;
use testcontainers_modules::testcontainers::core::WaitFor::Healthcheck;
use testcontainers_modules::testcontainers::core::wait::HealthWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn setup(mode: WireMockMode) -> WiremockConnectorsRuntime {
    let iggy_setup = IggySetup::default();
    let mut runtime = setup_runtime();

    let container = create_wiremock_container(mode).await;
    let base_url = get_base_url(&container).await;

    let mut envs = HashMap::new();
    envs.insert("IGGY_CONNECTORS_CONNECTORS_BASE_URL".to_owned(), base_url);

    let config_path = match mode {
        WireMockMode::Direct => "http_config_provider/config_direct.toml",
        WireMockMode::Wrapped => "http_config_provider/config_wrapped.toml",
    };

    runtime.init(config_path, Some(envs), iggy_setup).await;
    WiremockConnectorsRuntime {
        connectors_runtime: runtime,
        wiremock_container: container,
    }
}

async fn get_base_url(container: &ContainerAsync<GenericImage>) -> String {
    let host = container
        .get_host()
        .await
        .expect("WireMock container should have a host");
    let host_port = container
        .get_host_port_ipv4(8080)
        .await
        .expect("WireMock container should have a port");
    format!("http://{host}:{host_port}")
}

async fn create_wiremock_container(mode: WireMockMode) -> ContainerAsync<GenericImage> {
    let current_dir = std::env::current_dir().unwrap();
    GenericImage::new("wiremock/wiremock", "latest")
        .with_exposed_port(8080.tcp())
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
        .expect("WireMock container should be started")
}

#[derive(Display, Clone, Copy)]
#[strum(serialize_all = "lowercase")]
enum WireMockMode {
    Direct,
    Wrapped,
}

struct WiremockConnectorsRuntime {
    connectors_runtime: ConnectorsRuntime,
    #[allow(dead_code)]
    wiremock_container: ContainerAsync<GenericImage>,
}
