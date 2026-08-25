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

use std::fmt::{Display, Formatter};
use std::net::TcpListener;

use fluss::client::FlussConnection;
use integration::harness::TestBinaryError;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::connectors::fixtures;

const FLUSS_IMAGE: &str = "apache/fluss";
const ZOOKEEPER_IMAGE: &str = "zookeeper";
const ZOOKEEPER_VERSION: &str = "3.9.2";
const ZOOKEEPER_PORT: u16 = 2181;
const FLUSS_CLIENT_PORT: u16 = 9123;
const CONNECTION_RETRY: u16 = 3;
const CONNECTION_RETRY_DELAY_S: u64 = 5;

struct CoordinatorProperties {
    zookeeper_address: String,
    container_name: String,
    advertised_port: u16,
}

struct TabletServerProperties {
    zookeeper_address: String,
    container_name: String,
    advertised_port: u16,
    tablet_server_id: u32,
}

impl Display for CoordinatorProperties {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "zookeeper.address: {}\n\
             bind.listeners: INTERNAL://{}:0, CLIENT://{}:{}\n\
             advertised.listeners: CLIENT://localhost:{}\n\
             internal.listener.name: INTERNAL\n\
             remote.data.dir: /tmp/fluss/remote-data",
            self.zookeeper_address,
            self.container_name,
            self.container_name,
            FLUSS_CLIENT_PORT,
            self.advertised_port,
        )
    }
}

impl Display for TabletServerProperties {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "zookeeper.address: {}\n\
             bind.listeners: INTERNAL://{}:0, CLIENT://{}:{}\n\
             advertised.listeners: CLIENT://localhost:{}\n\
             internal.listener.name: INTERNAL\n\
             tablet-server.id: {}\n\
             kv.snapshot.interval: 0s\n\
             data.dir: /tmp/fluss/data/tablet-server-{}\n\
             remote.data.dir: /tmp/fluss/remote-data",
            self.zookeeper_address,
            self.container_name,
            self.container_name,
            FLUSS_CLIENT_PORT,
            self.advertised_port,
            self.tablet_server_id,
            self.tablet_server_id,
        )
    }
}

pub struct FlussCluster {
    #[allow(dead_code)]
    zookeeper: ContainerAsync<GenericImage>,
    #[allow(dead_code)]
    coordinator_server: ContainerAsync<GenericImage>,
    #[allow(dead_code)]
    tablet_server: ContainerAsync<GenericImage>,
    pub coordinator_address: String,
    #[allow(dead_code)]
    pub fluss_version: String,
}

impl FlussCluster {
    pub async fn new(fluss_version: &str) -> Result<Self, TestBinaryError> {
        Self::start(fluss_version).await
    }

    pub async fn get_connection(&self) -> Result<FlussConnection, TestBinaryError> {
        let config = fluss::config::Config {
            bootstrap_servers: self.coordinator_address.clone(),
            ..fluss::config::Config::default()
        };

        FlussConnection::new(config).await.map_err(|error| {
            super::fixture_error(format!("Failed to create Fluss connection: {error}"))
        })
    }

    async fn wait_for_fluss_to_become_healthy(&self) -> Result<(), TestBinaryError> {
        let mut attempts = 0;
        loop {
            match self.get_connection().await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    attempts += 1;
                    if attempts >= CONNECTION_RETRY {
                        return Err(super::fixture_error(format!(
                            "Failed to establish Fluss connection after {} attempts: {}",
                            CONNECTION_RETRY, error
                        )));
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(CONNECTION_RETRY_DELAY_S))
                        .await;
                }
            }
        }
    }

    async fn start(fluss_version: &str) -> Result<Self, TestBinaryError> {
        let network = fixtures::unique_container_name("fluss-network");
        let zookeeper_name = fixtures::unique_container_name("fluss-zookeeper");
        let coordinator_name = fixtures::unique_container_name("fluss-coordinator");
        let tablet_name = fixtures::unique_container_name("fluss-tablet-0");
        let coordinator_host_port = available_host_port()?;
        let tablet_host_port = available_host_port_except(coordinator_host_port)?;

        let zookeeper = GenericImage::new(ZOOKEEPER_IMAGE, ZOOKEEPER_VERSION)
            .with_exposed_port(ZOOKEEPER_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Started AdminServer"))
            .with_network(&network)
            .with_container_name(&zookeeper_name)
            .start()
            .await
            .map_err(|error| super::fixture_error(format!("Failed to start ZooKeeper: {error}")))?;

        let zookeeper_address = format!("{zookeeper_name}:{ZOOKEEPER_PORT}");
        let coordinator_properties = CoordinatorProperties {
            zookeeper_address: zookeeper_address.clone(),
            container_name: coordinator_name.clone(),
            advertised_port: coordinator_host_port,
        };
        let coordinator_server = GenericImage::new(FLUSS_IMAGE, fluss_version)
            .with_exposed_port(FLUSS_CLIENT_PORT.tcp())
            .with_wait_for(WaitFor::Nothing)
            .with_network(&network)
            .with_container_name(&coordinator_name)
            .with_env_var("FLUSS_PROPERTIES", coordinator_properties.to_string())
            .with_cmd(["coordinatorServer"])
            .with_mapped_port(coordinator_host_port, FLUSS_CLIENT_PORT.tcp())
            .start()
            .await
            .map_err(|error| {
                super::fixture_error(format!("Failed to start Fluss coordinator server: {error}"))
            })?;

        let tablet_properties = TabletServerProperties {
            zookeeper_address: zookeeper_address.clone(),
            container_name: tablet_name.clone(),
            advertised_port: tablet_host_port,
            tablet_server_id: 0,
        };
        let tablet_server = GenericImage::new(FLUSS_IMAGE, fluss_version)
            .with_exposed_port(FLUSS_CLIENT_PORT.tcp())
            .with_wait_for(WaitFor::Nothing)
            .with_network(&network)
            .with_container_name(&tablet_name)
            .with_env_var("FLUSS_PROPERTIES", tablet_properties.to_string())
            .with_cmd(["tabletServer"])
            .with_mapped_port(tablet_host_port, FLUSS_CLIENT_PORT.tcp())
            .start()
            .await
            .map_err(|error| {
                super::fixture_error(format!("Failed to start Fluss tablet server: {error}"))
            })?;

        let result = Self {
            fluss_version: fluss_version.to_string(),
            zookeeper,
            coordinator_server,
            tablet_server,
            coordinator_address: format!("localhost:{coordinator_host_port}"),
        };

        result.wait_for_fluss_to_become_healthy().await?;

        Ok(result)
    }
}

fn available_host_port() -> Result<u16, TestBinaryError> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| super::fixture_error(format!("Failed to reserve a host port: {error}")))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| {
            super::fixture_error(format!("Failed to read the reserved host port: {error}"))
        })
}

fn available_host_port_except(excluded_port: u16) -> Result<u16, TestBinaryError> {
    loop {
        let host_port = available_host_port()?;
        if host_port != excluded_port {
            return Ok(host_port);
        }
    }
}
