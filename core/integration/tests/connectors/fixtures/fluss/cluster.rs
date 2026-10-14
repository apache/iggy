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
use std::time::Duration;

use fluss::{
    ServerType,
    client::FlussConnection,
    error::Error as FlussError,
    metadata::{DataTypes, Schema, TableDescriptor, TablePath},
    rpc::message::OffsetSpec,
};
use integration::harness::TestBinaryError;
use testcontainers_modules::testcontainers::core::{
    CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor,
};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

use tokio::time::{Instant, sleep, timeout_at};

use crate::connectors::fixtures;

const FLUSS_IMAGE: &str = "apache/fluss";
const ZOOKEEPER_IMAGE: &str = "zookeeper";
const ZOOKEEPER_VERSION: &str = "3.9.2";
const ZOOKEEPER_PORT: u16 = 2181;
const FLUSS_CLIENT_PORT: u16 = 9123;
const READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const READINESS_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(250);
const READINESS_DATABASE: &str = "iggy_fixture_readiness";
const TABLET_SERVER_ID: i32 = 0;

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
             auto-partition.check.interval: 5s\n\
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
        let deadline = Instant::now() + READINESS_TIMEOUT;
        loop {
            let attempt_deadline = deadline.min(Instant::now() + READINESS_ATTEMPT_TIMEOUT);
            let result = timeout_at(attempt_deadline, async {
                let connection = self.get_connection().await?;
                probe_tablet_server(&connection).await.map_err(|error| {
                    super::fixture_error(format!("Tablet readiness probe failed: {error}"))
                })
            })
            .await;
            let error = match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => error.to_string(),
                Err(_) => "readiness probe attempt timed out".to_string(),
            };
            if Instant::now() >= deadline {
                return Err(super::fixture_error(format!(
                    "Fluss tablet server was not ready within {}s: {error}",
                    READINESS_TIMEOUT.as_secs()
                )));
            }
            sleep(READINESS_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())))
                .await;
        }
    }

    async fn start(fluss_version: &str) -> Result<Self, TestBinaryError> {
        let network = fixtures::unique_container_name("fluss-network");
        let zookeeper_name = fixtures::unique_container_name("fluss-zookeeper");
        let coordinator_name = fixtures::unique_container_name("fluss-coordinator");
        let tablet_name = fixtures::unique_container_name("fluss-tablet-0");

        let zookeeper = GenericImage::new(ZOOKEEPER_IMAGE, ZOOKEEPER_VERSION)
            .with_exposed_port(ZOOKEEPER_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Started AdminServer"))
            .with_network(&network)
            .with_container_name(&zookeeper_name)
            .start()
            .await
            .map_err(|error| super::fixture_error(format!("Failed to start ZooKeeper: {error}")))?;

        let zookeeper_address = format!("{zookeeper_name}:{ZOOKEEPER_PORT}");
        let coordinator_server = start_fluss_server(
            fluss_version,
            &network,
            &coordinator_name,
            "coordinatorServer",
        )
        .await?;
        let coordinator_host_port = coordinator_server
            .get_host_port_ipv4(FLUSS_CLIENT_PORT.tcp())
            .await
            .map_err(|error| {
                super::fixture_error(format!("Failed to read coordinator port: {error}"))
            })?;
        let coordinator_properties = CoordinatorProperties {
            zookeeper_address: zookeeper_address.clone(),
            container_name: coordinator_name,
            advertised_port: coordinator_host_port,
        };
        configure_fluss_server(&coordinator_server, &coordinator_properties).await?;

        let tablet_server =
            start_fluss_server(fluss_version, &network, &tablet_name, "tabletServer").await?;
        let tablet_host_port = tablet_server
            .get_host_port_ipv4(FLUSS_CLIENT_PORT.tcp())
            .await
            .map_err(|error| {
                super::fixture_error(format!("Failed to read tablet port: {error}"))
            })?;
        let tablet_properties = TabletServerProperties {
            zookeeper_address,
            container_name: tablet_name,
            advertised_port: tablet_host_port,
            tablet_server_id: TABLET_SERVER_ID as u32,
        };
        configure_fluss_server(&tablet_server, &tablet_properties).await?;

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

async fn probe_tablet_server(connection: &FlussConnection) -> Result<(), FlussError> {
    let admin = connection.get_admin()?;
    let servers = admin.get_server_nodes().await?;
    if !servers.iter().any(|server| {
        server.id() == TABLET_SERVER_ID && server.server_type() == &ServerType::TabletServer
    }) {
        return Err(FlussError::UnexpectedError {
            message: format!("Tablet server {TABLET_SERVER_ID} has not registered"),
            source: None,
        });
    }

    // Registration can precede bucket service readiness; exercise an actual bucket RPC.
    let table_path = TablePath::new(READINESS_DATABASE, "tablet_probe");
    let schema = Schema::builder()
        .column("id", DataTypes::bigint())
        .build()?;
    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec!["id".to_string()])
        .build()?;
    admin
        .create_database(READINESS_DATABASE, None, true)
        .await?;
    admin.create_table(&table_path, &descriptor, true).await?;
    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await?;
    if !offsets.contains_key(&0) {
        return Err(FlussError::UnexpectedError {
            message: "Readiness probe returned no offset for bucket 0".to_string(),
            source: None,
        });
    }
    admin.drop_database(READINESS_DATABASE, true, true).await?;
    Ok(())
}

async fn start_fluss_server(
    fluss_version: &str,
    network: &str,
    container_name: &str,
    server_role: &str,
) -> Result<ContainerAsync<GenericImage>, TestBinaryError> {
    // Docker must allocate the host port before Fluss can advertise it.
    GenericImage::new(FLUSS_IMAGE, fluss_version)
        .with_entrypoint("/bin/sh")
        .with_exposed_port(FLUSS_CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_network(network)
        .with_container_name(container_name)
        .with_cmd([
            "-c",
            "while [ ! -f /tmp/iggy-fluss.properties ]; do sleep 0.1; done; \
             export FLUSS_PROPERTIES=\"$(cat /tmp/iggy-fluss.properties)\"; \
             exec /docker-entrypoint.sh \"$1\"",
            "iggy-fluss",
            server_role,
        ])
        .start()
        .await
        .map_err(|error| {
            super::fixture_error(format!("Failed to start Fluss {server_role}: {error}"))
        })
}

async fn configure_fluss_server(
    container: &ContainerAsync<GenericImage>,
    properties: &impl Display,
) -> Result<(), TestBinaryError> {
    container
        .exec(
            ExecCommand::new([
                "sh".to_string(),
                "-c".to_string(),
                "printf '%s\\n' \"$1\" > /tmp/iggy-fluss.properties.tmp && mv /tmp/iggy-fluss.properties.tmp /tmp/iggy-fluss.properties".to_string(),
                "iggy-fluss".to_string(),
                properties.to_string(),
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .map_err(|error| super::fixture_error(format!("Failed to configure Fluss server: {error}")))?;
    Ok(())
}
