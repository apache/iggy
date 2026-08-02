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

use crate::connectors::fixtures;
use integration::harness::TestBinaryError;
use std::net::TcpListener;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const FLUSS_IMAGE: &str = "apache/fluss";
const FLUSS_VERSION: &str = "0.9.1-incubating";
const READY_MESSAGE: &str = "Registered tablet server 0";

pub(super) const ENV_SOURCE_BOOTSTRAP_SERVERS: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_BOOTSTRAP_SERVERS";
pub(super) const ENV_SOURCE_DATABASE: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_DATABASE";
pub(super) const ENV_SOURCE_TABLE: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_TABLE";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PATH";

/// A whole Fluss cluster (embedded ZooKeeper, coordinator server, tablet server) inside one
/// container.
///
/// The image ships `local-cluster.sh`, which starts the same three processes, but it rewrites
/// the tablet server's bind port to 0 so it cannot collide with the coordinator. A random port
/// inside the container cannot be published to the host, so the tablet server is given an
/// explicit second port here instead.
///
/// Both servers advertise `localhost`, which resolves to the coordinator/tablet pair inside the
/// container and to the published ports from the test process. That only holds while the host
/// and container port numbers match, so free host ports are reserved up front and mapped
/// one-to-one rather than letting Docker assign them.
pub(super) struct FlussContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) bootstrap_servers: String,
}

impl FlussContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let coordinator_port = reserve_host_port()?;
        let tablet_port = reserve_host_port()?;

        let container = GenericImage::new(FLUSS_IMAGE, FLUSS_VERSION)
            .with_wait_for(WaitFor::message_on_stdout(READY_MESSAGE))
            .with_entrypoint("/bin/bash")
            .with_container_name(fixtures::unique_container_name("fluss"))
            .with_mapped_port(coordinator_port, coordinator_port.tcp())
            .with_mapped_port(tablet_port, tablet_port.tcp())
            .with_env_var("FLUSS_PROPERTIES", server_properties(coordinator_port))
            .with_cmd(["-c", &startup_script(tablet_port)])
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        Ok(Self {
            container,
            bootstrap_servers: format!("localhost:{coordinator_port}"),
        })
    }
}

/// The coordinator settings land in `server.yaml`. The tablet server inherits them and
/// overrides only its listeners on the command line.
fn server_properties(coordinator_port: u16) -> String {
    format!(
        "zookeeper.address: localhost:2181\n\
         bind.listeners: CLIENT://0.0.0.0:{coordinator_port}\n\
         advertised.listeners: CLIENT://localhost:{coordinator_port}\n\
         internal.listener.name: CLIENT\n\
         default.bucket.number: 1\n\
         default.replication.factor: 1\n\
         data.dir: /tmp/fluss/data\n\
         remote.data.dir: /tmp/fluss/remote-data\n\
         tablet-server.id: 0\n"
    )
}

/// `/docker-entrypoint.sh true` only runs the image's configuration step, which appends
/// `FLUSS_PROPERTIES` to `server.yaml`. The tablet server runs in the foreground so the
/// container stays alive with it.
fn startup_script(tablet_port: u16) -> String {
    format!(
        "set -e\n\
         /docker-entrypoint.sh true\n\
         /opt/fluss/bin/fluss-daemon.sh start zookeeper /opt/fluss/conf/zookeeper.properties\n\
         /opt/fluss/bin/coordinator-server.sh start\n\
         exec /opt/fluss/bin/tablet-server.sh start-foreground \
         -Dbind.listeners=CLIENT://0.0.0.0:{tablet_port} \
         -Dadvertised.listeners=CLIENT://localhost:{tablet_port}\n"
    )
}

/// Binds port 0, reads back what the kernel picked, then releases it. The port is only
/// reserved by convention until the container claims it, which is the same trade every
/// fixture that needs a known port ahead of time makes.
fn reserve_host_port() -> Result<u16, TestBinaryError> {
    let listener =
        TcpListener::bind("127.0.0.1:0").map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "FlussContainer".to_string(),
            message: format!("Failed to reserve a host port: {error}"),
        })?;
    let port = listener
        .local_addr()
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "FlussContainer".to_string(),
            message: format!("Failed to read the reserved host port: {error}"),
        })?
        .port();
    Ok(port)
}
