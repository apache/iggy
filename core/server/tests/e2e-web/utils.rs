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

use reqwest::Url;
use std::time::Duration;
use testcontainers::core::{WaitFor, ports::IntoContainerPort as _};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};

const IGGY_HTTP_PORT: u16 = 3000;
const IGGY_HTTP_ADDRESS: &str = "0.0.0.0:3000";
const IGGY_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct IggyContainer {
    #[allow(unused)]
    handle: ContainerAsync<GenericImage>,
    #[allow(unused)]
    pub port: u16,
    pub address: Url,
}

/// Launch Iggy server instance in a container.
///
/// This will spin up a container and wait until it is ready and return [`IggyContainer`],
/// which keeps the container's handle (whose `Drop` implemention knows how to
/// clean up) as well as the host port and HTTP url of the Iggy server.
///
/// Note that this relies on the "iggy:local" image to be available, i.e. we need
/// to build and tag the image prior to running tests which are using this utility.
/// E.g. with `docker` the image can be built with (from the workspace root):
///
/// ```console
/// docker build -t iggy:local -f core/server/Dockerfile .
/// ```
///
/// For debugging/inspecting of what is being done here programmatically, you can
/// launch a container with the equivalent (again, using `docker`):
/// ```console
/// docker run \
///     --security-opt seccomp=unconfined \
///     -e IGGY_SYSTEM_SHARDING_CPU_ALLOCATION=2 \
///     -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
///     -e IGGY_HTTP_WEB_UI=true \
///     -e IGGY_TCP_ENABLED=false \
///     -e IGGY_WEBSOCKET_ENABLED=false \
///     -e IGGY_QUIC_ENABLED=false \
///     -p 0:3000 iggy:local
/// ```
pub(crate) async fn launch_iggy_container() -> IggyContainer {
    let container = GenericImage::new("iggy", "local")
        .with_exposed_port(IGGY_HTTP_PORT.tcp())
        // this needle we are looking for in stdout comes from `http_server.rs`,
        // once it's found we know that the server is ready to accept connections
        .with_wait_for(WaitFor::message_on_stdout(format!(
            "Started HTTP API on: {IGGY_HTTP_ADDRESS}"
        )))
        // Apache Iggy is powered by `io_uring` (find raionale and insights here:
        // https://www.youtube.com/watch?v=oddHJslao64&t=2649s), but io_uring
        // runtime requires specific syscalls (io_uring_setup, io_uring_enter,
        // io_uring_register) which are by default blocked in containerized
        // environments (such as Docker or Podman); for testing purposes solely,
        // we are removing all the restrictions from the environment
        .with_security_opt("seccomp=unconfined")
        // from our local testing experience, container quickly runs out
        // of memory during the bootstrapping process and so we can the number
        // of shards we are spawning
        .with_env_var("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "2")
        // as of Iggy Server v0.6.0, 3000 is the default port anyways, but we need
        // Iggy to be listening across all interfaces (by default it's only loopback)
        // since it is running in a container; note though that we are going to talk
        // to Iggy via an ephemeral port on the host that OS is going to assign
        .with_env_var("IGGY_HTTP_ADDRESS", IGGY_HTTP_ADDRESS)
        .with_env_var("IGGY_HTTP_WEB_UI", "true")
        // since Iggy Web UI utilizes HTTP, we are switching off other transports
        .with_env_var("IGGY_TCP_ENABLED", "false")
        .with_env_var("IGGY_WEBSOCKET_ENABLED", "false")
        .with_env_var("IGGY_QUIC_ENABLED", "false")
        .with_startup_timeout(IGGY_STARTUP_TIMEOUT)
        .start()
        .await
        .expect("container with Iggy server to be up and running");

    let host_port = container
        .ports()
        .await
        .expect("post to have been published")
        .map_to_host_port_ipv4(IGGY_HTTP_PORT)
        .expect("host port to have been assigned by OS");
    let iggy_url = format!("http://127.0.0.1:{}", host_port).parse().unwrap();

    IggyContainer {
        handle: container,
        port: host_port,
        address: iggy_url,
    }
}
