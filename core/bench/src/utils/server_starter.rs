/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::args::common::IggyBenchArgs;
use iggy::prelude::TransportProtocol;
use integration::test_server::{IpAddrKind, SYSTEM_PATH_ENV_VAR, TestServer};
use serde::Deserialize;
use std::net::SocketAddr;
use std::{collections::HashMap, time::Instant};
use tokio::net::{TcpStream, UdpSocket};
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct ServerConfig {
    http: ConfigAddress,
    tcp: ConfigAddress,
    quic: ConfigAddress,
    websocket: ConfigAddress,
}

#[derive(Debug, Deserialize)]
struct ConfigAddress {
    address: String,
}

#[allow(clippy::cognitive_complexity)]
pub async fn start_server_if_needed(args: &IggyBenchArgs) -> Option<TestServer> {
    if args.skip_server_start {
        info!("Skipping iggy-server start");
        return None;
    }

    let (should_start, mut envs) = evaluate_server_start_condition(args).await;

    if should_start {
        envs.insert(SYSTEM_PATH_ENV_VAR.to_owned(), "local_data".to_owned());

        if args.verbose {
            envs.insert("IGGY_TEST_VERBOSE".to_owned(), "true".to_owned());
            info!("Enabling verbose output - iggy-server will logs print to stdout");
        } else {
            info!("Disabling verbose output - iggy-server will print logs to files");
        }

        info!(
            "Starting test server, transport: {}, cleanup: {}, verbosity: {}",
            args.transport(),
            args.cleanup,
            args.verbose
        );
        let mut test_server = TestServer::new(
            Some(envs),
            args.cleanup,
            args.server_executable_path.clone(),
            IpAddrKind::V4,
        );
        let now = Instant::now();
        test_server.start();
        let elapsed = now.elapsed();
        if elapsed.as_millis() > 1000 {
            warn!(
                "Test iggy-server started, pid: {}, startup took {} ms because it had to load messages from disk to cache",
                test_server.pid(),
                elapsed.as_millis()
            );
        } else {
            info!(
                "Test iggy-server started, pid: {}, startup time: {} ms",
                test_server.pid(),
                elapsed.as_millis()
            );
        }

        Some(test_server)
    } else {
        info!("Skipping iggy-server start");
        None
    }
}

async fn evaluate_server_start_condition(args: &IggyBenchArgs) -> (bool, HashMap<String, String>) {
    let default_config: ServerConfig =
        toml::from_str(include_str!("../../../configs/server.toml")).unwrap();

    match &args.transport() {
        TransportProtocol::Http => {
            let args_http_address = args.server_address().parse::<SocketAddr>().unwrap();
            let config_http_address = default_config.http.address.parse::<SocketAddr>().unwrap();
            let envs = HashMap::from([
                (
                    "IGGY_HTTP_ADDRESS".to_owned(),
                    default_config.http.address.clone(),
                ),
                ("IGGY_TCP_ENABLED".to_owned(), "false".to_owned()),
                ("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned()),
            ]);
            (
                addresses_are_equivalent(&args_http_address, &config_http_address)
                    && !is_tcp_addr_in_use(&args_http_address).await,
                envs,
            )
        }
        TransportProtocol::Tcp => {
            let args_tcp_address = args.server_address().parse::<SocketAddr>().unwrap();
            let config_tcp_address = default_config.tcp.address.parse::<SocketAddr>().unwrap();

            let envs = HashMap::from([
                (
                    "IGGY_TCP_ADDRESS".to_owned(),
                    default_config.tcp.address.clone(),
                ),
                ("IGGY_HTTP_ENABLED".to_owned(), "false".to_owned()),
                ("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned()),
            ]);
            (
                addresses_are_equivalent(&args_tcp_address, &config_tcp_address)
                    && !is_tcp_addr_in_use(&args_tcp_address).await,
                envs,
            )
        }
        TransportProtocol::Quic => {
            let args_quic_address = args.server_address().parse::<SocketAddr>().unwrap();
            let config_quic_address = default_config.quic.address.parse::<SocketAddr>().unwrap();
            let envs = HashMap::from([
                (
                    "IGGY_QUIC_ADDRESS".to_owned(),
                    default_config.quic.address.clone(),
                ),
                ("IGGY_HTTP_ENABLED".to_owned(), "false".to_owned()),
                ("IGGY_TCP_ENABLED".to_owned(), "false".to_owned()),
            ]);

            (
                addresses_are_equivalent(&args_quic_address, &config_quic_address)
                    && !is_udp_addr_in_use(&args_quic_address).await,
                envs,
            )
        }
        TransportProtocol::WebSocket => {
            let args_websocket_address = args.server_address().parse::<SocketAddr>().unwrap();
            let config_websocket_address = default_config
                .websocket
                .address
                .parse::<SocketAddr>()
                .unwrap();
            let envs = HashMap::from([(
                "IGGY_WEBSOCKET_ADDRESS".to_owned(),
                default_config.websocket.address.clone(),
            )]);
            (
                addresses_are_equivalent(&args_websocket_address, &config_websocket_address)
                    && !is_tcp_addr_in_use(&args_websocket_address).await,
                envs,
            )
        }
    }
}

async fn is_tcp_addr_in_use(addr: &SocketAddr) -> bool {
    TcpStream::connect(addr).await.is_ok()
}

async fn is_udp_addr_in_use(addr: &SocketAddr) -> bool {
    UdpSocket::bind(addr).await.is_err()
}

fn addresses_are_equivalent(first: &SocketAddr, second: &SocketAddr) -> bool {
    if first.ip().is_unspecified() || second.ip().is_unspecified() {
        first.port() == second.port()
    } else {
        first == second
    }
}
