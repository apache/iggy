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

//! Test server spawn helper - compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use iggy_gateway_kafka::auth::SaslAuthenticator;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

/// Bind an ephemeral port, start `KafkaGateway`, return address + shutdown sender.
pub async fn spawn_test_server() -> (SocketAddr, broadcast::Sender<()>) {
    spawn_test_server_with_config(GatewayConfig {
        idle_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        shutdown_drain_timeout: Duration::from_secs(5),
        ..GatewayConfig::default()
    })
    .await
}

/// Start `KafkaGateway` with explicit config, every listener rebound to an ephemeral port.
///
/// Returns the first listener's address: every suite but the TLS one runs a single listener.
/// [`spawn_gateway`] hands back all of them.
///
/// `bind_listener` is synchronous, so this function's own body has no `.await` left - kept
/// `async` anyway (with the lint silenced) rather than churning every one of this suite's ~20
/// call sites from `spawn_test_server_with_config(...).await` to a sync call for a test helper
/// whose whole purpose is spawning an async task.
#[allow(clippy::unused_async)]
pub async fn spawn_test_server_with_config(
    config: GatewayConfig,
) -> (SocketAddr, broadcast::Sender<()>) {
    let (addrs, shutdown) = spawn_gateway(KafkaGateway::new, config);
    (addrs[0], shutdown)
}

/// Same as [`spawn_test_server_with_config`], with a verifier attached for the SASL suites.
#[allow(clippy::unused_async)]
pub async fn spawn_test_server_with_authenticator(
    config: GatewayConfig,
    authenticator: Arc<dyn SaslAuthenticator>,
) -> (SocketAddr, broadcast::Sender<()>) {
    let (addrs, shutdown) = spawn_gateway(
        |config| KafkaGateway::new(config).with_authenticator(authenticator),
        config,
    );
    (addrs[0], shutdown)
}

/// Starts a gateway built by `build` from `config`, with every listener rebound to an ephemeral
/// port. Returns the bound addresses in the order of `config.listeners`.
pub fn spawn_gateway(
    build: impl FnOnce(GatewayConfig) -> KafkaGateway,
    mut config: GatewayConfig,
) -> (Vec<SocketAddr>, broadcast::Sender<()>) {
    // Routed through the same tuned bind path as `main` (deep accept backlog via socket2), not a
    // bare `TcpListener::bind`, so integration tests actually exercise it.
    let mut listeners = Vec::with_capacity(config.listeners.len());
    let mut addrs = Vec::with_capacity(config.listeners.len());
    for listener in &mut config.listeners {
        let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
        let addr = socket.local_addr().expect("local addr");
        listener.bind_addr = addr.to_string();
        addrs.push(addr);
        listeners.push(BoundListener::new(listener.clone(), socket));
    }
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let server = build(config);
    tokio::spawn(async move {
        let _ = server.run(listeners, shutdown_rx).await;
    });
    (addrs, shutdown_tx)
}

/// One listener of `protocol`, on a port [`spawn_gateway`] picks.
pub fn listener(protocol: SecurityProtocol) -> ListenerConfig {
    ListenerConfig::new(protocol, "127.0.0.1:0")
}
