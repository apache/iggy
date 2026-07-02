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

//! Test server spawn helper — compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::broadcast;

use iggy_gateway_kafka::{KafkaServer, ServerConfig};

/// Bind an ephemeral port, start `KafkaServer`, return address + shutdown sender.
pub async fn spawn_test_server() -> (SocketAddr, broadcast::Sender<()>) {
    spawn_test_server_with_config(ServerConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: 8 * 1024 * 1024,
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    })
    .await
}

/// Start `KafkaServer` with explicit config (`bind_addr` overwritten with ephemeral port).
pub async fn spawn_test_server_with_config(
    mut config: ServerConfig,
) -> (SocketAddr, broadcast::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    config.bind_addr = addr.to_string();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let server = KafkaServer::new(config);
    tokio::spawn(async move {
        let _ = server.run(listener, shutdown_rx).await;
    });

    (addr, shutdown_tx)
}
