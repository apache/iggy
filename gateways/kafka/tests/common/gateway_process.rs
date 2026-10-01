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

//! Real `iggy-gateway-kafka` process harness, mirroring `iggy_server.rs`'s `TestServer` for the
//! gateway binary itself. Every existing "e2e" suite in this crate drives `KafkaGateway::run`
//! in-process (`common/server.rs`), never the compiled binary an operator actually runs - this
//! type closes that gap for #3539's docker-compose-shaped end-to-end test.

#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use crate::iggy_server::{PortGuard, TestServer, graceful_kill};

/// Locates the already-built `iggy-gateway-kafka` binary.
///
/// Unlike `iggy_server.rs`'s `iggy_server_binary()`, this does not need an ancestor walk from
/// `env::current_exe()`: that workaround exists because `iggy-server` lives in a *different*
/// package, where `CARGO_BIN_EXE_*` never resolves. `iggy-gateway-kafka` is this package's own
/// `[[bin]]` (`Cargo.toml`), so `CARGO_BIN_EXE_iggy-gateway-kafka` is set at compile time and
/// Cargo builds the binary itself before running this test - same pattern already used in
/// `gateways/kafka/tools/kafka-tool/tests/generate_cli_tests.rs`.
fn iggy_gateway_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_iggy-gateway-kafka"))
}

pub struct TestGateway {
    child: Child,
    pub address: String,
    _port_guard: PortGuard,
}

impl TestGateway {
    /// Spawns `iggy-gateway-kafka` with the bridge enabled against `server`, then blocks until
    /// its listener is ready or the startup budget is exhausted.
    pub async fn spawn(server: &TestServer) -> Self {
        let port_guard = PortGuard::acquire();
        let address = format!("127.0.0.1:{}", port_guard.port);
        let config = server.test_config();

        let mut command = Command::new(iggy_gateway_binary());
        // env_clear(): without it the child inherits this test process's full environment, and
        // the gateway's own reject_unknown_kafka_env_vars (main.rs) hard-fails startup on any
        // unrecognized inherited IGGY_KAFKA_* var - or, worse, a *recognized* one (say
        // IGGY_KAFKA_ADVERTISED_HOST, which this crate's own README/docker-compose.yml teach a
        // contributor to export) silently changes the spawned gateway's behavior out from under
        // this harness's assertions.
        command
            .env_clear()
            .env("IGGY_KAFKA_BIND_ADDR", &address)
            .env("IGGY_KAFKA_BRIDGE_ENABLED", "true")
            .env("IGGY_KAFKA_IGGY_ADDR", &config.address)
            .env("IGGY_KAFKA_IGGY_USERNAME", &config.username)
            .env(
                "IGGY_KAFKA_IGGY_PASSWORD",
                secrecy::ExposeSecret::expose_secret(&config.password),
            );
        let child = command.spawn().expect("spawn iggy-gateway-kafka");

        let mut gateway = Self {
            child,
            address,
            _port_guard: port_guard,
        };
        gateway.wait_ready().await;
        gateway
    }

    /// Bare TCP connect poll, same rationale as `TestServer::wait_ready`: a full Kafka client
    /// handshake would pay retry overhead on every failed attempt. A connect slightly ahead of
    /// the gateway's own accept loop can't happen here - `main.rs` connects the bridge before
    /// binding the listener, so a successful connect already implies the bridge is up too.
    async fn wait_ready(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child status") {
                panic!(
                    "iggy-gateway-kafka at {} exited during startup with {status}",
                    self.address
                );
            }
            if tokio::net::TcpStream::connect(self.address.as_str())
                .await
                .is_ok()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "iggy-gateway-kafka at {} did not become ready within the startup budget",
                self.address
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        graceful_kill(&mut self.child);
        let _ = self.child.wait();
    }
}
