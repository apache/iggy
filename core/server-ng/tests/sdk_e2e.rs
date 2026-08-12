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

//! Process-level boot smoke test for the `iggy-server-ng` binary.
//!
//! Spawns the production binary against an isolated tempdir with every
//! transport except TCP disabled and `IGGY_TCP_ADDRESS` bound to port 0,
//! reads the OS-assigned port back from `runtime/current_config.toml`, and
//! asserts the listener accepts connections. Scope is the bootstrap path
//! only: the binary starts, resolves its configuration from the
//! environment, and reaches a serving state.

use assert_cmd::prelude::CommandCargoExt;
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use toml::Value;

/// `iggy-server-ng` bootstrap can take ~5s on cold caches before the TCP
/// listener binds and `current_config.toml` is written; 30s is generous
/// enough for slow CI runners without hanging the suite indefinitely.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Cadence for polling `current_config.toml` while waiting for the
/// listener to bind. Small enough to keep observable latency under a
/// second without wasting CPU.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Spawned binary handle. Drops on test exit kill the child + wait so
/// the test does not leave zombie processes if it panics mid-assertion.
struct TestServer {
    child: Child,
    tcp_addr: SocketAddr,
    data_dir: TempDir,
}

impl TestServer {
    fn start() -> Self {
        let data_dir = TempDir::new().expect("tempdir for system.path");
        let mut cmd = Command::cargo_bin("iggy-server-ng")
            .expect("iggy-server-ng binary must be built by the test runner");
        cmd.env("IGGY_SYSTEM_PATH", data_dir.path())
            // Ephemeral port; the actual bound port is read back from
            // `runtime/current_config.toml` after the listener binds.
            .env("IGGY_TCP_ADDRESS", "127.0.0.1:0")
            // Disable every other transport so the test does not race
            // unrelated listeners (HTTP UI bind, QUIC self-signed cert
            // material, WebSocket handshake, etc.).
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .expect("iggy-server-ng must spawn under cargo test");

        let runtime_path = data_dir.path().join("runtime").join("current_config.toml");
        let tcp_addr = match wait_for_bound_tcp(&runtime_path, STARTUP_TIMEOUT) {
            Ok(addr) => addr,
            Err(err) => {
                // Reap the spawn-failed child so the test does not
                // leave a zombie behind; the tempdir drops naturally
                // when `data_dir` goes out of scope at the panic.
                let _ = child.kill();
                let _ = child.wait();
                panic!("server-ng startup failed: {err}");
            }
        };

        Self {
            child,
            tcp_addr,
            data_dir,
        }
    }
}

impl std::fmt::Debug for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestServer")
            .field("tcp_addr", &self.tcp_addr)
            .field("data_dir", &self.data_dir.path())
            .field("child_pid", &self.child.id())
            .finish()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Best-effort: SIGKILL the child, drain its exit status so the
        // OS reclaims the PID, then drop the tempdir.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Poll `runtime/current_config.toml` until it exists and reports a
/// non-zero bound TCP port. Returns the resolved [`SocketAddr`].
fn wait_for_bound_tcp(config_path: &Path, timeout: Duration) -> Result<SocketAddr, String> {
    let deadline = Instant::now() + timeout;
    let mut last_err = String::from("config file never appeared");
    while Instant::now() < deadline {
        match read_bound_tcp(config_path) {
            Ok(addr) => {
                // Confirm the port is reachable before handing it back.
                // The TOML write races slightly ahead of `bind()`
                // becoming `connect()`-able on some kernels.
                if StdTcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
                    return Ok(addr);
                }
                last_err = format!("bound TCP {addr} not connectable yet");
            }
            Err(err) => last_err = err,
        }
        sleep(POLL_INTERVAL);
    }
    Err(format!(
        "timed out after {timeout:?} waiting for {}: {last_err}",
        config_path.display()
    ))
}

fn read_bound_tcp(config_path: &Path) -> Result<SocketAddr, String> {
    let raw = std::fs::read_to_string(config_path)
        .map_err(|e| format!("read {}: {e}", config_path.display()))?;
    let parsed: Value =
        toml::from_str(&raw).map_err(|e| format!("parse {}: {e}", config_path.display()))?;
    let address = parsed
        .get("tcp")
        .and_then(|t| t.get("address"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{} missing tcp.address", config_path.display()))?;
    let addr: SocketAddr = address
        .parse()
        .map_err(|e| format!("invalid tcp.address {address:?}: {e}"))?;
    if addr.port() == 0 {
        return Err(format!("tcp.address {addr} still on port 0"));
    }
    Ok(addr)
}

/// Spawn `iggy-server-ng` and verify the production binary bootstraps
/// cleanly.
///
/// Specifically asserts:
///   * The binary path is reachable via `cargo_bin`.
///   * Env overrides for `IGGY_TCP_ADDRESS=127.0.0.1:0` cause the
///     server to bind an OS-assigned port (`port != 0` in
///     `current_config.toml`).
///   * The bound port is `connect()`-able from this test process.
#[tokio::test(flavor = "current_thread")]
async fn server_ng_bootstraps_and_binds_ephemeral_tcp_port() {
    let server = TestServer::start();
    assert_ne!(
        server.tcp_addr.port(),
        0,
        "TCP listener must bind an OS-assigned port; got {}",
        server.tcp_addr
    );
    // A successful `wait_for_bound_tcp` already proved the port is
    // `connect()`-able once; doing it again here is a sanity check that
    // the listener stays bound for the lifetime of the test harness.
    let _ = StdTcpStream::connect_timeout(&server.tcp_addr, Duration::from_secs(1))
        .expect("server-ng TCP listener must accept connections post-bootstrap");
}
