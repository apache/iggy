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

//! Process-level boot smoke test for the `iggy-server` binary.
//!
//! Spawns the production binary against an isolated tempdir with every
//! transport except TCP disabled and `IGGY_TCP_ADDRESS` bound to port 0,
//! reads the OS-assigned port back from `runtime/current_config.toml`, and
//! asserts the listener accepts connections. Scope is the bootstrap path
//! only: the binary starts, resolves its configuration from the
//! environment, and reaches a serving state.

use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::{Client, IggyClientBuilder, UserClient};
use iggy_binary_protocol::{HEADER_SIZE, PrepareHeader};
use std::fs::File;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use toml::Value;

/// `iggy-server` bootstrap can take ~5s on cold caches before the TCP
/// listener binds and `current_config.toml` is written; 30s is generous
/// enough for slow CI runners without hanging the suite indefinitely.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Covers the default `sharding.shutdown_join_timeout` plus margin.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);

/// Cadence for polling `current_config.toml` while waiting for the
/// listener to bind. Small enough to keep observable latency under a
/// second without wasting CPU.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

const ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
const ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

/// Values that never occur in a log by chance, so any hit is a leak.
const SENTINEL_ROOT_USERNAME: &str = "sentinel-root-user-4Kq9";
const SENTINEL_ROOT_PASSWORD: &str = "sentinel-root-password-7Qm2xZ";

const LOGS_DIRECTORY: &str = "logs";
const STDOUT_FILE: &str = "stdout.txt";
const STDERR_FILE: &str = "stderr.txt";

/// Spawn `iggy-server` and verify the production binary bootstraps
/// cleanly.
///
/// Specifically asserts:
///   * The binary path is reachable via `cargo_bin`.
///   * Env overrides for `IGGY_TCP_ADDRESS=127.0.0.1:0` cause the
///     server to bind an OS-assigned port (`port != 0` in
///     `current_config.toml`).
///   * The bound port is `connect()`-able from this test process.
#[tokio::test(flavor = "current_thread")]
async fn server_bootstraps_and_binds_ephemeral_tcp_port() {
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
        .expect("server TCP listener must accept connections post-bootstrap");
}

#[test]
fn given_fresh_data_dir_without_root_credentials_when_server_starts_should_exit_with_error() {
    let workspace = TempDir::new().expect("tempdir for the server workspace");
    let mut child = server_command(workspace.path())
        .spawn()
        .expect("iggy-server must spawn under cargo test");

    let status = match wait_for_exit(&mut child, STARTUP_TIMEOUT) {
        Ok(status) => status,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server without root credentials must refuse to boot: {error}");
        }
    };

    let output = read_process_output(workspace.path());
    assert!(
        !status.success(),
        "server must exit with an error, got {status}"
    );
    assert!(
        output.contains(ROOT_USERNAME_ENV) && output.contains(ROOT_PASSWORD_ENV),
        "boot error must name both root credential variables, got: {output}"
    );
    assert!(
        output.contains(
            "Error: Root credentials are required. Set both IGGY_ROOT_USERNAME and IGGY_ROOT_PASSWORD, or use --with-default-root-credentials for local development only."
        ),
        "boot error must explain how to supply credentials, got: {output}"
    );
    assert!(
        !output.contains("root user password"),
        "server must never mint and print a root password, got: {output}"
    );
}

#[test]
fn given_a_single_replica_cluster_without_root_credentials_when_started_should_exit_with_error() {
    let workspace = TempDir::new().unwrap();
    let (mut command, tcp_addr) = cluster_server_command(workspace.path(), 1);
    let child = command.spawn().unwrap();
    let mut server = TestServer {
        child,
        tcp_addr,
        workspace,
    };
    let status = wait_for_exit(&mut server.child, STARTUP_TIMEOUT).unwrap();
    let output = read_process_output(server.workspace.path());
    assert!(
        !status.success(),
        "fresh solo cluster unexpectedly started: {output}"
    );
    assert!(
        output.contains("Root credentials are required"),
        "unexpected boot failure: {output}"
    );
    assert!(
        !server
            .workspace
            .path()
            .join("data/runtime/current_config.toml")
            .exists()
    );
}

#[test]
fn given_a_cluster_waiting_for_root_when_terminated_should_exit_successfully() {
    let workspace = TempDir::new().unwrap();
    let (mut command, tcp_addr) = cluster_server_command(workspace.path(), 3);
    let child = command.spawn().unwrap();
    let mut server = TestServer {
        child,
        tcp_addr,
        workspace,
    };
    wait_for_bound_tcp(
        &server
            .workspace
            .path()
            .join("data/runtime/current_config.toml"),
        STARTUP_TIMEOUT,
    )
    .unwrap();
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        let output = read_process_output(server.workspace.path());
        if output.contains("Waiting for root initialization without local credentials.") {
            break;
        }
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before waiting for root: {output}"
        );
        assert!(
            Instant::now() < deadline,
            "server never waited for root: {output}"
        );
        sleep(POLL_INTERVAL);
    }
    let terminated = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status()
        .unwrap();
    assert!(terminated.success());
    let status = wait_for_exit(&mut server.child, SHUTDOWN_TIMEOUT).unwrap();
    let output = read_process_output(server.workspace.path());
    assert!(
        status.success(),
        "normal bootstrap shutdown failed: {status}: {output}"
    );
    assert!(!output.contains("Root user initialization complete."));
}

#[test]
fn given_root_credentials_when_server_runs_and_stops_should_never_write_them_to_output_or_logs() {
    let mut server = TestServer::start();
    let status = server.stop();
    assert!(
        status.success(),
        "server must shut down cleanly, got {status}"
    );

    let output = read_process_output(server.workspace.path());
    let logs = read_log_files(&server.data_path().join(LOGS_DIRECTORY));
    assert!(
        logs.contains("server shutdown complete"),
        "the log files must cover the whole run, got: {logs}"
    );
    for (source, text) in [("stdout and stderr", &output), ("log files", &logs)] {
        assert!(
            !text.contains(SENTINEL_ROOT_USERNAME),
            "{source} must not contain the root username"
        );
        assert!(
            !text.contains(SENTINEL_ROOT_PASSWORD),
            "{source} must not contain the root password"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn given_root_credentials_when_restarting_without_them_should_recover_root_from_the_journal()
{
    for default_credentials in [false, true] {
        let mut server = TestServer::start_with(|command| {
            if default_credentials {
                command.arg("--with-default-root-credentials");
            } else {
                command
                    .env(ROOT_USERNAME_ENV, SENTINEL_ROOT_USERNAME)
                    .env(ROOT_PASSWORD_ENV, SENTINEL_ROOT_PASSWORD);
            }
        });
        assert!(server.stop().success());
        assert!(
            !server.data_path().join("metadata/snapshot.bin").exists(),
            "root recovery must not depend on a metadata checkpoint"
        );
        server.restart_without_credentials();
        let (username, password) = if default_credentials {
            ("iggy", "iggy")
        } else {
            (SENTINEL_ROOT_USERNAME, SENTINEL_ROOT_PASSWORD)
        };
        server.assert_root_login(username, password).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn given_repaired_wal_order_when_restarted_without_credentials_should_recover_root() {
    let mut server = TestServer::start();
    server
        .assert_root_login(SENTINEL_ROOT_USERNAME, SENTINEL_ROOT_PASSWORD)
        .await;
    assert!(server.stop().success());
    let path = server.data_path().join("metadata/journal.wal");
    let bytes = std::fs::read(&path).unwrap();
    let mut remaining = bytes.as_slice();
    let mut entries = Vec::new();
    while !remaining.is_empty() {
        let header =
            bytemuck::checked::pod_read_unaligned::<PrepareHeader>(&remaining[..HEADER_SIZE]);
        let (entry, tail) = remaining.split_at(header.size as usize);
        entries.push(entry);
        remaining = tail;
    }
    assert!(entries.len() >= 2);
    entries.swap(0, 1);
    std::fs::write(&path, entries.concat()).unwrap();
    server.restart_without_credentials();
    server
        .assert_root_login(SENTINEL_ROOT_USERNAME, SENTINEL_ROOT_PASSWORD)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn given_initialized_root_when_killed_and_restarted_without_credentials_should_recover_root()
{
    let mut server = TestServer::start();
    server
        .child
        .kill()
        .expect("kill the server without flushing");
    server.child.wait().expect("reap the killed server");
    server.restart_without_credentials();
    server
        .assert_root_login(SENTINEL_ROOT_USERNAME, SENTINEL_ROOT_PASSWORD)
        .await;
}

/// Spawned binary handle. Drops on test exit kill the child + wait so
/// the test does not leave zombie processes if it panics mid-assertion.
struct TestServer {
    child: Child,
    tcp_addr: SocketAddr,
    workspace: TempDir,
}

impl TestServer {
    fn start() -> Self {
        Self::start_with(|command| {
            command
                .env(ROOT_USERNAME_ENV, SENTINEL_ROOT_USERNAME)
                .env(ROOT_PASSWORD_ENV, SENTINEL_ROOT_PASSWORD);
        })
    }

    fn start_with(configure: impl FnOnce(&mut Command)) -> Self {
        let workspace = TempDir::new().expect("tempdir for the server workspace");
        let mut command = server_command(workspace.path());
        configure(&mut command);
        let mut child = command
            .spawn()
            .expect("iggy-server must spawn under cargo test");

        let runtime_path = workspace
            .path()
            .join("data")
            .join("runtime")
            .join("current_config.toml");
        let tcp_addr =
            match wait_for_bound_tcp(&runtime_path, STARTUP_TIMEOUT).and_then(|address| {
                wait_for_root_initialization(workspace.path())?;
                Ok(address)
            }) {
                Ok(addr) => addr,
                Err(err) => {
                    // Reap the spawn-failed child so the test does not
                    // leave a zombie behind; the tempdir drops naturally
                    // when `workspace` goes out of scope at the panic.
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("server startup failed: {err}");
                }
            };

        Self {
            child,
            tcp_addr,
            workspace,
        }
    }

    fn data_path(&self) -> PathBuf {
        self.workspace.path().join("data")
    }

    fn restart_without_credentials(&mut self) {
        let runtime_path = self.data_path().join("runtime/current_config.toml");
        std::fs::remove_file(&runtime_path).expect("remove the previous runtime address");
        self.child = server_command(self.workspace.path())
            .spawn()
            .expect("restart the server without root credentials");
        self.tcp_addr = wait_for_bound_tcp(&runtime_path, STARTUP_TIMEOUT)
            .unwrap_or_else(|error| panic!("restart must recover root: {error}"));
        wait_for_root_initialization(self.workspace.path())
            .unwrap_or_else(|error| panic!("restart must initialize root: {error}"));
    }

    async fn assert_root_login(&self, username: &str, password: &str) {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(self.tcp_addr.to_string())
            .build()
            .expect("TCP client");
        client.connect().await.expect("connect to the server");
        let user = client
            .login_user(username, password)
            .await
            .expect("the original root credentials still authenticate");
        assert_eq!(user.user_id, 0);
        client.disconnect().await.expect("disconnect the client");
    }

    /// Graceful stop through the server's own SIGINT handler, so every log
    /// appender flushes before the files are read.
    fn stop(&mut self) -> ExitStatus {
        let interrupted = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status()
            .expect("kill must run");
        assert!(interrupted.success(), "SIGINT must reach the server");
        wait_for_exit(&mut self.child, SHUTDOWN_TIMEOUT)
            .unwrap_or_else(|error| panic!("server must stop after SIGINT: {error}"))
    }
}

impl std::fmt::Debug for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestServer")
            .field("tcp_addr", &self.tcp_addr)
            .field("workspace", &self.workspace.path())
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

/// The binary with an isolated data dir under `workspace/data`, TCP alone on
/// an ephemeral port, and stdout / stderr captured to files in `workspace`.
/// Root credentials and `.env` files from the caller's environment are kept
/// out, so each test states the credentials it boots with.
fn server_command(workspace: &Path) -> Command {
    let mut command = Command::cargo_bin("iggy-server")
        .expect("iggy-server binary must be built by the test runner");
    let stdout = File::create(workspace.join(STDOUT_FILE)).expect("stdout capture file");
    let stderr = File::create(workspace.join(STDERR_FILE)).expect("stderr capture file");
    command
        .env("RUST_LOG", "info")
        .env_remove(ROOT_USERNAME_ENV)
        .env_remove(ROOT_PASSWORD_ENV)
        .env("IGGY_ENV_PATH", workspace.join("absent.env"))
        .env("IGGY_PATH", workspace.join("data"))
        .env("IGGY_LOGGING_PATH", LOGS_DIRECTORY)
        .env("IGGY_LOGGING_FILE_ENABLED", "true")
        // Ephemeral port; the actual bound port is read back from
        // `runtime/current_config.toml` after the listener binds.
        .env("IGGY_TCP_ADDRESS", "127.0.0.1:0")
        // Disable every other transport so the test does not race
        // unrelated listeners (HTTP UI bind, QUIC self-signed cert
        // material, WebSocket handshake, etc.).
        .env("IGGY_HTTP_ENABLED", "false")
        .env("IGGY_QUIC_ENABLED", "false")
        .env("IGGY_WEBSOCKET_ENABLED", "false")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    command
}

fn cluster_server_command(workspace: &Path, replica_count: usize) -> (Command, SocketAddr) {
    let reservations: Vec<_> = (0..replica_count * 2)
        .map(|_| StdTcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let tcp_addr = reservations[0].local_addr().unwrap();
    let mut command = server_command(workspace);
    command
        .args(["--replica-id", "0"])
        .env("IGGY_CLUSTER_ENABLED", "true")
        .env("IGGY_MESSAGE_BUS_RECONNECT_PERIOD", "100ms");
    for node in 0..replica_count {
        command
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_NAME"),
                format!("node-{node}"),
            )
            .env(format!("IGGY_CLUSTER_NODES_{node}_IP"), "127.0.0.1")
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_REPLICA_ID"),
                node.to_string(),
            )
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_PORTS_TCP"),
                reservations[node * 2]
                    .local_addr()
                    .unwrap()
                    .port()
                    .to_string(),
            )
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_PORTS_TCP_REPLICA"),
                reservations[node * 2 + 1]
                    .local_addr()
                    .unwrap()
                    .port()
                    .to_string(),
            );
    }
    (command, tcp_addr)
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> Result<ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => sleep(POLL_INTERVAL),
            Err(error) => return Err(format!("try_wait failed: {error}")),
        }
    }
    Err(format!("still running after {timeout:?}"))
}

fn wait_for_root_initialization(workspace: &Path) -> Result<(), String> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        if read_process_output(workspace).contains("Root user initialization complete.") {
            return Ok(());
        }
        sleep(POLL_INTERVAL);
    }
    Err("root initialization did not complete".to_owned())
}

fn read_process_output(workspace: &Path) -> String {
    [STDOUT_FILE, STDERR_FILE]
        .iter()
        .map(|file| std::fs::read_to_string(workspace.join(file)).expect("captured process output"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn read_log_files(logs_path: &Path) -> String {
    std::fs::read_dir(logs_path)
        .expect("the server must create its log directory")
        .map(|entry| entry.expect("log directory entry").path())
        .filter(|path| path.is_file())
        .map(|path| std::fs::read_to_string(&path).expect("log file contents"))
        .collect::<Vec<_>>()
        .join("\n")
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
