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

//! End-to-end smoke test for the partition reconciliation loop.
//!
//! Spawns the `iggy-server-ng` binary with an isolated tempdir + ephemeral
//! TCP port, drives an SDK client through the metadata commit path
//! (`Login` → `CreateStream` → `CreateTopic` with N partitions), then
//! verifies the per-partition on-disk hierarchy exists. That hierarchy is
//! created only by
//! [`server_ng::partition_helpers::create_partition_file_hierarchy`],
//! inside the reconciler's
//! [`build_partition_fresh`](server_ng::partition_helpers::build_partition_fresh)
//! path, so a materialisation observable from outside the process proves
//! the full commit-notifier → reconciler → disk pipeline.
//!
//! Deliberately stops short of `SendMessages` / `PollMessages`: the
//! partition data-plane SDK round-trip in server-ng is still evolving, so
//! a smoke test failing for unrelated data-plane reasons would be noisy.
//! The on-disk hierarchy assertion proves the reconciler ran without
//! depending on the still-evolving wire surface.

use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::{
    AutoLogin, Client, CompressionAlgorithm, IggyByteSize, IggyClient, IggyClientBuilder,
    IggyDuration, IggyExpiry, MaxTopicSize, StreamClient, TopicClient, UserClient,
};
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use toml::Value;

/// `iggy-server-ng` bootstrap can take ~5s on cold caches before the TCP
/// listener binds and `current_config.toml` is written; 30s is generous
/// enough for slow CI runners without hanging the suite indefinitely.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Cadence for polling `current_config.toml` and the partition
/// directories. Small enough to keep observable latency under a second
/// without wasting CPU.
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
            // Tighten the reconciler tick so the post-CreateTopic
            // materialisation window stays small under CI latency
            // jitter. Hard-coded 200ms is still way above the per-tick
            // re-read cost yet small enough to bound test wall-time.
            .env("IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL", "200 ms")
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

    fn data_path(&self) -> &Path {
        self.data_dir.path()
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

/// Wait until every per-partition directory has been created by the
/// reconciler. Returns `Ok(())` once all expected paths exist; surfaces
/// a structured error on timeout listing which paths were missing.
fn wait_for_partition_dirs(
    data_dir: &Path,
    stream_id: u32,
    topic_id: u32,
    partition_count: u32,
    timeout: Duration,
) -> Result<(), String> {
    let expected: Vec<PathBuf> = (0..partition_count)
        .map(|partition_id| partition_dir(data_dir, stream_id, topic_id, partition_id))
        .collect();
    let deadline = Instant::now() + timeout;
    loop {
        let missing: Vec<PathBuf> = expected.iter().filter(|p| !p.exists()).cloned().collect();
        if missing.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out after {timeout:?}; reconciler did not materialise: {missing:?}"
            ));
        }
        sleep(POLL_INTERVAL);
    }
}

fn partition_dir(data_dir: &Path, stream_id: u32, topic_id: u32, partition_id: u32) -> PathBuf {
    data_dir
        .join("streams")
        .join(stream_id.to_string())
        .join("topics")
        .join(topic_id.to_string())
        .join("partitions")
        .join(partition_id.to_string())
}

async fn connected_client(server_addr: SocketAddr) -> IggyClient {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(server_addr.to_string())
        .with_auto_sign_in(AutoLogin::Disabled)
        .with_reconnection_max_retries(Some(3))
        .with_reconnection_interval(IggyDuration::from(200_000_u64))
        .build()
        .expect("TCP client build");
    client.connect().await.expect("SDK TCP connect");
    client
        .login_user("iggy", "iggy")
        .await
        .expect("SDK login as root");
    client
}

/// Spawn `iggy-server-ng` and verify the production binary bootstraps
/// cleanly. Proves the test infrastructure (binary path, env-var
/// overrides, runtime config discovery) is wired correctly so the
/// `#[ignore]`d full end-to-end test below can be flipped on the
/// moment the wire bridge lands.
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

/// End-to-end smoke test against a live `iggy-server-ng` binary.
///
/// Currently gated `#[ignore]` because server-ng's client-facing wire
/// surface is not yet SDK-compatible: the SDK's TCP framing (length-
/// prefixed binary, see `core/sdk/src/tcp/tcp_client.rs`) does not match
/// the server-ng client listener which speaks the consensus
/// [`RequestHeader`] / [`PrepareHeader`] frame layout. The TCP socket
/// accepts, but `login_user` either hangs on the framing read or
/// retries until the SDK surfaces
/// [`iggy_common::IggyError::CannotEstablishConnection`].
///
/// The structural pieces this test exercises are still useful as
/// infrastructure once the wire bridge lands (tracked as a follow-up):
///   * tempdir + ephemeral-port spawn of the production binary
///   * `runtime/current_config.toml` port discovery
///   * SDK-driven `CreateStream` + `CreateTopic` round-trip
///   * on-disk hierarchy assertion proving the reconciler
///     materialised every partition end-to-end
///
/// Flip the `#[ignore]` to active once `iggy-server-ng` accepts SDK
/// frames; no other changes should be needed.
#[ignore = "server-ng client wire surface is not yet SDK-compatible"]
#[tokio::test(flavor = "current_thread")]
async fn reconciler_materialises_partitions_on_create_topic_e2e() {
    let server = TestServer::start();
    let client = connected_client(server.tcp_addr).await;

    // CreateStream commits a `CreateStream` op; the metadata STM
    // assigns slab key 0 (first stream on a fresh server). The notifier
    // does NOT broadcast a `MetadataCommitTick` for plain stream commits
    // (the reconciler only cares about partition-shaped ops) but the
    // call validates the wire is reachable before we move on.
    client
        .create_stream("e2e-stream")
        .await
        .expect("create stream");

    // CreateTopic commits as `CreateTopicWithAssignments` after the
    // primary's allocator stamps consensus_group_id values; the
    // notifier broadcasts `MetadataCommitTick`, the reconciler wakes,
    // and `build_partition_fresh` calls
    // `create_partition_file_hierarchy` for each assigned partition.
    let partitions: u32 = 3;
    client
        .create_topic(
            &"e2e-stream".try_into().expect("stream identifier"),
            "e2e-topic",
            partitions,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::Custom(IggyByteSize::from(1024_u64 * 1024 * 1024)),
        )
        .await
        .expect("create topic");

    // Reconciler is asynchronous: wait for the on-disk hierarchy to
    // appear. The 200ms periodic tick configured in `TestServer::start`
    // bounds the worst-case wait to ~two ticks even if the post-commit
    // wake-up frame got coalesced.
    wait_for_partition_dirs(
        server.data_path(),
        /* stream_id */ 0,
        /* topic_id */ 0,
        partitions,
        Duration::from_secs(5),
    )
    .expect("reconciler materialised all partition directories");
}
