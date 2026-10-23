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

//! End-to-end tests driving **real Kafka clients** against the gateway.
//!
//! Every other suite in this crate hand-builds wire frames, which cannot catch a client
//! compatibility problem by construction. One already slipped through that way: advertising
//! `SaslHandshake` from v1 rather than v0 passed every hand-built test and made librdkafka report
//! "SASL Handshake not supported by broker" before it sent anything. These tests exist so that
//! class of bug fails in CI instead of during manual testing.
//!
//! The stack is real on all three sides: a spawned `iggy-server` process, the gateway in-process
//! with a real `IggyAuthenticator`, and a client from a container. They automate categories S and T
//! of `docs/MANUAL_TESTING.md`, and tests G9 to G11.
//!
//! The Fetch tests swap the authenticator for a bridge: kcat writes records through Produce, and
//! kcat and the Java consumer read them back through Fetch. The group tests run Java consumers in
//! one group across a gateway restart and a member leaving.
//!
//! Prerequisites are Docker, an already-built `iggy-server`, and a host JDK (`javac`) for the
//! `offsetsForTimes` probe. Missing any skips, the way the wire-fixture suites do, unless
//! `KAFKA_E2E_REQUIRED=1` is set, which turns a skip into a failure so a broken CI step cannot
//! leave these silently green.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use iggy::prelude::IggyTimestamp;
use secrecy::SecretString;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::{
    DEFAULT_MAX_MESSAGE_SIZE, IggyBridge, IggyBridgeConfig, IggyTls, ProduceSessionLimits,
    TopicMapping,
};
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::protocol::auto_topic_creation::AutoTopicCreationConfig;
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/certs.rs"]
mod certs;
#[path = "common/server.rs"]
mod server;

use certs::{Issued, TestPki};
use iggy_gateway_kafka::tls::{ClientAuth, TlsConfig};
use server::{spawn_gateway as spawn_listeners, spawn_test_server_with_authenticator};

const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
const KAFKA_IMAGE: &str = "apache/kafka:3.9.0";
const ROOT_USER: &str = "iggy";
const ROOT_PASSWORD: &str = "iggy";
/// Password for every non-root principal these tests create.
const USER_PASSWORD: &str = "s3cretpass";
/// The topic the bridged tests write to and read from. It has one partition.
const TOPIC: &str = "orders";
/// What the bridged tests write, as `(key, value)`, and read back in this order.
const RECORDS: [(&str, &str); 3] = [("k1", "alpha"), ("k2", "beta"), ("k3", "gamma")];
/// The topic the group member test shares. Two partitions, so two members get one each.
const GROUP_TOPIC: &str = "payments";
const GROUP_TOPIC_PARTITIONS: u32 = 2;

/// Budget for `iggy-server` to start listening. Generous: it is a cold process start, and a debug
/// build on a loaded machine is not quick.
const SERVER_READY_TIMEOUT: Duration = Duration::from_secs(45);

/// Wall-clock cap on one client container. The ACL test runs five in a row inside nextest's 300s
/// kill budget, so a single wedged client must fail on its own terms, well before that budget
/// kills the test and hides which step hung.
const CLIENT_RUN_TIMEOUT: &str = "45s";

/// Per-request and per-call budget for the Java admin tools. Their defaults (30s and 60s) let one
/// unanswered request eat most of `CLIENT_RUN_TIMEOUT` retrying.
const JAVA_CLIENT_TIMEOUT_MS: u32 = 10_000;

/// Cap on a client left running, enforced by `timeout` inside its container. Longer than
/// `CLIENT_RUN_TIMEOUT`, because a group member lives through several rebalances.
const RUNNING_CLIENT_TIMEOUT: &str = "120s";

/// How long a test waits for the next line it expects from a running client.
const CLIENT_EVENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Above the gateway's 25s shutdown drain.
const GATEWAY_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Numbers running client containers, so their names differ within one test process.
static NEXT_CONTAINER: AtomicUsize = AtomicUsize::new(0);

/// Reports why the suite cannot run, and whether that is fatal.
///
/// Returns `true` when the caller should skip. `KAFKA_E2E_REQUIRED=1` makes it panic instead, so a
/// CI job that means to run these fails loudly rather than reporting a pass over zero assertions.
fn skip(reason: &str) -> bool {
    assert!(
        std::env::var("KAFKA_E2E_REQUIRED").as_deref() != Ok("1"),
        "KAFKA_E2E_REQUIRED=1 but the suite cannot run: {reason}"
    );
    eprintln!("skipping real-client end-to-end test: {reason}");
    true
}

fn docker_missing() -> bool {
    let available = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if available {
        false
    } else {
        skip("docker is unavailable")
    }
}

/// Whether `image` is already in the local Docker store.
///
/// Containers run with `--pull never`, so a missing image would otherwise surface as a client that
/// failed to start, reported against whichever feature that test happened to cover.
/// Whether this host can run `run_client` at all.
///
/// `run_client` depends on two things outside Docker: `--network host` reaching the host's
/// loopback, which only Linux does, and GNU `timeout` with `--kill-after`, which macOS lacks. A
/// host without either would get past `stack` and then panic inside `expect_ran` or the spawn
/// itself, reading as a gateway failure rather than a missing prerequisite.
fn host_unsupported() -> bool {
    if !cfg!(target_os = "linux") {
        return skip("`docker run --network host` reaches the host loopback only on Linux");
    }
    let gnu_timeout = Command::new("timeout")
        .args(["--kill-after=1s", "1s", "true"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if gnu_timeout {
        false
    } else {
        skip("GNU `timeout` with `--kill-after` is unavailable")
    }
}

fn image_present(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Locates the already-built `iggy-server` alongside this test binary. Does not build it.
fn iggy_server_binary() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    // .../target/<profile>/deps/<test binary> -> .../target/<profile>
    dir.pop();
    dir.pop();
    let candidate = dir.join(format!("iggy-server{}", std::env::consts::EXE_SUFFIX));
    candidate.is_file().then_some(candidate)
}

/// Outcome of waiting for the spawned server to come up.
enum Ready {
    Listening,
    /// The process exited before it bound, carrying its status.
    Exited(String),
    TimedOut,
}

/// A spawned `iggy-server`, killed on drop.
///
/// The environment recipe mirrors `bridge_iggy_integration_tests.rs`, which explains each setting:
/// the other listeners are off so concurrently spawned servers do not fight over their fixed
/// ports, and the shard pool is capped so a spawned server does not size itself to the whole
/// machine against unrelated packages' tests in the same run.
struct TestServer {
    child: Child,
    address: String,
    http_address: String,
    _data_dir: tempfile::TempDir,
}

impl TestServer {
    fn spawn() -> Result<Self, String> {
        let Some(binary) = iggy_server_binary() else {
            return Err(
                "iggy-server is not built; run `cargo build --bin iggy-server` first".to_string(),
            );
        };
        let data_dir = tempfile::tempdir().expect("create server data dir");
        let port = free_port();
        let address = format!("127.0.0.1:{port}");
        let http_address = format!("127.0.0.1:{}", free_port());

        let child = Command::new(binary)
            .arg("--fresh")
            .env("IGGY_PATH", data_dir.path().display().to_string())
            .env("IGGY_TCP_ADDRESS", &address)
            // Left on, unlike the sibling suite: the three-principal procedure creates its
            // non-root users over the HTTP API, which is the only administrative surface reachable
            // from a test without pulling in the SDK.
            .env("IGGY_HTTP_ENABLED", "true")
            .env("IGGY_HTTP_ADDRESS", &http_address)
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .env("IGGY_SHARDING_PIN_CORES", "false")
            .env("IGGY_SHARDING_CPU_ALLOCATION", "0..4")
            .env("IGGY_ROOT_USERNAME", ROOT_USER)
            .env("IGGY_ROOT_PASSWORD", ROOT_PASSWORD)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn iggy-server");

        let mut server = Self {
            child,
            address,
            http_address,
            _data_dir: data_dir,
        };
        match server.wait_ready() {
            Ready::Listening => Ok(server),
            Ready::Exited(status) => Err(format!(
                "iggy-server exited during startup with {status}; its output is suppressed, so \
                 rerun it by hand with the same environment to see why"
            )),
            Ready::TimedOut => Err(format!(
                "iggy-server never bound {} within {SERVER_READY_TIMEOUT:?}",
                server.address
            )),
        }
    }

    fn wait_ready(&mut self) -> Ready {
        let deadline = Instant::now() + SERVER_READY_TIMEOUT;
        while Instant::now() < deadline {
            // Both listeners, not just the data one: the server binds HTTP after TCP, so probing
            // TCP alone declares readiness while the provisioning calls that follow would still
            // be refused.
            if std::net::TcpStream::connect(&self.address).is_ok()
                && std::net::TcpStream::connect(&self.http_address).is_ok()
            {
                return Ready::Listening;
            }
            // Without this a server that dies at boot burns the whole budget and is then reported
            // as a timeout, which sends the reader looking at the wrong thing entirely.
            if let Ok(Some(status)) = self.child.try_wait() {
                return Ready::Exited(status.to_string());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Ready::TimedOut
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Binds an ephemeral port and releases it, so the server can take it.
///
/// A window exists between release and rebind. Acceptable here because this suite is serialized
/// into its own nextest group, so nothing else in it is drawing ports concurrently.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Starts the gateway with SASL on, verifying credentials against `iggy_address`.
async fn spawn_gateway(iggy_address: &str) -> SocketAddr {
    let config = GatewayConfig {
        listeners: vec![ListenerConfig::new(
            SecurityProtocol::SaslPlaintext,
            "127.0.0.1:0",
        )],
        ..GatewayConfig::default()
    };
    let authenticator = Arc::new(IggyAuthenticator::new(iggy_address.to_string()));
    let (addr, shutdown) = spawn_test_server_with_authenticator(config, authenticator).await;
    // Held for the test's lifetime: dropping the sender shuts the gateway down mid-exchange.
    std::mem::forget(shutdown);
    addr
}

/// Starts the gateway with a bridge to `server` and SASL off. Creates `TOPIC` first, because
/// neither Produce nor Fetch creates one.
async fn spawn_bridged_gateway(server: &TestServer) -> SocketAddr {
    let gateway = start_bridged_gateway(server, &[(TOPIC, 1)]).await;
    // Held for the test's lifetime: dropping the sender shuts the gateway down mid-exchange.
    std::mem::forget(gateway.shutdown);
    gateway.addr
}

/// A bridged gateway the test holds. Dropping it shuts the gateway down.
struct BridgedGateway {
    addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
    task: JoinHandle<()>,
}

impl BridgedGateway {
    /// Shuts the gateway down and waits until it has stopped, so a test can start a new one in
    /// its place.
    async fn stop(self) {
        drop(self.shutdown);
        tokio::time::timeout(GATEWAY_STOP_TIMEOUT, self.task)
            .await
            .expect("the gateway stops within its shutdown drain")
            .expect("the gateway task does not panic");
    }
}

/// As [`spawn_bridged_gateway`], creating each `(topic, partitions)` first.
async fn start_bridged_gateway(server: &TestServer, topics: &[(&str, u32)]) -> BridgedGateway {
    start_bridged_gateway_with(server, topics, GatewayConfig::default()).await
}

/// [`start_bridged_gateway`] with `gateway_config` instead of the defaults. Its listeners are
/// replaced by one plaintext listener on an ephemeral port.
async fn start_bridged_gateway_with(
    server: &TestServer,
    topics: &[(&str, u32)],
    gateway_config: GatewayConfig,
) -> BridgedGateway {
    start_gateway(server, topics, false, gateway_config).await
}

/// A bridge to `server` as the root user, with the default topic mapping.
async fn root_bridge(server: &TestServer) -> IggyBridge {
    let config = IggyBridgeConfig {
        address: server.address.clone(),
        username: ROOT_USER.to_string(),
        password: SecretString::from(ROOT_PASSWORD.to_string()),
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("the default mapping is valid"),
        max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        tls: IggyTls::default(),
        produce_sessions: ProduceSessionLimits::default(),
    };
    IggyBridge::connect(config)
        .await
        .expect("the bridge connects to a ready server")
}

/// A bridged gateway on `gateway_config`, with SASL on when `sasl` is set, creating each
/// `(topic, partitions)` first. Its listeners are replaced by one on an ephemeral port.
async fn start_gateway(
    server: &TestServer,
    topics: &[(&str, u32)],
    sasl: bool,
    gateway_config: GatewayConfig,
) -> BridgedGateway {
    let bridge = root_bridge(server).await;
    for &(topic, partitions) in topics {
        bridge
            .ensure_stream_and_topic(topic, partitions)
            .await
            .expect("create the topic");
    }
    let listener = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let protocol = if sasl {
        SecurityProtocol::SaslPlaintext
    } else {
        SecurityProtocol::Plaintext
    };
    let listener = BoundListener::new(ListenerConfig::new(protocol, addr.to_string()), listener);
    let mut gateway = KafkaGateway::new(gateway_config).with_bridge(Some(Arc::new(bridge)));
    if sasl {
        gateway =
            gateway.with_authenticator(Arc::new(IggyAuthenticator::new(server.address.clone())));
    }
    let (shutdown, receiver) = broadcast::channel(1);
    let task = tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    BridgedGateway {
        addr,
        shutdown,
        task,
    }
}

/// Runs a container against the host network and returns its combined output.
///
/// `--network host` is what lets a containerised client reach a gateway bound to the host's
/// loopback. It is Linux-specific, which matches where CI runs.
///
/// This blocks the calling thread for the life of the container, which is why every test here uses
/// a multi-threaded runtime: the gateway runs as a spawned task, and on the single-threaded runtime
/// `#[tokio::test]` gives by default, this call would starve it and nothing would ever listen.
///
/// `--pull never` keeps a registry download out of the test's time budget: images are pulled up
/// front, and `stack` skips when they are absent. `timeout` bounds the run itself, and a run it
/// cuts short exits non-zero, so `expect_ran` reports it rather than asserting over partial output.
fn run_client(image: &str, args: &[&str], mounts: &[(&str, &str)]) -> ClientRun {
    let mut command = Command::new("timeout");
    command.args(["--kill-after=10s", CLIENT_RUN_TIMEOUT]);
    command.args([
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "host",
    ]);
    for (host, guest) in mounts {
        command.args(["-v", &format!("{host}:{guest}:ro")]);
    }
    command.arg(image).args(args);
    let output = command.output().expect("run client container");
    ClientRun {
        succeeded: output.status.success(),
        text: format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// What a client container produced, and whether it ran at all.
///
/// The exit status is carried deliberately. Every negative assertion in this suite is of the form
/// "the output does not contain X", and a container that never started produces output satisfying
/// all of them. Without this a wrong image tag, a malformed config or a Docker failure reads as a
/// passing test.
struct ClientRun {
    succeeded: bool,
    text: String,
}

impl ClientRun {
    /// Asserts the client itself ran, then hands back its output for content assertions.
    fn expect_ran(self, what: &str) -> String {
        assert!(
            self.succeeded,
            "{what}: the client container failed to run, so any assertion about its output would \
             be meaningless. Output: {}",
            self.text
        );
        self.text
    }

    /// For the cases where the client is *expected* to fail, so only the output matters.
    fn text(self) -> String {
        self.text
    }
}

/// A client container left running, so the test can act while it runs. `run_client` returns only
/// after its container exits.
///
/// Lines arrive over a channel as the client prints them. Dropping this removes the container,
/// which `--rm` does not do while a client outlives a failed assertion. The `timeout` inside the
/// container bounds it if this process dies before it can drop.
struct RunningClient {
    container: String,
    process: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
}

impl RunningClient {
    fn start(image: &str, args: &[&str]) -> Self {
        let container = format!(
            "iggy-kafka-e2e-{}-{}",
            std::process::id(),
            NEXT_CONTAINER.fetch_add(1, Ordering::Relaxed)
        );
        let mut process = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--pull",
                "never",
                "--network",
                "host",
                "--name",
            ])
            .arg(&container)
            .arg(image)
            .args(["timeout", "--kill-after=10s", RUNNING_CLIENT_TIMEOUT])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start client container");
        let (sender, lines) = mpsc::channel();
        forward_lines(
            process.stdout.take().expect("stdout is piped"),
            sender.clone(),
        );
        forward_lines(process.stderr.take().expect("stderr is piped"), sender);
        Self {
            container,
            process,
            lines,
            output: Vec::new(),
        }
    }

    /// Reads output until `found` returns a value for a line, and returns that value. Fails the
    /// test if the client exits first or `CLIENT_EVENT_TIMEOUT` passes.
    fn wait_for<T>(&mut self, what: &str, mut found: impl FnMut(&str) -> Option<T>) -> T {
        let deadline = Instant::now() + CLIENT_EVENT_TIMEOUT;
        loop {
            let Some(line) = self.next_line(what, deadline) else {
                panic!(
                    "{what}: the client exited first. Output:\n{}",
                    self.output.join("\n")
                );
            };
            if let Some(value) = found(&line) {
                return value;
            }
        }
    }

    /// Waits for the client to exit on its own, checks that it succeeded, and returns every line
    /// it printed.
    fn wait_exit(mut self, what: &str) -> Vec<String> {
        let deadline = Instant::now() + CLIENT_EVENT_TIMEOUT;
        while self.next_line(what, deadline).is_some() {}
        let status = self.process.wait().expect("wait for the client container");
        assert!(
            status.success(),
            "{what}: the client container exited with {status}. Output:\n{}",
            self.output.join("\n")
        );
        std::mem::take(&mut self.output)
    }

    /// The next line the client prints, also kept in `output`, or `None` once it has exited and
    /// every line was read. Fails the test at `deadline`.
    fn next_line(&mut self, what: &str, deadline: Instant) -> Option<String> {
        match self
            .lines
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(line) => {
                self.output.push(line.clone());
                Some(line)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => None,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "{what}: nothing within {CLIENT_EVENT_TIMEOUT:?}. Output:\n{}",
                self.output.join("\n")
            ),
        }
    }
}

impl Drop for RunningClient {
    fn drop(&mut self) {
        // After a clean exit `--rm` has already removed it, and this fails harmlessly.
        let _ = Command::new("docker")
            .args(["rm", "--force"])
            .arg(&self.container)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Sends each line of `pipe` over `sender`, on a thread of its own, until either end closes.
fn forward_lines(pipe: impl Read + Send + 'static, sender: mpsc::Sender<String>) {
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
}

fn kcat(addr: SocketAddr, username: &str, password: &str, mechanism: &str) -> ClientRun {
    run_client(
        KCAT_IMAGE,
        &[
            "-b",
            &addr.to_string(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            &format!("sasl.mechanisms={mechanism}"),
            "-X",
            &format!("sasl.username={username}"),
            "-X",
            &format!("sasl.password={password}"),
            "-L",
        ],
        &[],
    )
}

/// Creates an Iggy user over the HTTP API with the given global permissions.
///
/// The spec's three-principal procedure needs non-root accounts, and root holds every flag, so a
/// suite that only ever authenticates as root checks neither the inheritance projection nor the
/// empty view against a real client.
fn create_user(http: &str, token: &str, username: &str, permissions: &str) {
    let body = format!(
        r#"{{"username":"{username}","password":"{USER_PASSWORD}","status":"active","permissions":{permissions}}}"#
    );
    let status = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-X",
            "POST",
            &format!("http://{http}/users"),
            "-H",
            &format!("Authorization: Bearer {token}"),
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
        ])
        .output()
        .expect("create user");
    let code = String::from_utf8_lossy(&status.stdout).to_string();
    assert!(
        code.starts_with('2'),
        "creating {username} returned HTTP {code}"
    );
}

/// Logs in as root over HTTP and returns the bearer token.
fn root_token(http: &str) -> String {
    let output = Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            &format!("http://{http}/users/login"),
            "-H",
            "Content-Type: application/json",
            "-d",
            &format!(r#"{{"username":"{ROOT_USER}","password":"{ROOT_PASSWORD}"}}"#),
        ])
        .output()
        .expect("root login");
    let body = String::from_utf8_lossy(&output.stdout);
    // Avoids a JSON dependency for one field: the token is the value after this key.
    let key = "\"token\":\"";
    let start = body.find(key).expect("login response carries a token") + key.len();
    let end = start + body[start..].find('"').expect("token is terminated");
    body[start..end].to_string()
}

/// Writes a JAAS client config for the Java tools and returns its path, kept alive by the handle.
fn java_client_config(username: &str, password: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("create config dir");
    let path = dir.path().join("client.properties");
    std::fs::write(
        &path,
        format!(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism=PLAIN\n\
             sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"{username}\" password=\"{password}\";\n\
             request.timeout.ms={JAVA_CLIENT_TIMEOUT_MS}\n\
             default.api.timeout.ms={JAVA_CLIENT_TIMEOUT_MS}\n"
        ),
    )
    .expect("write client config");
    (dir, path)
}

/// Brings up a server and a gateway, or reports why it could not.
async fn stack() -> Option<(TestServer, SocketAddr)> {
    let server = ready_server()?;
    let gateway = spawn_gateway(&server.address).await;
    Some((server, gateway))
}

/// As [`stack`], with a bridged gateway and SASL off.
async fn bridged_stack() -> Option<(TestServer, SocketAddr)> {
    let server = ready_server()?;
    let gateway = spawn_bridged_gateway(&server).await;
    Some((server, gateway))
}

/// As [`bridged_stack`], with SASL on, so every client has to authenticate before the bridge
/// serves it.
async fn sasl_bridged_stack() -> Option<(TestServer, SocketAddr)> {
    let server = ready_server()?;
    let gateway = start_gateway(&server, &[(TOPIC, 1)], true, GatewayConfig::default()).await;
    // Held for the test's lifetime: dropping the sender shuts the gateway down mid-exchange.
    std::mem::forget(gateway.shutdown);
    Some((server, gateway.addr))
}

/// Brings up a server once every prerequisite holds, or reports why it could not.
fn ready_server() -> Option<TestServer> {
    if host_unsupported() || docker_missing() {
        return None;
    }
    if let Some(image) = [KCAT_IMAGE, KAFKA_IMAGE]
        .into_iter()
        .find(|image| !image_present(image))
    {
        skip(&format!(
            "client image {image} is not pulled; run `docker pull {image}`"
        ));
        return None;
    }
    match TestServer::spawn() {
        Ok(server) => Some(server),
        Err(reason) => {
            skip(&reason);
            None
        }
    }
}

/// Writes `records` to partition 0 of `TOPIC` through the gateway, with kcat's producer and one
/// header on each record.
fn produce_with_kcat(gateway: SocketAddr, records: &[(&str, &str)]) {
    produce_to_partition(gateway, TOPIC, 0, records);
}

/// As [`produce_with_kcat`], to `partition` of `topic`.
fn produce_to_partition(
    gateway: SocketAddr,
    topic: &str,
    partition: u32,
    records: &[(&str, &str)],
) {
    kcat_produce(gateway, topic, partition, records, &[]).expect_ran("kcat produce");
}

/// The kcat properties that authenticate as `username` with SASL/PLAIN.
fn sasl_plain_args(username: &str, password: &str) -> Vec<String> {
    [
        "security.protocol=SASL_PLAINTEXT".to_string(),
        "sasl.mechanisms=PLAIN".to_string(),
        format!("sasl.username={username}"),
        format!("sasl.password={password}"),
    ]
    .into_iter()
    .flat_map(|property| ["-X".to_string(), property])
    .collect()
}

/// Writes `records` to `partition` of `topic` with kcat's producer, one header on each record,
/// with `auth` prepended to kcat's arguments.
fn kcat_produce(
    gateway: SocketAddr,
    topic: &str,
    partition: u32,
    records: &[(&str, &str)],
    auth: &[String],
) -> ClientRun {
    let dir = tempfile::tempdir().expect("create records dir");
    let path = dir.path().join("records.txt");
    let lines: String = records
        .iter()
        .flat_map(|&(key, value)| [key, ":", value, "\n"])
        .collect();
    std::fs::write(&path, lines).expect("write records");
    let bootstrap = gateway.to_string();
    let partition = partition.to_string();
    let mut args: Vec<&str> = vec!["-b", &bootstrap];
    args.extend(auth.iter().map(String::as_str));
    args.extend([
        "-P",
        "-t",
        topic,
        "-p",
        &partition,
        "-K",
        ":",
        "-H",
        "trace=e2e",
        "-l",
        "/tmp/records.txt",
    ]);
    run_client(
        KCAT_IMAGE,
        &args,
        &[(
            path.to_str().expect("records path is utf-8"),
            "/tmp/records.txt",
        )],
    )
}

/// Reads partition 0 of `TOPIC` from the beginning to the high watermark with kcat's consumer,
/// with `auth` prepended to kcat's arguments. One `record <offset> <key>=<value> <headers>` line
/// per record.
fn kcat_consume(gateway: SocketAddr, auth: &[String]) -> ClientRun {
    let bootstrap = gateway.to_string();
    let mut args: Vec<&str> = vec!["-b", &bootstrap];
    args.extend(auth.iter().map(String::as_str));
    // `-e` stops at the end of the partition, which kcat takes from the high watermark that Fetch
    // sends. A wrong watermark stops it early or holds it until the container timeout.
    args.extend([
        "-C",
        "-t",
        TOPIC,
        "-p",
        "0",
        "-o",
        "beginning",
        "-e",
        "-f",
        "record %o %k=%s %h\n",
    ]);
    run_client(KCAT_IMAGE, &args, &[])
}

/// Asserts `output` holds exactly `records`, in order from offset 0, and that kcat stopped at the
/// high watermark after them.
fn assert_read_in_order(output: &str, records: &[(&str, &str)]) {
    let read: Vec<&str> = output
        .lines()
        .filter(|line| line.starts_with("record "))
        .collect();
    let expected: Vec<String> = records
        .iter()
        .enumerate()
        .map(|(offset, (key, value))| format!("record {offset} {key}={value} trace=e2e"))
        .collect();
    assert_eq!(
        read, expected,
        "librdkafka must read every record in order, with its key and header, got: {output}"
    );
    assert!(
        output.contains(&format!(
            "Reached end of topic {TOPIC} [0] at offset {}",
            records.len()
        )),
        "kcat must stop at the high watermark, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_valid_credentials_when_a_real_client_connects_should_serve_metadata() {
    let Some((_server, gateway)) = stack().await else {
        return;
    };
    let output = kcat(gateway, ROOT_USER, ROOT_PASSWORD, "PLAIN").expect_ran("valid credentials");
    assert!(
        output.contains("Metadata for all topics"),
        "librdkafka must authenticate and receive metadata, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_wrong_password_when_a_real_client_connects_should_report_an_auth_failure() {
    let Some((_server, gateway)) = stack().await else {
        return;
    };
    // kcat exits non-zero here by design, so only its output is asserted.
    let output = kcat(gateway, ROOT_USER, "definitely-not-the-password", "PLAIN").text();
    assert!(
        output.contains("Authentication failed"),
        "a rejected credential must reach the client as an auth failure, got: {output}"
    );
    assert!(
        !output.contains("Metadata for all topics"),
        "nothing may be served to a rejected client"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_an_unsupported_mechanism_should_name_the_supported_one_back() {
    let Some((_server, gateway)) = stack().await else {
        return;
    };
    let output = kcat(gateway, ROOT_USER, ROOT_PASSWORD, "SCRAM-SHA-256").text();
    assert!(
        output.contains("PLAIN"),
        "the refusal must name what is supported or an operator cannot act on it, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_no_credentials_when_a_real_client_connects_should_be_refused() {
    let Some((_server, gateway)) = stack().await else {
        return;
    };
    // kcat exits non-zero here by design, so only its output is asserted, and the assertion has
    // to be a positive one: a container that never started also fails to print the metadata this
    // test forbids, which would pass over nothing. librdkafka only reports this particular
    // diagnosis after it connected and the broker then dropped it before authenticating, so it
    // stands in for the refusal itself.
    let output = run_client(KCAT_IMAGE, &["-b", &gateway.to_string(), "-L"], &[]).text();
    assert!(
        output.contains("broker might require SASL authentication"),
        "an unauthenticated client must be disconnected by the broker, got: {output}"
    );
    assert!(
        !output.contains("Metadata for all topics"),
        "an unauthenticated client must not be served, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_the_java_client_should_authenticate_and_list_the_sasl_apis() {
    // The Java client is the one that negotiates `SaslAuthenticate` v2 and sends `ApiVersions`
    // both before and after authenticating, none of which kcat exercises identically.
    let Some((_server, gateway)) = stack().await else {
        return;
    };
    let (_dir, config) = java_client_config(ROOT_USER, ROOT_PASSWORD);
    let output = run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-broker-api-versions.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--command-config",
            "/tmp/client.properties",
        ],
        &[(
            config.to_str().expect("config path is utf-8"),
            "/tmp/client.properties",
        )],
    )
    .expect_ran("java client api-versions");
    assert!(
        output.contains("SaslHandshake(17)"),
        "the Java client must authenticate and read the advertisement, got: {output}"
    );
    assert!(
        output.contains("SaslAuthenticate(36)"),
        "both SASL keys must be advertised while the feature is on, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_distinct_principals_when_listing_acls_should_describe_each_differently() {
    // The spec's category T procedure. Root alone proves nothing about the mapping, because it
    // holds every flag: a fixed set would satisfy it. Each of the others is chosen to fail
    // differently if the projection is wrong. The consume-only principal shows the projection
    // distinguishes principals and applies Iggy's inheritance; the poll-only one is the single
    // case that separates the group binding's real source from `poll_messages`; the produce-only
    // one shows a write grant drags in neither a read nor a group; and the ungranted one shows an
    // empty view is a successful answer rather than a failure.
    let Some((server, gateway)) = stack().await else {
        return;
    };
    let token = root_token(&server.http_address);
    create_user(
        &server.http_address,
        &token,
        "consumer-only",
        r#"{"global":{"manage_servers":false,"read_servers":false,"manage_users":false,
           "read_users":false,"manage_streams":false,"read_streams":false,"manage_topics":false,
           "read_topics":true,"poll_messages":false,"send_messages":false},"streams":null}"#,
    );
    create_user(&server.http_address, &token, "no-grants", "null");
    // Discriminates the group-derivation fix. `poll_messages` alone leaves the projected topic
    // read false, so Iggy would refuse this principal a consumer group, yet the old derivation
    // rendered one from polling. Reverting that fix makes this principal's listing grow a GROUP
    // section, which the assertion below catches.
    create_user(
        &server.http_address,
        &token,
        "poller-only",
        r#"{"global":{"manage_servers":false,"read_servers":false,"manage_users":false,
           "read_users":false,"manage_streams":false,"read_streams":false,"manage_topics":false,
           "read_topics":false,"poll_messages":true,"send_messages":false},"streams":null}"#,
    );
    create_user(
        &server.http_address,
        &token,
        "producer-only",
        r#"{"global":{"manage_servers":false,"read_servers":false,"manage_users":false,
           "read_users":false,"manage_streams":false,"read_streams":false,"manage_topics":false,
           "read_topics":false,"poll_messages":false,"send_messages":true},"streams":null}"#,
    );

    let root = list_acls(gateway, ROOT_USER, ROOT_PASSWORD);
    assert!(
        root.contains(&format!("principal=User:{ROOT_USER}")),
        "every rendered binding names the authenticated principal, got: {root}"
    );
    assert!(
        grants(&root, "TOPIC", "WRITE"),
        "root holds every permission, so it must be described as able to write, got: {root}"
    );
    assert!(
        grants(&root, "CLUSTER", "DESCRIBE"),
        "root holds the server flags, so the cluster must render as describable, got: {root}"
    );
    assert!(
        !grants(&root, "CLUSTER", "ALTER"),
        "no Iggy flag gates a cluster mutation, so even root must not be shown one, got: {root}"
    );

    let consumer = list_acls(gateway, "consumer-only", USER_PASSWORD);
    // Paired assertions: which operation sits under which resource is the whole claim.
    assert!(
        grants(&consumer, "TOPIC", "READ"),
        "read_topics grants polling in Iggy, so a topic read must render, got: {consumer}"
    );
    assert!(
        grants(&consumer, "TOPIC", "DESCRIBE"),
        "a read grant must also describe, got: {consumer}"
    );
    assert!(
        grants(&consumer, "GROUP", "READ"),
        "a principal Iggy admits to consumer groups needs the derived group binding, got: \
         {consumer}"
    );
    assert!(
        !grants(&consumer, "TOPIC", "WRITE"),
        "it may not write, and describing it as able to would over-report, got: {consumer}"
    );
    assert!(
        !consumer.contains("resourceType=CLUSTER"),
        "it holds no server permission, got: {consumer}"
    );

    let poller = list_acls(gateway, "poller-only", USER_PASSWORD);
    assert!(
        grants(&poller, "TOPIC", "READ"),
        "poll_messages must still render a topic read, got: {poller}"
    );
    assert!(
        !poller.contains("resourceType=GROUP"),
        "polling alone grants no topic read, so Iggy would refuse this principal a consumer \
         group; rendering one over-reports, got: {poller}"
    );

    let producer = list_acls(gateway, "producer-only", USER_PASSWORD);
    assert!(
        grants(&producer, "TOPIC", "WRITE"),
        "send_messages must render a topic write, got: {producer}"
    );
    assert!(
        !producer.contains("resourceType=GROUP"),
        "a principal with no topic read grant must get no group binding, because Iggy would \
         refuse it one, got: {producer}"
    );

    let none = list_acls(gateway, "no-grants", USER_PASSWORD);
    assert!(
        !none.contains("principal=User:"),
        "a principal with no grants must be described with no bindings, got: {none}"
    );
    assert!(
        !none.to_lowercase().contains("error"),
        "an empty view is a successful answer, not a failure, got: {none}"
    );
}

/// Splits `kafka-acls.sh --list` output into `(resource_type, operations)` per resource section.
///
/// Independent substring scans over the whole blob cannot tell which resource an operation belongs
/// to, so `READ on TOPIC` and `DESCRIBE on GROUP` satisfy an assertion meant to prove the reverse.
fn acl_sections(listing: &str) -> Vec<(String, Vec<String>)> {
    let mut sections: Vec<(String, Vec<String>)> = Vec::new();
    for line in listing.lines() {
        if let Some(rest) = line.split_once("resourceType=") {
            let resource = rest
                .1
                .split([',', ')'])
                .next()
                .unwrap_or_default()
                .trim()
                .to_string();
            sections.push((resource, Vec::new()));
        } else if let Some(rest) = line.split_once("operation=") {
            let operation = rest
                .1
                .split([',', ')'])
                .next()
                .unwrap_or_default()
                .trim()
                .to_string();
            if let Some(current) = sections.last_mut() {
                current.1.push(operation);
            }
        }
    }
    sections
}

/// Whether `listing` grants `operation` on `resource`, with the pairing actually checked.
fn grants(listing: &str, resource: &str, operation: &str) -> bool {
    acl_sections(listing)
        .iter()
        .any(|(kind, operations)| kind == resource && operations.iter().any(|op| op == operation))
}

/// Lists a principal's ACLs with the real Kafka admin client.
fn list_acls(gateway: SocketAddr, username: &str, password: &str) -> String {
    let (_dir, config) = java_client_config(username, password);
    run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-acls.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--command-config",
            "/tmp/client.properties",
            "--list",
        ],
        &[(
            config.to_str().expect("config path is utf-8"),
            "/tmp/client.properties",
        )],
    )
    .expect_ran(&format!("listing ACLs as {username}"))
}

/// Runs `kafka-topics.sh` with `args` against `gateway`. No `--command-config`: the bridged
/// stack this test uses runs with SASL off, same as the kcat calls against it above.
fn kafka_topics(gateway: SocketAddr, args: &[&str]) -> ClientRun {
    let bootstrap = gateway.to_string();
    let mut full_args: Vec<&str> = vec![
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &bootstrap,
    ];
    full_args.extend_from_slice(args);
    run_client(KAFKA_IMAGE, &full_args, &[])
}

/// Runs `kafka-configs.sh` with `args` against `gateway`, with SASL off like [`kafka_topics`].
fn kafka_configs(gateway: SocketAddr, args: &[&str]) -> ClientRun {
    let bootstrap = gateway.to_string();
    let mut full_args: Vec<&str> = vec![
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        &bootstrap,
    ];
    full_args.extend_from_slice(args);
    run_client(KAFKA_IMAGE, &full_args, &[])
}

/// `kafka-configs.sh --alter` on a topic sends only `IncrementalAlterConfigs` (44): a `SET` for
/// `--add-config` and a `DELETE` for `--delete-config`, after a describe of the topic's dynamic
/// configs. This is the real-tool round trip for that key through the bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_kafka_configs_sh_when_adding_then_deleting_retention_should_round_trip_through_the_bridge()
 {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    let completed = format!("Completed updating config for topic {TOPIC}.");

    let added = kafka_configs(
        gateway,
        &[
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
            "--add-config",
            "retention.ms=8000",
        ],
    )
    .expect_ran("kafka-configs.sh --add-config");
    assert!(
        added.contains(&completed),
        "kafka-configs.sh must report the alter completed, got: {added}"
    );

    let described = kafka_configs(
        gateway,
        &[
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
        ],
    )
    .expect_ran("kafka-configs.sh --describe after --add-config");
    assert!(
        described.contains("retention.ms=8000 sensitive=false"),
        "the describe must show the stored retention.ms, got: {described}"
    );

    let deleted = kafka_configs(
        gateway,
        &[
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
            "--delete-config",
            "retention.ms",
        ],
    )
    .expect_ran("kafka-configs.sh --delete-config");
    assert!(
        deleted.contains(&completed),
        "kafka-configs.sh must report the delete completed, got: {deleted}"
    );

    let described_after = kafka_configs(
        gateway,
        &[
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
        ],
    )
    .expect_ran("kafka-configs.sh --describe after --delete-config");
    assert!(
        described_after.contains("retention.ms=-1 sensitive=false"),
        "a deleted retention.ms reads as never expire, got: {described_after}"
    );
}

/// #3546's own acceptance criterion: `kafka-topics.sh` must work against the gateway for create
/// and delete. (`--describe` needs `DescribeConfigs`, not yet implemented, so it is not covered
/// here.) Runs against the bridged stack, since this exercises the real `CreateTopics`/
/// `DeleteTopics` bridge path, not authentication - the other Java-client tests above already
/// cover SASL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_kafka_topics_sh_when_creating_then_deleting_a_topic_should_round_trip_through_the_bridge()
 {
    const NEW_TOPIC: &str = "kafka-topics-roundtrip";
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };

    let created = kafka_topics(
        gateway,
        &[
            "--create",
            "--topic",
            NEW_TOPIC,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
    )
    .expect_ran("kafka-topics.sh --create");
    assert!(
        created.contains(&format!("Created topic {NEW_TOPIC}")),
        "kafka-topics.sh must report the topic created, got: {created}"
    );

    let listed =
        kafka_topics(gateway, &["--list"]).expect_ran("kafka-topics.sh --list after create");
    assert!(
        listed.lines().any(|line| line.trim() == NEW_TOPIC),
        "the created topic must appear in the listing, got: {listed}"
    );

    let deleted = kafka_topics(gateway, &["--delete", "--topic", NEW_TOPIC])
        .expect_ran("kafka-topics.sh --delete");
    assert!(
        !deleted.to_lowercase().contains("error"),
        "kafka-topics.sh --delete must succeed, got: {deleted}"
    );

    let listed_after =
        kafka_topics(gateway, &["--list"]).expect_ran("kafka-topics.sh --list after delete");
    assert!(
        !listed_after.lines().any(|line| line.trim() == NEW_TOPIC),
        "the deleted topic must no longer appear in the listing, got: {listed_after}"
    );
    assert!(
        listed_after.lines().any(|line| line.trim() == TOPIC),
        "deleting one topic must not take the shared default stream, and `{TOPIC}`, with it, got: \
         {listed_after}"
    );
}

/// `kafka-topics.sh --alter --partitions N` grows a topic through
/// the bridge and `--describe` (Metadata) reports the new count, while a smaller count fails
/// with `INVALID_PARTITIONS` and leaves the count alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_kafka_topics_sh_when_altering_the_partition_count_should_grow_the_topic_through_the_bridge()
 {
    const NEW_TOPIC: &str = "kafka-topics-grow";
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };

    let created = kafka_topics(
        gateway,
        &[
            "--create",
            "--topic",
            NEW_TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    )
    .expect_ran("kafka-topics.sh --create");
    assert!(
        created.contains(&format!("Created topic {NEW_TOPIC}")),
        "kafka-topics.sh must report the topic created, got: {created}"
    );

    let altered = kafka_topics(
        gateway,
        &["--alter", "--topic", NEW_TOPIC, "--partitions", "3"],
    )
    .expect_ran("kafka-topics.sh --alter --partitions 3");
    assert!(
        !altered.to_lowercase().contains("error"),
        "kafka-topics.sh --alter must succeed, got: {altered}"
    );

    let described = kafka_topics(gateway, &["--describe", "--topic", NEW_TOPIC])
        .expect_ran("kafka-topics.sh --describe after the grow");
    assert!(
        described.contains("PartitionCount: 3"),
        "Metadata must report the grown count, got: {described}"
    );

    let shrunk = kafka_topics(
        gateway,
        &["--alter", "--topic", NEW_TOPIC, "--partitions", "2"],
    )
    .text();
    assert!(
        shrunk.contains("InvalidPartitionsException"),
        "a smaller count must fail with INVALID_PARTITIONS, got: {shrunk}"
    );

    let described_after = kafka_topics(gateway, &["--describe", "--topic", NEW_TOPIC])
        .expect_ran("kafka-topics.sh --describe after the refused shrink");
    assert!(
        described_after.contains("PartitionCount: 3"),
        "a refused shrink must leave the count alone, got: {described_after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_records_produced_by_kcat_when_kcat_consumes_should_read_them_back_in_order() {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    let output = kcat_consume(gateway, &[]).expect_ran("kcat consume");
    assert_read_in_order(&output, &RECORDS);
}

/// #4340: SASL and the bridge together. Every other SASL test stops at Metadata on a gateway with
/// no bridge, and every produce and consume test runs with SASL off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_sasl_and_the_bridge_when_kcat_produces_and_consumes_should_read_back_in_order() {
    let Some((_server, gateway)) = sasl_bridged_stack().await else {
        return;
    };
    let auth = sasl_plain_args(ROOT_USER, ROOT_PASSWORD);
    kcat_produce(gateway, TOPIC, 0, &RECORDS, &auth).expect_ran("kcat produce over SASL");
    let output = kcat_consume(gateway, &auth).expect_ran("kcat consume over SASL");
    assert_read_in_order(&output, &RECORDS);
}

/// The other half of the same path: a client the gateway refuses must not reach the bridge. kcat
/// exits non-zero here by design, so only its output and what the partition holds are asserted.
///
/// What the partition holds is read from Iggy, not through the gateway. Every container shares
/// the host's address, so the rejected logins put that address in the gateway's failed-login
/// delay, and a SASL read-back right after could be refused for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_sasl_and_the_bridge_when_a_rejected_client_produces_should_write_nothing() {
    let Some((server, gateway)) = sasl_bridged_stack().await else {
        return;
    };
    let auth = sasl_plain_args(ROOT_USER, ROOT_PASSWORD);
    kcat_produce(gateway, TOPIC, 0, &RECORDS, &auth).expect_ran("kcat produce over SASL");

    let rejected = kcat_produce(
        gateway,
        TOPIC,
        0,
        &[("k4", "delta")],
        &sasl_plain_args(ROOT_USER, "definitely-not-the-password"),
    )
    .text();
    assert!(
        rejected.contains("Authentication failed"),
        "a rejected credential must reach the producer as an auth failure, got: {rejected}"
    );
    // A client that never authenticates is cut off before Produce; the read-back below is what
    // proves its record never landed.
    kcat_produce(gateway, TOPIC, 0, &[("k5", "epsilon")], &[]);

    let watermark = root_bridge(&server)
        .await
        .high_watermark(TOPIC, 0)
        .await
        .expect("read the partition's high watermark");
    assert_eq!(
        watermark,
        i64::try_from(RECORDS.len()).expect("record count fits i64"),
        "only the authenticated producer's records may land"
    );
}

/// G9 of `docs/MANUAL_TESTING.md`, with a real client on both offset APIs. The first run reads
/// every record and commits as it closes. `--from-beginning` applies only to a group with no
/// committed offset, so the second run must print only the record written in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_group_that_committed_when_the_java_consumer_runs_again_should_resume_after_it() {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    let expected: Vec<String> = RECORDS
        .iter()
        .enumerate()
        .map(|(offset, (key, value))| format!("Offset:{offset}\t{key}\t{value}"))
        .collect();
    let first = consume_as_group(gateway, RECORDS.len());
    assert_eq!(
        records_read(&first),
        expected,
        "a group with no offset must start from the beginning, got: {first}"
    );

    produce_with_kcat(gateway, &[("k4", "delta")]);
    let second = consume_as_group(gateway, 1);
    assert_eq!(
        records_read(&second),
        vec![format!("Offset:{}\tk4\tdelta", RECORDS.len())],
        "the group must resume after its commit, got: {second}"
    );
}

/// Reads `count` records of `TOPIC` with the Java console consumer in group `e2e-resume`, from
/// the group's committed offset, or from the beginning when it has none. It commits as it closes.
fn consume_as_group(gateway: SocketAddr, count: usize) -> String {
    let count = count.to_string();
    run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--topic",
            TOPIC,
            "--group",
            "e2e-resume",
            "--from-beginning",
            "--max-messages",
            &count,
            "--timeout-ms",
            "20000",
            "--property",
            "print.offset=true",
            "--property",
            "print.key=true",
        ],
        &[],
    )
    .expect_ran("java console consumer in a group")
}

/// KIP-345 with the Java client. A static consumer sends no `LeaveGroup` on close, so its
/// member stays in the group, holding the only partition, for its whole session. The second run
/// comes back under the same `group.instance.id` and must take that member over at once. Without
/// replacement it would join as a new member, wait out a rebalance its old incarnation never
/// rejoins, and exit after `--timeout-ms` with nothing read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_static_java_consumer_when_it_restarts_within_its_session_should_resume_at_once() {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    let first = consume_as_static_member(gateway, RECORDS.len());
    assert_eq!(
        records_read(&first).len(),
        RECORDS.len(),
        "the first run must read every record, got: {first}"
    );

    produce_with_kcat(gateway, &[("k4", "delta")]);
    let second = consume_as_static_member(gateway, 1);
    assert_eq!(
        records_read(&second),
        vec![format!("Offset:{}\tk4\tdelta", RECORDS.len())],
        "the returning static member must take its partition back, got: {second}"
    );
}

/// [`consume_as_group`] as static member `e2e-static-0` of group `e2e-static`, with a session long
/// enough that its previous incarnation is still a member when it returns.
fn consume_as_static_member(gateway: SocketAddr, count: usize) -> String {
    let count = count.to_string();
    run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--topic",
            TOPIC,
            "--group",
            "e2e-static",
            "--consumer-property",
            "group.instance.id=e2e-static-0",
            "--consumer-property",
            "session.timeout.ms=60000",
            "--from-beginning",
            "--max-messages",
            &count,
            "--timeout-ms",
            "20000",
            "--property",
            "print.offset=true",
            "--property",
            "print.key=true",
        ],
        &[],
    )
    .expect_ran("java console consumer as a static member")
}

/// KIP-345 with librdkafka, which sends `JoinGroup` below v9: the returning member is told the
/// previous leader, acts as a follower, and gets its old assignment back through `SyncGroup`.
/// Without replacement the second run would wait on a rebalance its old incarnation never rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_static_kcat_consumer_when_it_restarts_within_its_session_should_resume_at_once() {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    let first = consume_as_static_kcat(gateway);
    assert_eq!(
        kcat_records_read(&first).len(),
        RECORDS.len(),
        "the first run must read every record, got: {first}"
    );

    produce_with_kcat(gateway, &[("k4", "delta")]);
    let second = consume_as_static_kcat(gateway);
    assert_eq!(
        kcat_records_read(&second),
        vec![format!("record {} k4=delta", RECORDS.len())],
        "the returning static member must take its partition back, got: {second}"
    );
}

/// kcat in balanced mode as static member `e2e-kcat-0` of group `e2e-kcat-static`, reading to
/// the end of its partitions. It commits as it closes and sends no `LeaveGroup`.
fn consume_as_static_kcat(gateway: SocketAddr) -> String {
    run_client(
        KCAT_IMAGE,
        &[
            "-b",
            &gateway.to_string(),
            "-G",
            "e2e-kcat-static",
            "-X",
            "group.instance.id=e2e-kcat-0",
            "-X",
            "session.timeout.ms=60000",
            "-X",
            "auto.offset.reset=earliest",
            "-e",
            "-f",
            "record %o %k=%s\n",
            TOPIC,
        ],
        &[],
    )
    .expect_ran("kcat as a static member")
}

fn kcat_records_read(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| line.starts_with("record "))
        .collect()
}

/// With auto topic creation on, a stock Java producer sends to a topic nobody created and succeeds.
/// Its first `Metadata` is answered `UNKNOWN_TOPIC_OR_PARTITION` while the gateway creates the
/// topic, and it retries until the topic appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_auto_topic_creation_when_the_java_producer_sends_to_a_new_topic_should_succeed() {
    const NEW_TOPIC: &str = "auto-created";
    let Some(server) = ready_server() else {
        return;
    };
    let gateway = start_bridged_gateway_with(
        &server,
        &[],
        GatewayConfig {
            auto_topic_creation: AutoTopicCreationConfig {
                enabled: true,
                partitions: 1,
            },
            ..GatewayConfig::default()
        },
    )
    .await;
    let bootstrap = gateway.addr.to_string();

    let produce = format!(
        "printf 'k1:alpha\\n' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server \
         {bootstrap} --topic {NEW_TOPIC} --property parse.key=true --property key.separator=: \
         --producer-property max.block.ms=20000"
    );
    let produced =
        run_client(KAFKA_IMAGE, &["sh", "-c", &produce], &[]).expect_ran("java console producer");
    assert!(
        !produced.contains("ERROR"),
        "the producer must send to the new topic, got: {produced}"
    );

    let consumed = run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            &bootstrap,
            "--topic",
            NEW_TOPIC,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            "1",
            "--timeout-ms",
            "20000",
            "--property",
            "print.offset=true",
            "--property",
            "print.key=true",
        ],
        &[],
    )
    .expect_ran("java console consumer");
    assert_eq!(
        records_read(&consumed),
        vec!["Offset:0\tk1\talpha"],
        "the auto-created topic must hold the record, got: {consumed}"
    );
    gateway.stop().await;
}

/// The records a console consumer printed, as `Offset:<offset>\t<key>\t<value>`.
fn records_read(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| line.starts_with("Offset:"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_records_when_the_java_consumer_reads_the_partition_should_get_them_in_order() {
    // A second Fetch client with its own code: the Java consumer resolves `earliest` through
    // ListOffsets, then fetches. `--partition` makes it assign the partition, with no group
    // offsets to load or commit.
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    let count = RECORDS.len().to_string();
    let output = run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            &count,
            "--timeout-ms",
            "20000",
            "--property",
            "print.offset=true",
            "--property",
            "print.key=true",
        ],
        &[],
    )
    .expect_ran("java console consumer");
    let read: Vec<&str> = output
        .lines()
        .filter(|line| line.starts_with("Offset:"))
        .collect();
    let expected: Vec<String> = RECORDS
        .iter()
        .enumerate()
        .map(|(offset, (key, value))| format!("Offset:{offset}\t{key}\t{value}"))
        .collect();
    assert_eq!(
        read, expected,
        "the Java consumer must read every record in order, with its key, got: {output}"
    );
    assert!(
        output.contains(&format!("Processed a total of {count} messages")),
        "the Java consumer must stop after the records it asked for, got: {output}"
    );
}

/// G10 of `docs/MANUAL_TESTING.md`. The second gateway has a new bridge and an empty coordinator,
/// as a restart leaves it, so the group's offset can only come from Iggy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_group_that_committed_when_the_gateway_restarts_should_resume_after_it() {
    let Some(server) = ready_server() else {
        return;
    };
    let gateway = start_bridged_gateway(&server, &[(TOPIC, 1)]).await;
    produce_with_kcat(gateway.addr, &RECORDS);
    let first = consume_as_group(gateway.addr, RECORDS.len());
    assert_eq!(
        records_read(&first).len(),
        RECORDS.len(),
        "the first run must read every record, got: {first}"
    );
    gateway.stop().await;

    let restarted = start_bridged_gateway(&server, &[(TOPIC, 1)]).await;
    produce_with_kcat(restarted.addr, &[("k4", "delta")]);
    let second = consume_as_group(restarted.addr, 1);
    assert_eq!(
        records_read(&second),
        vec![format!("Offset:{}\tk4\tdelta", RECORDS.len())],
        "the group must resume after its commit on a gateway that never saw it, got: {second}"
    );
}

/// The id `kafka-cluster.sh cluster-id` prints, which it reads through `DescribeCluster`.
fn cluster_id(gateway: SocketAddr) -> String {
    let bootstrap = gateway.to_string();
    let output = run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-cluster.sh",
            "cluster-id",
            "--bootstrap-server",
            &bootstrap,
        ],
        &[],
    )
    .expect_ran("kafka-cluster.sh cluster-id");
    output
        .lines()
        .find_map(|line| line.strip_prefix("Cluster ID: "))
        .unwrap_or_else(|| panic!("no cluster id printed, got: {output}"))
        .trim()
        .to_string()
}

/// The Java tool prints the same id before and after a gateway restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_gateway_restart_when_kafka_cluster_prints_the_id_should_print_the_same_one() {
    let Some(server) = ready_server() else {
        return;
    };
    let gateway = start_bridged_gateway(&server, &[]).await;
    let before = cluster_id(gateway.addr);
    gateway.stop().await;

    let restarted = start_bridged_gateway(&server, &[]).await;
    let after = cluster_id(restarted.addr);
    assert_eq!(before, after, "the id must survive a restart");
    assert_ne!(before, "None", "a null id prints as None");
}

/// G11 of `docs/MANUAL_TESTING.md`. Two members split the topic. When one leaves, the other
/// resumes its partition at the offset it committed: an earlier offset reads records twice, a
/// later one skips them. Records are written only once both members hold a partition, so each
/// reads only its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_two_members_of_one_group_when_one_leaves_should_let_the_other_resume_its_partition()
{
    let Some(server) = ready_server() else {
        return;
    };
    let gateway = start_bridged_gateway(&server, &[(GROUP_TOPIC, GROUP_TOPIC_PARTITIONS)]).await;
    let round = i64::try_from(RECORDS.len()).expect("a handful of records");

    // One round on its own partition, then one on each.
    let mut survivor = group_member(gateway.addr, 3 * round);
    let alone = survivor.wait_for("the first member's assignment", assigned);
    assert_eq!(alone, [0, 1], "a lone member must hold every partition");

    let mut leaver = group_member(gateway.addr, round);
    let handed_over = leaver.wait_for("the second member's assignment", assigned_one);
    let kept = survivor.wait_for("the first member's new assignment", assigned_one);
    assert_ne!(
        kept, handed_over,
        "two members of one group must hold different partitions"
    );

    produce_round(gateway.addr);
    let leaver_output = leaver.wait_exit("the leaving member");
    assert_eq!(
        records_by_partition(&leaver_output),
        BTreeMap::from([(handed_over, (0..round).collect::<Vec<_>>())]),
        "the leaver must read its own partition and nothing else, got:\n{}",
        leaver_output.join("\n")
    );

    survivor.wait_for("the survivor taking every partition", |line| {
        assigned(line).filter(|partitions| *partitions == [0, 1])
    });
    produce_round(gateway.addr);
    let survivor_output = survivor.wait_exit("the surviving member");
    let read = records_by_partition(&survivor_output);
    assert_eq!(
        read.get(&handed_over),
        Some(&(round..2 * round).collect::<Vec<_>>()),
        "the survivor must resume partition {handed_over} at the leaver's commit, offset {round}, \
         got:\n{}",
        survivor_output.join("\n")
    );
    assert_eq!(
        read.get(&kept),
        Some(&(0..2 * round).collect::<Vec<_>>()),
        "the survivor must read all of partition {kept}, got:\n{}",
        survivor_output.join("\n")
    );
}

/// Starts `kafka-verifiable-consumer.sh` as a member of group `e2e-members` on `GROUP_TOPIC`. It
/// commits each batch synchronously after reading it, prints a JSON event for each step, and one
/// for each record with `--verbose`. It leaves the group after `count` records.
fn group_member(gateway: SocketAddr, count: i64) -> RunningClient {
    RunningClient::start(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-verifiable-consumer.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--topic",
            GROUP_TOPIC,
            "--group-id",
            "e2e-members",
            "--max-messages",
            &count.to_string(),
            "--verbose",
        ],
    )
}

/// Writes `RECORDS` to every partition of `GROUP_TOPIC`.
fn produce_round(gateway: SocketAddr) {
    for partition in 0..GROUP_TOPIC_PARTITIONS {
        produce_to_partition(gateway, GROUP_TOPIC, partition, &RECORDS);
    }
}

/// The JSON event a verifiable consumer printed on `line`, if it is named `name`.
fn event(line: &str, name: &str) -> Option<Value> {
    let parsed: Value = serde_json::from_str(line).ok()?;
    (parsed["name"] == name).then_some(parsed)
}

/// The partitions a `partitions_assigned` event hands out, sorted.
fn assigned(line: &str) -> Option<Vec<i64>> {
    let assignment = event(line, "partitions_assigned")?;
    let mut partitions: Vec<i64> = assignment["partitions"]
        .as_array()?
        .iter()
        .filter_map(|partition| partition["partition"].as_i64())
        .collect();
    partitions.sort_unstable();
    Some(partitions)
}

/// The partition a `partitions_assigned` event hands out, when it hands out exactly one.
fn assigned_one(line: &str) -> Option<i64> {
    match assigned(line)?.as_slice() {
        [partition] => Some(*partition),
        _ => None,
    }
}

/// The offset of every record a verifiable consumer printed, by partition, in the order read.
fn records_by_partition(output: &[String]) -> BTreeMap<i64, Vec<i64>> {
    let mut read: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for record in output.iter().filter_map(|line| event(line, "record_data")) {
        if let (Some(partition), Some(offset)) =
            (record["partition"].as_i64(), record["offset"].as_i64())
        {
            read.entry(partition).or_default().push(offset);
        }
    }
    read
}

/// Milliseconds since the epoch, the unit Kafka timestamps use.
fn now_millis() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past the epoch");
    i64::try_from(elapsed.as_millis()).expect("now fits an i64")
}

/// `millis` as the `YYYY-MM-DDTHH:mm:SS.sssZ` that `--to-datetime` reads. The zone is explicit,
/// so the client container's own zone does not matter.
fn utc_datetime(millis: i64) -> String {
    let micros = u64::try_from(millis).expect("a time after the epoch") * 1000;
    IggyTimestamp::from(micros).to_utc_string("%Y-%m-%dT%H:%M:%S%.3fZ")
}

/// Runs `command` under the same GNU `timeout` bound as [`run_client`].
fn output_bounded(command: &[&str], what: &str) -> std::process::Output {
    Command::new("timeout")
        .args(["--kill-after=10s", CLIENT_RUN_TIMEOUT])
        .args(command)
        .output()
        .unwrap_or_else(|error| panic!("run {what}: {error}"))
}

/// Compiles `tests/java/OffsetsForTimesProbe.java` against the `kafka-clients` jar of the client
/// image, into a directory the client container mounts. `None`, after reporting why, when there
/// is no JDK.
fn compile_offsets_for_times_probe() -> Option<tempfile::TempDir> {
    let javac = Command::new("javac")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !javac {
        skip("javac is unavailable, so the consumer offsetsForTimes probe cannot be built");
        return None;
    }
    let dir = tempfile::tempdir().expect("create probe dir");
    let jar = dir.path().join("kafka-clients.jar");
    let (_, kafka_version) = KAFKA_IMAGE
        .rsplit_once(':')
        .expect("the client image is pinned to a version tag");
    let image_jar = format!("/opt/kafka/libs/kafka-clients-{kafka_version}.jar");
    let extracted = output_bounded(
        &[
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--entrypoint",
            "cat",
            KAFKA_IMAGE,
            &image_jar,
        ],
        "kafka-clients extraction",
    );
    assert!(
        extracted.status.success(),
        "extracting {image_jar} failed: {}",
        String::from_utf8_lossy(&extracted.stderr)
    );
    std::fs::write(&jar, extracted.stdout).expect("write kafka-clients jar");
    let source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/OffsetsForTimesProbe.java");
    let (jar, out, source) = (
        jar.to_str().expect("jar path is utf-8"),
        dir.path().to_str().expect("probe dir is utf-8"),
        source.to_str().expect("probe source path is utf-8"),
    );
    let compiled = output_bounded(
        &["javac", "--release", "17", "-cp", jar, "-d", out, source],
        "javac",
    );
    assert!(
        compiled.status.success(),
        "javac failed: {}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    Some(dir)
}

/// #4516 acceptance: Java's lookups by time resolve against what the gateway stored. Three
/// records, then a time, then a fourth record a second later, so every lookup has one right
/// answer whichever clock the client and Iggy stamp with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_records_on_both_sides_of_a_time_when_java_looks_up_by_time_should_find_the_later_one()
 {
    let Some((_server, gateway)) = bridged_stack().await else {
        return;
    };
    produce_with_kcat(gateway, &RECORDS);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let between = now_millis();
    tokio::time::sleep(Duration::from_secs(1)).await;
    produce_with_kcat(gateway, &[("k4", "delta")]);

    let get_offsets = |time: &str| {
        run_client(
            KAFKA_IMAGE,
            &[
                "/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server",
                &gateway.to_string(),
                "--topic",
                TOPIC,
                "--time",
                time,
            ],
            &[],
        )
        .expect_ran(&format!("kafka-get-offsets --time {time}"))
    };
    let at = get_offsets(&between.to_string());
    assert!(
        at.lines().any(|line| line.trim() == format!("{TOPIC}:0:3")),
        "a lookup by time must find the record after the time, got: {at}"
    );
    let start = get_offsets("0");
    assert!(
        start
            .lines()
            .any(|line| line.trim() == format!("{TOPIC}:0:0")),
        "time 0 finds where the partition starts, as AKHQ asks, got: {start}"
    );
    let max = get_offsets("-3");
    assert!(
        max.lines()
            .any(|line| line.trim() == format!("{TOPIC}:0:3")),
        "max-timestamp names the last record, got: {max}"
    );
    let future = now_millis() + 3_600_000;
    let after = get_offsets(&future.to_string());
    assert!(
        !after
            .lines()
            .any(|line| line.starts_with(&format!("{TOPIC}:0:"))),
        "a time past every record has no offset, which the tool prints as nothing, got: {after}"
    );

    let reset = run_client(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            &gateway.to_string(),
            "--group",
            "e2e-reset-by-time",
            "--topic",
            TOPIC,
            "--reset-offsets",
            "--to-datetime",
            &utc_datetime(between),
            "--execute",
        ],
        &[],
    )
    .expect_ran("kafka-consumer-groups --reset-offsets --to-datetime");
    // The tool prints its header and rows without a line break between them, so the row is
    // matched as a run of fields anywhere in the output.
    let fields: Vec<&str> = reset.split_whitespace().collect();
    let moved = fields
        .windows(4)
        .any(|row| row == ["e2e-reset-by-time", TOPIC, "0", "3"]);
    assert!(
        moved,
        "the group must move to the first record after the time, got: {reset}"
    );

    // The consumer's own `offsetsForTimes`, which plain consumers and Kafka Streams seek by time
    // with, not the Admin call the tools above use. Same request on the wire, different client
    // code.
    let Some(probe) = compile_offsets_for_times_probe() else {
        return;
    };
    let probe_dir = probe.path().to_str().expect("probe path is utf-8");
    let consumer = run_client(
        KAFKA_IMAGE,
        &[
            "java",
            "-cp",
            "/opt/kafka/libs/*:/probe",
            "OffsetsForTimesProbe",
            &gateway.to_string(),
            TOPIC,
            "0",
            &between.to_string(),
            "0",
            &future.to_string(),
        ],
        &[(probe_dir, "/probe")],
    )
    .expect_ran("java consumer offsetsForTimes");
    for expected in [
        format!("time={between} offset=3"),
        "time=0 offset=0".to_string(),
        format!("time={future} offset=null"),
    ] {
        assert!(
            consumer.lines().any(|line| line.trim() == expected),
            "offsetsForTimes must answer `{expected}`, got: {consumer}"
        );
    }
}

/// The TLS-side stack: a bridged gateway with an `SSL` and a `SASL_SSL` listener, client
/// certificates requested and mapped to Iggy users by common name, and the material every
/// client needs on disk.
struct TlsStack {
    _server: TestServer,
    ssl: SocketAddr,
    sasl_ssl: SocketAddr,
    _certs_dir: tempfile::TempDir,
    ca_file: PathBuf,
    /// A certificate whose common name is `TLS_USER`, an Iggy user the stack creates.
    user_cert: Issued,
    /// A certificate the CA signed for a name no Iggy user has.
    stranger_cert: Issued,
}

/// The Iggy user a client certificate maps to. Created with a topic read so `DescribeAcls`
/// renders a binding that names it.
const TLS_USER: &str = "cert-alice";

/// Brings up the TLS stack, or reports why it could not.
async fn tls_stack() -> Option<TlsStack> {
    let server = ready_server()?;
    let token = root_token(&server.http_address);
    create_user(
        &server.http_address,
        &token,
        TLS_USER,
        r#"{"global":{"manage_servers":false,"read_servers":false,"manage_users":false,
           "read_users":false,"manage_streams":false,"read_streams":false,"manage_topics":false,
           "read_topics":true,"poll_messages":false,"send_messages":false},"streams":null}"#,
    );

    let certs_dir = tempfile::tempdir().expect("certs dir");
    let pki = TestPki::new();
    let (cert_file, key_file) = pki.server_cert("gateway").write(certs_dir.path(), "server");
    let ca_file = pki.write_ca(certs_dir.path());
    let user_cert = pki.client_cert(TLS_USER, "Iggy");
    let stranger_cert = pki.client_cert("nobody", "Iggy");

    let bridge = IggyBridge::connect(IggyBridgeConfig {
        address: server.address.clone(),
        username: ROOT_USER.to_string(),
        password: SecretString::from(ROOT_PASSWORD.to_string()),
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("the default mapping is valid"),
        max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        produce_sessions: ProduceSessionLimits::default(),
        tls: IggyTls::default(),
    })
    .await
    .expect("the bridge connects to a ready server");
    bridge
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("create the topic");
    let authenticator = Arc::new(IggyAuthenticator::new(server.address.clone()));
    let config = GatewayConfig {
        listeners: vec![
            ListenerConfig::new(SecurityProtocol::Ssl, "127.0.0.1:0"),
            ListenerConfig::new(SecurityProtocol::SaslSsl, "127.0.0.1:0"),
        ],
        tls: Some(
            TlsConfig::new(cert_file, key_file)
                .with_client_auth(ClientAuth::Requested, &ca_file)
                .with_principal_mapping("RULE:^CN=([^,]+).*$/$1/".parse().expect("valid rule")),
        ),
        ..GatewayConfig::default()
    };
    let (addrs, shutdown) = spawn_listeners(
        move |config| {
            KafkaGateway::new(config)
                .with_bridge(Some(Arc::new(bridge)))
                .with_authenticator(authenticator)
        },
        config,
    );
    // Held for the test's lifetime: dropping the sender shuts the gateway down mid-exchange.
    std::mem::forget(shutdown);
    Some(TlsStack {
        _server: server,
        ssl: addrs[0],
        sasl_ssl: addrs[1],
        _certs_dir: certs_dir,
        ca_file,
        user_cert,
        stranger_cert,
    })
}

impl TlsStack {
    /// kcat against `addr` with `extra` librdkafka properties, trusting the stack's CA.
    fn kcat_tls(&self, addr: SocketAddr, extra: &[&str], client: Option<&Issued>) -> ClientRun {
        let dir = tempfile::tempdir().expect("client certs dir");
        let mut args = vec![
            "-b".to_string(),
            addr.to_string(),
            "-X".to_string(),
            "ssl.ca.location=/tmp/certs/ca.pem".to_string(),
        ];
        let mut mounts = vec![(self.ca_file.display().to_string(), "/tmp/certs/ca.pem")];
        let (cert_file, key_file) = client.map_or_else(
            || (PathBuf::new(), PathBuf::new()),
            |issued| issued.write(dir.path(), "client"),
        );
        if client.is_some() {
            args.extend([
                "-X".to_string(),
                "ssl.certificate.location=/tmp/certs/client.pem".to_string(),
                "-X".to_string(),
                "ssl.key.location=/tmp/certs/client.key".to_string(),
            ]);
            mounts.push((cert_file.display().to_string(), "/tmp/certs/client.pem"));
            mounts.push((key_file.display().to_string(), "/tmp/certs/client.key"));
        }
        for property in extra {
            args.extend(["-X".to_string(), (*property).to_string()]);
        }
        args.push("-L".to_string());
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let mounts: Vec<(&str, &str)> = mounts
            .iter()
            .map(|(host, guest)| (host.as_str(), *guest))
            .collect();
        run_client(KCAT_IMAGE, &args, &mounts)
    }

    /// Writes a Java client config for TLS with `extra` properties, plus the CA and, when
    /// given, a PEM keystore. Returns the mounts the container needs.
    fn java_tls_config(
        &self,
        extra: &str,
        client: Option<&Issued>,
    ) -> (tempfile::TempDir, Vec<(String, &'static str)>) {
        let dir = tempfile::tempdir().expect("create config dir");
        let mut mounts = vec![(self.ca_file.display().to_string(), "/tmp/certs/ca.pem")];
        let mut properties = format!(
            "ssl.truststore.type=PEM\n\
             ssl.truststore.location=/tmp/certs/ca.pem\n\
             request.timeout.ms={JAVA_CLIENT_TIMEOUT_MS}\n\
             default.api.timeout.ms={JAVA_CLIENT_TIMEOUT_MS}\n\
             {extra}\n"
        );
        if let Some(issued) = client {
            let bundle = issued.write_bundle(dir.path(), "client");
            mounts.push((bundle.display().to_string(), "/tmp/certs/client.pem"));
            properties.push_str(
                "ssl.keystore.type=PEM\n\
                 ssl.keystore.location=/tmp/certs/client.pem\n",
            );
        }
        let path = dir.path().join("client.properties");
        std::fs::write(&path, properties).expect("write client config");
        mounts.push((path.display().to_string(), "/tmp/client.properties"));
        (dir, mounts)
    }

    fn java(
        &self,
        tool: &str,
        addr: SocketAddr,
        extra: &str,
        client: Option<&Issued>,
    ) -> ClientRun {
        let (_dir, mounts) = self.java_tls_config(extra, client);
        let mounts: Vec<(&str, &str)> = mounts
            .iter()
            .map(|(host, guest)| (host.as_str(), *guest))
            .collect();
        let bootstrap = addr.to_string();
        let mut args = vec![
            tool,
            "--bootstrap-server",
            &bootstrap,
            "--command-config",
            "/tmp/client.properties",
        ];
        if tool.ends_with("kafka-acls.sh") {
            args.push("--list");
        }
        run_client(KAFKA_IMAGE, &args, &mounts)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_an_ssl_listener_when_librdkafka_connects_with_ssl_should_serve_metadata() {
    let Some(stack) = tls_stack().await else {
        return;
    };
    let output = stack
        .kcat_tls(stack.ssl, &["security.protocol=SSL"], None)
        .expect_ran("kcat over SSL");
    assert!(
        output.contains("Metadata for all topics"),
        "librdkafka must handshake against the gateway certificate and be served, got: {output}"
    );
    assert!(
        output.contains(&format!("127.0.0.1:{}", stack.ssl.port())),
        "the broker advertised must be the SSL listener the client used, got: {output}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_sasl_ssl_listener_when_librdkafka_authenticates_should_serve_metadata() {
    let Some(stack) = tls_stack().await else {
        return;
    };
    let sasl = |password: &str| {
        stack.kcat_tls(
            stack.sasl_ssl,
            &[
                "security.protocol=SASL_SSL",
                "sasl.mechanisms=PLAIN",
                &format!("sasl.username={ROOT_USER}"),
                &format!("sasl.password={password}"),
            ],
            None,
        )
    };
    let output = sasl(ROOT_PASSWORD).expect_ran("kcat over SASL_SSL");
    assert!(
        output.contains("Metadata for all topics"),
        "librdkafka must authenticate inside TLS and be served, got: {output}"
    );
    assert!(
        output.contains(&format!("127.0.0.1:{}", stack.sasl_ssl.port())),
        "the broker advertised must be the SASL_SSL listener the client used, got: {output}"
    );

    // kcat exits non-zero here by design, so only its output is asserted.
    let rejected = sasl("definitely-not-the-password").text();
    assert!(
        rejected.contains("Authentication failed"),
        "a rejected credential must reach the client as an auth failure, got: {rejected}"
    );
    assert!(
        !rejected.contains("Metadata for all topics"),
        "nothing may be served to a rejected client"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_client_certificate_when_librdkafka_connects_should_be_admitted_as_its_user() {
    let Some(stack) = tls_stack().await else {
        return;
    };
    let output = stack
        .kcat_tls(
            stack.ssl,
            &["security.protocol=SSL"],
            Some(&stack.user_cert),
        )
        .expect_ran("kcat with a client certificate");
    assert!(
        output.contains("Metadata for all topics"),
        "a certificate naming an Iggy user must be admitted, got: {output}"
    );

    let stranger = stack.kcat_tls(
        stack.ssl,
        &["security.protocol=SSL"],
        Some(&stack.stranger_cert),
    );
    assert!(
        !stranger.text.contains("Metadata for all topics"),
        "a certificate naming no Iggy user must not be served, got: {}",
        stranger.text
    );
    assert!(
        !stranger.succeeded,
        "kcat must fail when the gateway closes on it, got: {}",
        stranger.text
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_the_java_client_with_ssl_and_sasl_ssl_should_connect_through_both_listeners() {
    let Some(stack) = tls_stack().await else {
        return;
    };
    let ssl = stack
        .java(
            "/opt/kafka/bin/kafka-broker-api-versions.sh",
            stack.ssl,
            "security.protocol=SSL",
            None,
        )
        .expect_ran("java client over SSL");
    assert!(
        ssl.contains(&format!("127.0.0.1:{}", stack.ssl.port())),
        "the Java client reads Metadata and then dials the advertised SSL listener, got: {ssl}"
    );
    assert!(
        ssl.contains("SaslHandshake(17): UNSUPPORTED"),
        "an SSL listener advertises no SASL keys, got: {ssl}"
    );

    let sasl_ssl = stack
        .java(
            "/opt/kafka/bin/kafka-broker-api-versions.sh",
            stack.sasl_ssl,
            &format!(
                "security.protocol=SASL_SSL\n\
                 sasl.mechanism=PLAIN\n\
                 sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
                 required username=\"{ROOT_USER}\" password=\"{ROOT_PASSWORD}\";"
            ),
            None,
        )
        .expect_ran("java client over SASL_SSL");
    assert!(
        sasl_ssl.contains("SaslHandshake(17): 0 to 1")
            && sasl_ssl.contains("SaslAuthenticate(36): 0 to 2"),
        "the Java client must authenticate inside TLS and read the advertisement, got: {sasl_ssl}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_the_java_client_with_a_client_certificate_should_be_described_as_its_user() {
    let Some(stack) = tls_stack().await else {
        return;
    };
    let listing = stack
        .java(
            "/opt/kafka/bin/kafka-acls.sh",
            stack.ssl,
            "security.protocol=SSL",
            Some(&stack.user_cert),
        )
        .expect_ran("java kafka-acls over mTLS");
    assert!(
        listing.contains(&format!("principal=User:{TLS_USER}")),
        "the certificate's common name is the principal described, got: {listing}"
    );
    assert!(
        grants(&listing, "TOPIC", "READ"),
        "the mapped user's read_topics grant renders, got: {listing}"
    );
}
