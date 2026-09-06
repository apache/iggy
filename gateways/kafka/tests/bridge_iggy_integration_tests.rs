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

//! Integration tests for `IggyBridge` against a real `iggy-server` process - not the
//! `KafkaGateway` under test elsewhere in this suite. `#3533` acceptance criteria this file
//! exercises directly: `ensure_stream_and_topic` idempotent on repeated calls, and the bridge
//! module invoked from a real (non-unit) test rather than only compiled.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::time::Duration;

use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClient, IggyClientBuilder, IggyMessage,
    MessageClient, Partitioning,
};
use secrecy::SecretString;
use serial_test::serial;

use iggy_gateway_kafka::bridge::{BridgeError, IggyBridge, IggyBridgeConfig, TopicMapping};

/// First port of the local reservation band. `u16` has no room above the highest ephemeral
/// ceiling in use (macOS: 49152-65535), so "clear of ephemeral" only leaves room below the
/// lowest floor (Linux default: 32768) - picked here below `core/integration`'s own
/// `port_reserver.rs` band too (20000 up to that floor), so a `bind(0)` call in an unrelated
/// process, including that harness's own test servers, is never handed a port this band claims.
const PORT_LOCK_BAND_START: u16 = 15000;
const PORT_LOCK_BAND_SLOTS: u16 = 200;

/// Exclusive claim on one port, released (and the port freed for reuse) when dropped - including
/// on an unclean process exit, since the OS drops the `flock` with the file descriptor. Unlike
/// bind-then-drop, nothing ever binds the port on this side, so there is no TOCTOU window between
/// picking it and `iggy-server` binding it: the lock, not a socket, is the allocator.
struct PortGuard {
    port: u16,
    _lock: File,
}

impl PortGuard {
    fn acquire() -> Self {
        let lock_dir = std::env::temp_dir().join("iggy-kafka-gateway-test-port-locks");
        std::fs::create_dir_all(&lock_dir).expect("create port lock dir");
        for offset in 0..PORT_LOCK_BAND_SLOTS {
            let port = PORT_LOCK_BAND_START + offset;
            let path = lock_dir.join(format!("{port}.lock"));
            let Ok(file) = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
            else {
                continue;
            };
            if file.try_lock().is_ok() {
                return Self { port, _lock: file };
            }
        }
        panic!(
            "no free port slot in [{PORT_LOCK_BAND_START}, {}]",
            PORT_LOCK_BAND_START + PORT_LOCK_BAND_SLOTS - 1
        );
    }
}

/// Builds `iggy-server` (idempotent - a no-op rebuild once already current) and returns its path.
///
/// Not `assert_cmd::Command::cargo_bin`: that only resolves `CARGO_BIN_EXE_*` for binaries owned
/// by *this* package (confirmed - it fails here with "available binary names are
/// iggy-gateway-kafka"). `iggy-server` belongs to the separate `server` crate, and neither this
/// crate nor `core/integration` (same `Command::cargo_bin` pattern) declares that crate as a
/// dependency just to make its binary buildable. Driving `cargo build` directly sidesteps that
/// entirely - no Cargo.toml dependency edge needed on a crate this one otherwise never touches.
///
/// Reads the artifact path from `--message-format=json` rather than guessing
/// `target/debug/iggy-server`: a guessed path breaks under `CARGO_TARGET_DIR` (this workspace's
/// own coverage CI sets it), `--release`, or a `--target` triple subdirectory, none of which
/// `cargo build`'s own JSON output leaves to guesswork.
fn iggy_server_binary() -> &'static PathBuf {
    static BINARY_PATH: OnceLock<PathBuf> = OnceLock::new();
    BINARY_PATH.get_or_init(|| {
        let output = Command::new(env!("CARGO"))
            .args([
                "build",
                "--package",
                "server",
                "--bin",
                "iggy-server",
                "--message-format=json",
            ])
            .output()
            .expect("run cargo build for iggy-server");
        assert!(
            output.status.success(),
            "cargo build --package server --bin iggy-server failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|message| {
                if message.get("reason")? == "compiler-artifact"
                    && message.get("target")?.get("name")? == "iggy-server"
                {
                    message.get("executable")?.as_str().map(PathBuf::from)
                } else {
                    None
                }
            })
            .expect("cargo build --message-format=json reported no iggy-server executable")
    })
}

struct TestServer {
    child: Child,
    address: String,
    _port_guard: PortGuard,
}

impl TestServer {
    /// Spawns `iggy-server` with an isolated temp data dir and a locked TCP port, then blocks
    /// until a bridge connection succeeds or the startup budget is exhausted.
    async fn spawn(data_dir: &std::path::Path) -> Self {
        let port_guard = PortGuard::acquire();
        let address = format!("127.0.0.1:{}", port_guard.port);

        let mut command = Command::new(iggy_server_binary());
        command
            .env("IGGY_SYSTEM_PATH", data_dir.display().to_string())
            .env("IGGY_TCP_ADDRESS", &address)
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_QUIC_ENABLED", "false")
            // `--with-default-root-credentials` is off by default (args.rs) - without these,
            // a fresh server provisions no loginable root user at all, and every bridge connect
            // attempt fails with "invalid credentials" no matter what this test passes.
            .env("IGGY_ROOT_USERNAME", "iggy")
            .env("IGGY_ROOT_PASSWORD", "iggy");
        let child = command.spawn().expect("spawn iggy-server");

        let server = Self {
            child,
            address,
            _port_guard: port_guard,
        };
        server.wait_ready().await;
        server
    }

    /// Polls with a bare TCP connect, not a full `IggyBridge::connect`: the latter carries
    /// `RECONNECTION_RETRIES` (3 dials at ~1s apart) on every failed attempt, so a poll loop built
    /// on it pays several real seconds per iteration instead of running at its own 100ms cadence,
    /// and a *successful* poll iteration would authenticate a client and then drop it without
    /// `close()`, leaking a session server-side. A TCP accept slightly ahead of the app being
    /// ready to authenticate is fine - every caller's own subsequent `IggyBridge::connect` already
    /// retries a few times, which covers the last few hundred ms of that gap.
    async fn wait_ready(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(self.address.as_str())
                .await
                .is_ok()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "iggy-server at {} did not become ready within the startup budget",
                self.address
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn test_config(&self) -> IggyBridgeConfig {
        IggyBridgeConfig {
            address: self.address.clone(),
            username: "iggy".to_string(),
            password: SecretString::from("iggy"),
            topic_mapping: TopicMapping {
                default_stream: "kafka".to_string(),
                topics: HashMap::new(),
            },
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Builds and connects a raw `IggyClient` against `server` - for producing test data directly,
/// independent of the `IggyBridge` under test. Uses the fluent builder, not a hand-built
/// `iggy://user:pass@host` string: `ConnectionString` splits on `@` then `:`, which breaks for
/// any password containing either character.
async fn raw_client(server: &TestServer) -> IggyClient {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(server.address.clone())
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            "iggy".to_string(),
            SecretString::from("iggy"),
        )))
        .build()
        .expect("build raw test client");
    client.connect().await.expect("connect raw test client");
    client
}

#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_is_idempotent_on_repeated_calls() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("first call creates the stream and topic");
    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("second call is a no-op against the now-existing stream and topic");
    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("third call is still a no-op");
}

#[tokio::test]
#[serial]
async fn high_watermark_is_zero_for_a_fresh_empty_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking the watermark");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("fresh topic must report a watermark, not an error");
    assert_eq!(
        watermark, 0,
        "a freshly created, empty partition's high watermark must be 0"
    );
}

/// Pins the exact semantics of `Iggy::Partition::current_offset` (offset of the *last written*
/// message, not Kafka's "next offset to produce") against a real server - a test that only
/// checked the empty-topic case would pass under either interpretation and hide an off-by-one.
#[tokio::test]
#[serial]
async fn high_watermark_reflects_produced_messages() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before producing");

    let stream_id = Identifier::named("kafka").expect("valid stream name");
    let topic_id = Identifier::named("orders").expect("valid topic name");
    let mut messages: Vec<IggyMessage> = (0..3)
        .map(|i| IggyMessage::from(format!("message-{i}")))
        .collect();
    let client = raw_client(&server).await;
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send 3 messages");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("topic must report a watermark after producing");
    assert_eq!(
        watermark, 3,
        "high watermark after 3 messages (offsets 0, 1, 2) must be 3, not the last offset (2)"
    );
}

#[tokio::test]
#[serial]
async fn high_watermark_rejects_out_of_range_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking the watermark");

    let err = bridge
        .high_watermark("orders", 5)
        .await
        .expect_err("partition 5 does not exist on a 1-partition topic");
    assert!(matches!(err, BridgeError::PartitionOutOfRange { .. }));
}

#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_is_idempotent_for_a_numeric_topic_name() {
    // Regression test: Identifier::try_from/FromStr parses an all-digit string as a numeric ID,
    // not a name - a second call for the same numeric-named topic would look it up by the wrong
    // resource kind and fail with StreamIdNotFound/TopicIdNotFound despite the topic existing.
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("2024", 1)
        .await
        .expect("first call creates the numeric-named stream and topic");
    bridge
        .ensure_stream_and_topic("2024", 1)
        .await
        .expect("second call must still find the numeric-named topic by name, not by ID");
}

/// Finding: the SDK's connection-string parser splits on `@` then `:`, so a password containing
/// either character breaks unless credentials are passed as already-separated fields.
#[tokio::test]
#[serial]
async fn connect_succeeds_with_password_containing_special_characters() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let port_guard = PortGuard::acquire();
    let address = format!("127.0.0.1:{}", port_guard.port);
    let password = "p@ss:word";

    let mut command = Command::new(iggy_server_binary());
    command
        .env("IGGY_SYSTEM_PATH", data_dir.path().display().to_string())
        .env("IGGY_TCP_ADDRESS", &address)
        .env("IGGY_HTTP_ENABLED", "false")
        .env("IGGY_QUIC_ENABLED", "false")
        .env("IGGY_ROOT_USERNAME", "iggy")
        .env("IGGY_ROOT_PASSWORD", password);
    let mut child = command.spawn().expect("spawn iggy-server");

    let config = IggyBridgeConfig {
        address,
        username: "iggy".to_string(),
        password: SecretString::from(password),
        topic_mapping: TopicMapping {
            default_stream: "kafka".to_string(),
            topics: HashMap::new(),
        },
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let result = loop {
        match IggyBridge::connect(config.clone()).await {
            Ok(bridge) => break Ok(bridge),
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    break Err(err);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let _ = child.kill();
    let _ = child.wait();

    result.expect("bridge must connect with a password containing '@' and ':'");
}

/// Acceptance criterion: "no panics on Iggy unreachable at handler boundary." Connects to a port
/// nothing is listening on and asserts a plain `Err`, not a panic - the strongest way to fail this
/// assertion is exactly the failure mode being guarded against.
#[tokio::test]
async fn connect_to_unreachable_iggy_returns_err_not_panic() {
    let port_guard = PortGuard::acquire(); // locked but never bound - nothing listens on it
    let config = IggyBridgeConfig {
        address: format!("127.0.0.1:{}", port_guard.port),
        username: "iggy".to_string(),
        password: SecretString::from("iggy"),
        topic_mapping: TopicMapping {
            default_stream: "kafka".to_string(),
            topics: HashMap::new(),
        },
    };

    let result = IggyBridge::connect(config).await;
    assert!(matches!(result, Err(BridgeError::Iggy(_))));
}
