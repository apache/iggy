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
use std::net::TcpListener;
use std::path::{Path, PathBuf};
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

/// Picks a free TCP port by binding to `127.0.0.1:0` and immediately releasing it. Small TOCTOU
/// window between release and `iggy-server` binding the same port - acceptable for a test helper,
/// same tradeoff `core/integration`'s own `port_reserver.rs` makes.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Builds `iggy-server` (idempotent - a no-op rebuild once already current) and returns its path.
///
/// Not `assert_cmd::Command::cargo_bin`: that only resolves `CARGO_BIN_EXE_*` for binaries owned
/// by *this* package (confirmed - it fails here with "available binary names are
/// iggy-gateway-kafka"). `iggy-server` belongs to the separate `server` crate, and neither this
/// crate nor `core/integration` (same `Command::cargo_bin` pattern) declares that crate as a
/// dependency just to make its binary buildable. Driving `cargo build` directly sidesteps that
/// entirely - no Cargo.toml dependency edge needed on a crate this one otherwise never touches.
fn iggy_server_binary() -> &'static PathBuf {
    static BINARY_PATH: OnceLock<PathBuf> = OnceLock::new();
    BINARY_PATH.get_or_init(|| {
        let status = Command::new(env!("CARGO"))
            .args(["build", "--package", "server", "--bin", "iggy-server"])
            .status()
            .expect("run cargo build for iggy-server");
        assert!(
            status.success(),
            "cargo build --package server --bin iggy-server failed"
        );

        // CARGO_MANIFEST_DIR is gateways/kafka; the workspace root (and its target/ dir) is two
        // levels up.
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("gateways/kafka is two levels under the workspace root");
        workspace_root.join("target/debug/iggy-server")
    })
}

struct TestServer {
    child: Child,
    address: String,
}

impl TestServer {
    /// Spawns `iggy-server` with an isolated temp data dir and an ephemeral TCP port, then blocks
    /// until a bridge connection succeeds or the startup budget is exhausted.
    async fn spawn(data_dir: &std::path::Path) -> Self {
        let port = free_port();
        let address = format!("127.0.0.1:{port}");

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

        let server = Self { child, address };
        server.wait_ready().await;
        server
    }

    /// Retries a full bridge connect (not just a TCP connect) so the wait covers the server
    /// actually being ready to authenticate, not just its listener socket being open.
    async fn wait_ready(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if IggyBridge::connect(self.test_config()).await.is_ok() {
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
/// independent of the `IggyBridge` under test. Mirrors `IggyBridge::connect`'s fluent-builder
/// approach (not a hand-built connection string) purely so this test helper doesn't reintroduce
/// the credential-escaping bug finding #4 fixes.
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
    let port = free_port();
    let address = format!("127.0.0.1:{port}");
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
    let port = free_port(); // reserved, then immediately released, nothing binds it
    let config = IggyBridgeConfig {
        address: format!("127.0.0.1:{port}"),
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
