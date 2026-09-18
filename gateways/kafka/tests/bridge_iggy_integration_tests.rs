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
    MessageClient, Partitioning, StreamClient, TopicClient,
};
use secrecy::SecretString;
use serial_test::serial;

use iggy_gateway_kafka::bridge::{
    BridgeError, IggyBridge, IggyBridgeConfig, TopicMapping, TopicOverride,
};

/// Desired slot count - the actual count `port_band()` computes may be smaller if the machine's
/// real ephemeral range leaves little room, but this crate never needs more than a handful live
/// at once (this binary's own tests already run one at a time - see `.config/nextest.toml`'s
/// `kafka_bridge` test-group).
const DESIRED_SLOTS: u16 = 200;

/// Lowest port `port_band()` will ever place its band at - clear of the well-known/privileged
/// range (0-1023).
const MIN_CANDIDATE_PORT: u16 = 10000;

/// Ephemeral range assumed when the kernel's own can't be read (no `/proc` - e.g. macOS),
/// matching the Linux default. Mirrors `core/integration`'s own `port_reserver.rs`.
const DEFAULT_EPHEMERAL_RANGE: (u16, u16) = (32768, 60999);

const IP_LOCAL_PORT_RANGE: &str = "/proc/sys/net/ipv4/ip_local_port_range";

fn parse_ephemeral_range(range: &str) -> Option<(u16, u16)> {
    let mut bounds = range.split_whitespace();
    let floor: u16 = bounds.next()?.parse().ok()?;
    let ceiling: u16 = bounds.next()?.parse().ok()?;
    Some((floor, ceiling))
}

/// The range the kernel picks `bind(0)` ports from, read once.
fn ephemeral_range() -> (u16, u16) {
    static RANGE: OnceLock<(u16, u16)> = OnceLock::new();
    *RANGE.get_or_init(|| {
        std::fs::read_to_string(IP_LOCAL_PORT_RANGE)
            .ok()
            .as_deref()
            .and_then(parse_ephemeral_range)
            .unwrap_or(DEFAULT_EPHEMERAL_RANGE)
    })
}

/// First port and slot count of a band clear of the kernel's ephemeral range: below the floor by
/// preference (matches `core/integration`'s own `port_reserver.rs`), above the ceiling when the
/// floor leaves no room there.
///
/// A *hardcoded* band, chosen once without reading the real range (this file's own prior version:
/// `15000..15199`, on the unverified assumption that every real deployment's floor sits above
/// it), is wrong on any box tuned wider - `net.ipv4.ip_local_port_range = "1024 65535"` swallows
/// that whole band. `flock` alone doesn't save it either: it is advisory, so it only excludes
/// another `PortGuard`-based process, never an unrelated `bind(0)` elsewhere on the box landing on
/// the same number from the kernel's own ephemeral pool.
///
/// Known residual gap, not fixed by the "matches `port_reserver.rs`" preference above: this
/// `PortGuard` locks its own directory (`iggy-kafka-gateway-test-port-locks`), disjoint from
/// `port_reserver.rs`'s (`iggy-test-port-locks`), so the two share no lock state even when their
/// *bands* land on the same numbers. That only happens when the ephemeral floor leaves neither
/// allocator room below it - `port_reserver.rs` needs room below its own, higher `BAND_START`
/// (20000) to use the below-floor branch at all, while this function only needs room below
/// `MIN_CANDIDATE_PORT` (10000) - so a floor at or under 10000 pushes *both* to their
/// above-the-ceiling fallback, anchored at the identical `ceiling + 1`: this band takes the first
/// [`DESIRED_SLOTS`] of it, `port_reserver.rs` takes the entire rest up to `u16::MAX`. A port
/// this crate's tests reserve there is invisible to `port_reserver.rs`'s own lock, and vice versa,
/// so two suites in the same workspace could hand out the same port under two different locks.
/// `core/integration/src/harness/mod.rs`'s `mod port_reserver;` is private, so sharing its lock
/// directory (the complete fix) needs a visibility change there first; narrow enough (an
/// ephemeral floor this low is uncommon - the Linux default is 32768) that this is disclosed
/// rather than worked around here.
fn port_band() -> (u16, u16) {
    static BAND: OnceLock<(u16, u16)> = OnceLock::new();
    *BAND.get_or_init(|| {
        let (floor, ceiling) = ephemeral_range();
        band_for(floor, ceiling)
    })
}

/// Pure band-selection logic, split out from [`port_band`] so it's testable against synthetic
/// ranges without needing to fake `/proc` contents.
fn band_for(floor: u16, ceiling: u16) -> (u16, u16) {
    if floor > MIN_CANDIDATE_PORT {
        let room = floor - MIN_CANDIDATE_PORT;
        return (MIN_CANDIDATE_PORT, room.min(DESIRED_SLOTS));
    }
    let Some(start) = ceiling.checked_add(1) else {
        panic!(
            "kernel ephemeral range [{floor}, {ceiling}] leaves no room for a test port band \
             below {MIN_CANDIDATE_PORT} nor above {ceiling} - narrow the range, e.g. \
             sysctl -w net.ipv4.ip_local_port_range='32768 60999'"
        );
    };
    // start >= 1 (ceiling + 1), so this never overflows u16.
    let room = u16::MAX - start + 1;
    (start, room.min(DESIRED_SLOTS))
}

#[test]
fn given_a_readable_range_when_parsed_should_take_both_bounds() {
    assert_eq!(
        parse_ephemeral_range("32768\t60999\n"),
        Some((32768, 60999))
    );
    assert_eq!(parse_ephemeral_range("1024 65535"), Some((1024, 65535)));
    assert_eq!(parse_ephemeral_range(""), None);
    assert_eq!(parse_ephemeral_range("32768"), None);
    assert_eq!(parse_ephemeral_range("garbage 60999"), None);
}

#[test]
fn given_room_below_the_floor_when_choosing_a_band_should_take_it() {
    let (start, slots) = band_for(32768, 60999);
    assert_eq!(start, MIN_CANDIDATE_PORT);
    assert!(
        start + slots <= 32768,
        "band [{start}, {}] runs into the ephemeral floor",
        start + slots - 1
    );
}

/// A box tuned `net.ipv4.ip_local_port_range = "1024 60999"` is exactly the case the prior
/// hardcoded `15000..15199` band silently broke under: room below the floor down to
/// `MIN_CANDIDATE_PORT` is gone, but the ceiling still leaves room above it.
#[test]
fn given_no_room_below_the_floor_when_choosing_a_band_should_fall_back_above_the_ceiling() {
    let (start, slots) = band_for(1024, 60999);
    assert!(slots > 0, "must find room above the ceiling, not just fail");
    assert!(
        start > 60999,
        "band must start above the ephemeral ceiling, got {start}"
    );
}

#[test]
#[should_panic(expected = "leaves no room")]
fn given_no_room_on_either_side_when_choosing_a_band_should_fail_loudly() {
    band_for(1024, u16::MAX);
}

/// Exclusive claim on one port, released (and the port freed for reuse) when dropped - including
/// on an unclean process exit, since the OS drops the `flock` with the file descriptor. Unlike
/// bind-then-drop, this process itself never binds the port before `iggy-server` does; see
/// `port_band`'s doc comment for the limits of that guarantee against *other* processes.
struct PortGuard {
    port: u16,
    _lock: File,
}

impl PortGuard {
    fn acquire() -> Self {
        let (band_start, band_slots) = port_band();
        let lock_dir = std::env::temp_dir().join("iggy-kafka-gateway-test-port-locks");
        std::fs::create_dir_all(&lock_dir).expect("create port lock dir");
        for offset in 0..band_slots {
            let port = band_start + offset;
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
            "no free port slot in [{band_start}, {}]",
            band_start + band_slots - 1
        );
    }
}

/// Locates the already-built `iggy-server` binary. Does not build it - see this crate's
/// `docs/TEST_SUITE.md` for the prerequisite, the same one `core/integration`'s own
/// server-spawning tests already carry.
///
/// Walks up from this test binary's own path (`env::current_exe()`) rather than calling
/// `assert_cmd::cargo::cargo_bin("iggy-server")` directly: `cargo_bin`'s `CARGO_BIN_EXE_*`
/// env-var lookup only resolves for binaries owned by *this* package, so for `iggy-server`
/// (a different package) it always falls through to `cargo_bin`'s own `legacy_cargo_bin`, which
/// panics with its own internal message - not the "binary not found" text `TEST_SUITE.md`
/// promises - when the binary is not yet built (`CARGO_BIN_EXE_iggy-server` is unset, so its
/// panic path prints that plus every binary name it *did* find, not a helpful next step). Ancestor
/// directories of `current_exe()` reach the same `target/<profile>/` directory
/// `legacy_cargo_bin` derives - correct under `CARGO_TARGET_DIR`, `--release`, or a `--target`
/// triple subdirectory for the identical reason: it reads back from where cargo actually placed
/// *this* binary, not a guessed path - so checking existence there first, before ever calling
/// into `assert_cmd`, gives this crate's own message instead of a Cargo internal one. An earlier
/// version of this function drove `cargo build --package server --bin iggy-server` directly from
/// every server-spawning test (nextest runs each as its own process, so the `OnceLock` memoized
/// nothing across them), serialized by `.config/nextest.toml`'s `kafka_bridge` test-group but
/// still real, avoidable per-test overhead this version has none of.
///
/// # Panics
///
/// Panics if `iggy-server` cannot be found alongside this test binary's own target directory.
fn iggy_server_binary() -> PathBuf {
    let current_exe = std::env::current_exe().expect("resolve this test binary's own path");
    let binary_name = format!("iggy-server{}", std::env::consts::EXE_SUFFIX);
    current_exe
        .ancestors()
        .map(|dir| dir.join(&binary_name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| {
            panic!(
                "iggy-server binary not found near {} - build it first with \
                 `cargo build --package server --bin iggy-server`",
                current_exe.display()
            )
        })
}

struct TestServer {
    child: Child,
    address: String,
    password: String,
    _port_guard: PortGuard,
}

impl TestServer {
    /// Spawns `iggy-server` with the default `iggy`/`iggy` root credentials. See
    /// [`Self::spawn_with_password`] for the general form.
    async fn spawn(data_dir: &std::path::Path) -> Self {
        Self::spawn_with_password(data_dir, "iggy").await
    }

    /// Spawns `iggy-server` with an isolated temp data dir, a locked TCP port, and the given root
    /// password, then blocks until its listener is ready or the startup budget is exhausted.
    ///
    /// A dedicated password parameter (not just the `spawn()` default everywhere) lets a
    /// password-shaped regression test (special characters, say) reuse this harness's
    /// `PortGuard`/graceful-`Drop`/`wait_ready` machinery instead of hand-rolling a second,
    /// `Drop`-less spawn: a `Drop`-less copy has no guard to run `graceful_kill` on a panic before
    /// its assertions, orphaning a process that still holds its `PortGuard` slot, which then
    /// fails the next test that draws that slot to bind.
    async fn spawn_with_password(data_dir: &std::path::Path, password: &str) -> Self {
        let port_guard = PortGuard::acquire();
        let address = format!("127.0.0.1:{}", port_guard.port);

        let mut command = Command::new(iggy_server_binary());
        command
            .env("IGGY_PATH", data_dir.display().to_string())
            .env("IGGY_TCP_ADDRESS", &address)
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_QUIC_ENABLED", "false")
            // WebSocket defaults to enabled on a fixed 127.0.0.1:8092 (config.toml), unlike TCP
            // which reads a per-test port from PortGuard - every spawned server here would fight
            // over that one port otherwise, and a bind failure aborts boot.
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            // Matches core/integration's own spawned-server harness (harness/handle/server.rs):
            // pinned shards (config.toml default: pin_cores = true) of concurrently running
            // servers pile onto the same cores and starve each other. This crate's own tests are
            // serialized against each other (.config/nextest.toml's kafka_bridge test-group -
            // #[serial] alone does not survive nextest, which runs each test as its own process),
            // but a spawned server here can still land alongside a pinned server from a different
            // package's test in the same nextest run.
            .env("IGGY_SHARDING_PIN_CORES", "false")
            // Only half of core/integration's own pairing: unpinning cores stops a server from
            // being pinned to a specific range, but config.toml's own default
            // (cpu_allocation = "numa:auto") still sizes the shard pool to the whole machine with
            // nothing set here - every server this crate's tests spawn would still compete for
            // the entire box against unrelated packages' tests in the same nextest run. A small
            // fixed range (matching the same harness's own fallback for an unreadable core count)
            // caps that instead.
            .env("IGGY_SHARDING_CPU_ALLOCATION", "0..4")
            // `--with-default-root-credentials` is off by default (args.rs) - without these,
            // a fresh server provisions no loginable root user at all, and every bridge connect
            // attempt fails with "invalid credentials" no matter what this test passes.
            .env("IGGY_ROOT_USERNAME", "iggy")
            .env("IGGY_ROOT_PASSWORD", password);
        let child = command.spawn().expect("spawn iggy-server");

        let mut server = Self {
            child,
            address,
            password: password.to_string(),
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
    ///
    /// Checks `try_wait()` every iteration: a server that fails at boot (bad config, a port
    /// stolen between `PortGuard::acquire` and its own bind) exits almost immediately, and without
    /// this check that reads as "still starting" for the full 30s budget - the eventual failure
    /// names the wrong cause ("did not become ready") instead of the real one (exited early, with
    /// whatever it printed before dying).
    async fn wait_ready(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child status") {
                panic!(
                    "iggy-server at {} exited during startup with {status}",
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
            password: SecretString::from(self.password.clone()),
            topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
                .expect("valid mapping for this test's fixture data"),
        }
    }
}

/// SIGTERM, wait up to `SIGTERM_TIMEOUT`, then SIGKILL if it hasn't exited. Mirrors
/// `core/integration`'s own `harness::handle::common::graceful_kill` (not reused directly - that
/// crate is heavyweight, and pulling it in for one function isn't worth it; this crate already
/// depends directly on `iggy`/`core/sdk`, so `core/integration` wouldn't add a *new*
/// core/sdk-change-reruns-these-tests edge, just an unrelated dependency). A bare SIGKILL skips
/// `iggy-server`'s shutdown path entirely, which is a materially different exit than what the
/// binary is actually built to do on `SIGTERM` - `.kill()` alone bypassed that in every test run
/// before this fix.
const SIGTERM_TIMEOUT: Duration = Duration::from_secs(5);
const SIGKILL_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn graceful_kill(child: &mut Child) {
    // `wait_ready` calls `try_wait()` too (and panics on an early exit, unwinding into this via
    // `Drop`) - if it already reaped the child, `child.id()` is a PID the OS is free to hand to an
    // unrelated process by the time we get here, and a raw `libc::kill` (unlike `std::Child::kill`,
    // which checks its own cached exit status first and no-ops instead) has no such guard against
    // signaling that PID anyway. Checking here first closes the same gap `std` already closes for
    // its own `kill`.
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }

    let pid = child.id() as libc::pid_t;
    // Safety: `pid` is this process's own live child, confirmed by the `try_wait` check above;
    // sending it a signal is exactly what `Child::kill` itself does internally.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }

    let deadline = std::time::Instant::now() + SIGTERM_TIMEOUT;
    while std::time::Instant::now() < deadline {
        match child.try_wait() {
            Ok(None) => std::thread::sleep(SIGKILL_POLL_INTERVAL),
            Ok(Some(_)) | Err(_) => return,
        }
    }

    let _ = child.kill();
}

impl Drop for TestServer {
    fn drop(&mut self) {
        graceful_kill(&mut self.child);
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

    // Read back real state, not just three Oks: proves the second and third calls were actually
    // no-ops against the one topic the first call created, not e.g. three independent topics
    // that all happen to satisfy Ok(()) individually.
    let raw = raw_client(&server).await;
    let topics = raw
        .get_topics(&Identifier::named("kafka").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(topics.len(), 1, "must be exactly one topic, not three");
    assert_eq!(topics[0].name, "orders");
    assert_eq!(topics[0].partitions_count, 3);
}

/// Regression test for coverage: nothing previously exercised `PartitionCountMismatch` on a real
/// server - only unit tests constructed the variant directly.
#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_rejects_a_second_call_with_a_different_partition_count() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("first call creates the topic with 3 partitions");

    let err = bridge
        .ensure_stream_and_topic("orders", 5)
        .await
        .expect_err(
            "a different partition count against an existing topic must not silently succeed",
        );
    match err {
        BridgeError::PartitionCountMismatch {
            topic,
            existing,
            requested,
        } => {
            assert_eq!(topic, "orders", "must report the Kafka-side name");
            assert_eq!(existing, 3);
            assert_eq!(requested, 5);
        }
        other => panic!("expected PartitionCountMismatch, got {other:?}"),
    }
}

/// Regression test for coverage: every other test uses an empty `TopicMapping` (`HashMap::new()`),
/// so the Kafka-name-vs-Iggy-name distinction `ensure_topic`/`high_watermark` deliberately
/// maintain in their error paths (`kafka_topic`, not the resolved `topic_name`) can never actually
/// differ and so can never be caught wrong. This test maps a Kafka topic to a differently-named
/// Iggy stream/topic and checks both the happy path and the error paths report the Kafka-side
/// name a real caller (a future handler) would recognize.
#[tokio::test]
#[serial]
async fn bridge_operations_report_the_kafka_side_name_through_a_real_topic_mapping_override() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut topics = HashMap::new();
    topics.insert(
        "orders".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "orders_v2".to_string(),
        },
    );
    config.topic_mapping = TopicMapping::new("kafka".to_string(), topics)
        .expect("valid mapping for this test's fixture data");
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("must create the mapped Iggy stream/topic, not one named after the Kafka topic");

    // The Iggy-side resources are named differently from the Kafka topic - confirms the mapping
    // actually took effect, not that resolve() was a no-op that happened to pass either way.
    let raw = raw_client(&server).await;
    let billing_topics = raw
        .get_topics(&Identifier::named("billing").expect("valid stream name"))
        .await
        .expect("get_topics call");
    assert_eq!(billing_topics.len(), 1);
    assert_eq!(billing_topics[0].name, "orders_v2");

    let watermark = bridge
        .high_watermark("orders", 0)
        .await
        .expect("must resolve through the mapping, not fail looking for a stream named 'orders'");
    assert_eq!(watermark, 0);

    let out_of_range = bridge
        .high_watermark("orders", 5)
        .await
        .expect_err("partition 5 does not exist on a 1-partition topic");
    assert!(
        matches!(
            &out_of_range,
            BridgeError::PartitionOutOfRange { topic, .. } if topic == "orders"
        ),
        "error must quote the Kafka topic name 'orders', not the Iggy name 'orders_v2': \
         {out_of_range:?}"
    );

    let mismatch = bridge
        .ensure_stream_and_topic("orders", 2)
        .await
        .expect_err("different partition count against the mapped topic must not silently succeed");
    assert!(
        matches!(
            &mismatch,
            BridgeError::PartitionCountMismatch { topic, .. } if topic == "orders"
        ),
        "error must quote the Kafka topic name 'orders', not the Iggy name 'orders_v2': \
         {mismatch:?}"
    );
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

/// Regression test: `ensure_stream` used to hand `ensure_topic` the stream's *numeric* id
/// (`Identifier::numeric`), and streams are backed by a recycled slab (freed keys are reused by
/// the next created stream) - a stream deleted and recreated *between* `ensure_stream`'s own
/// `get_stream`/`create_stream` call and `ensure_topic`'s use of the id it returned would leave
/// `ensure_topic` writing into whatever stream now holds that recycled numeric key, not the one
/// `ensure_stream_and_topic`'s caller actually resolved. The fix threads the *named* `Identifier`
/// through instead.
///
/// What this test actually proves, and what it does not: `ensure_stream` and `ensure_topic` are
/// both private, called back-to-back inside one `ensure_stream_and_topic` invocation with no
/// `await` point this black-box test can land a delete-and-recreate inside - the exact race the
/// bug lived in is not reproducible from here, full stop, not just "hard." What this test does
/// verify is the weaker, but real, precondition: `ensure_stream_and_topic` has no bridge-level
/// cache, so its *second*, wholly separate invocation always resolves the stream fresh, by name.
/// A hypothetical regression back to `Identifier::numeric(...)` would still pass every assertion
/// below, because the numeric id that build would return comes from this call's own fresh,
/// by-name lookup - already correct by construction, not a stale value carried over from the
/// first call. Delete-and-recreate before the second call is not exercising the TOCTOU window at
/// all; it is here only to document (via the id-collision check right below) that the metadata
/// slab does in fact recycle freed keys, which is the premise the original bug depended on.
#[tokio::test]
#[serial]
async fn ensure_topic_targets_the_streams_live_incarnation_after_a_delete_and_recreate() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("first call creates the default stream and topic");

    let raw = raw_client(&server).await;
    let stream_name = Identifier::named("kafka").expect("valid stream name");
    let original = raw
        .get_stream(&stream_name)
        .await
        .expect("get_stream call")
        .expect("stream exists after ensure_stream_and_topic");
    raw.delete_stream(&Identifier::numeric(original.id).expect("numeric id"))
        .await
        .expect("delete the stream (and its topic with it)");
    let recreated = raw
        .create_stream("kafka")
        .await
        .expect("recreate a stream under the same name");
    if recreated.id == original.id {
        // Not guaranteed by any API contract, but the metadata slab does reuse freed low
        // indices - confirms this run actually exercised the recycled-key scenario, not just a
        // coincidentally-fresh one.
        eprintln!(
            "recreated stream reused the original numeric id {}",
            recreated.id
        );
    }

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("must create the topic under the live stream, not a stale numeric id");

    let topic = raw
        .get_topic(
            &stream_name,
            &Identifier::named("orders").expect("valid topic name"),
        )
        .await
        .expect("get_topic call")
        .expect("topic exists under the recreated stream");
    assert_eq!(topic.partitions_count, 1);
}

/// Finding: the SDK's connection-string parser splits on `@` then `:`, so a password containing
/// either character breaks unless credentials are passed as already-separated fields.
#[tokio::test]
#[serial]
async fn connect_succeeds_with_password_containing_special_characters() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn_with_password(data_dir.path(), "p@ss:word").await;

    IggyBridge::connect(server.test_config())
        .await
        .expect("bridge must connect with a password containing '@' and ':'");
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
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("valid mapping for this test's fixture data"),
    };

    let result = IggyBridge::connect(config).await;
    assert!(matches!(result, Err(BridgeError::Iggy(_))));
}

/// Regression test for the missing dial timeout: `TcpClient::establish_bounded` only applies its
/// own `FAILOVER_DIAL_TIMEOUT` when at least two failover candidates are configured, which a
/// bridge (always exactly one address) never has, so the underlying `TcpStream::connect` had no
/// deadline at all against an address that drops packets instead of refusing them. 192.0.2.1 is
/// RFC 5737 TEST-NET-1 - reserved for documentation, routed nowhere, so the connect attempt hangs
/// on the kernel's own SYN-retry timeout (confirmed against this exact address before writing this
/// test: over the sandbox's real network stack, a bare `connect()` past 3s had not yet failed).
/// Before `REQUEST_TIMEOUT`, this test would have hung for minutes; now it must fail within a
/// bounded window.
#[tokio::test]
async fn connect_to_a_black_hole_address_times_out_instead_of_hanging() {
    let config = IggyBridgeConfig {
        address: "192.0.2.1:1234".to_string(),
        username: "iggy".to_string(),
        password: SecretString::from("iggy"),
        topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new())
            .expect("valid mapping for this test's fixture data"),
    };

    let start = tokio::time::Instant::now();
    // Outer safety net, not the behavior under test: if REQUEST_TIMEOUT regresses to "none"
    // again, this fails the test in bounded time instead of hanging the whole suite.
    let result = tokio::time::timeout(Duration::from_secs(30), IggyBridge::connect(config))
        .await
        .expect("IggyBridge::connect must return on its own, not hang past a generous margin");

    assert!(
        start.elapsed() < Duration::from_secs(20),
        "connect took {:?}, longer than REQUEST_TIMEOUT (15s) should allow",
        start.elapsed()
    );
    // BridgeError::Timeout, not BridgeError::Iggy: with_request_timeout's own elapsed branch
    // maps here (see that function's doc for why an unknown outcome must not borrow
    // CannotEstablishConnection's known-safe code).
    assert!(matches!(result, Err(BridgeError::Timeout)));
}

/// Regression test: `IggyClient::disconnect` (what `close` used to call) never touches
/// `heartbeat_handle`, so a bridge that called it kept heartbeating on a schedule and would
/// silently reconnect. `close` now calls `shutdown`, which this test only asserts succeeds against
/// a live connection - the heartbeat task's actual termination isn't observable from outside the
/// SDK, but a `close` that itself errored or hung would be a regression this catches directly.
#[tokio::test]
#[serial]
async fn close_succeeds_against_a_live_connection() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge.close().await.expect("close must succeed");
}

/// Kafka-client-observable regression test for the auth-mapping fix: a wrong bridge password is
/// the *bridge's own* misconfiguration (`IGGY_KAFKA_IGGY_PASSWORD`), not anything the Kafka client
/// did - the SDK's own sign-in path raises `InvalidPassword`/`InvalidCredentials` for this, not
/// `Unauthorized` (that one means a real, authenticated-but-forbidden ACL problem). Mapping this
/// to `TOPIC_AUTHORIZATION_FAILED` (29) would hand a real Kafka client library a fatal,
/// non-retriable "Not authorized to access topics" it has no way to act on. Asserts the actual
/// wire code a handler would send, not just the `BridgeError` variant - that number is what a real
/// Kafka client's error-handling logic branches on.
#[tokio::test]
#[serial]
async fn connect_with_wrong_password_maps_to_unknown_server_error_not_authorization_failed() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn_with_password(data_dir.path(), "the-real-password").await;

    let mut config = server.test_config();
    config.password = SecretString::from("a-completely-wrong-password");
    // `IggyBridge` derives no `Debug`, so `expect_err`/`unwrap_err` (which require `T: Debug` on
    // the `Ok` side too) don't apply here - match it out by hand instead.
    let Err(err) = IggyBridge::connect(config).await else {
        panic!("wrong password must not connect")
    };

    assert_eq!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_UNKNOWN_SERVER_ERROR,
        "a bridge-side credential error must not surface as the Kafka client's own \
         TOPIC_AUTHORIZATION_FAILED (29): {err:?}"
    );
    assert_ne!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_TOPIC_AUTHORIZATION_FAILED,
        "must not blame the Kafka client's ACLs for the bridge's own wrong password: {err:?}"
    );
}

/// End-to-end regression test for the batch high-watermark API: creates a 3-partition topic,
/// produces a different message count to each partition, and confirms one `high_watermarks` call
/// reports all three correctly and in the order requested - not just that a single-partition call
/// still works (that's `high_watermark_reflects_produced_messages`), but that the batching itself
/// keeps each partition's own count separate rather than conflating them.
#[tokio::test]
#[serial]
async fn high_watermarks_reports_every_requested_partition_from_one_round_trip() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("stream and topic must exist before producing");

    let stream_id = Identifier::named("kafka").expect("valid stream name");
    let topic_id = Identifier::named("orders").expect("valid topic name");
    let client = raw_client(&server).await;
    for (partition, count) in [(0u32, 1usize), (1, 3), (2, 0)] {
        if count == 0 {
            continue;
        }
        let mut messages: Vec<IggyMessage> = (0..count)
            .map(|i| IggyMessage::from(format!("partition-{partition}-message-{i}")))
            .collect();
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .expect("send messages to this partition");
    }

    let watermarks: Vec<(u32, i64)> = bridge
        .high_watermarks("orders", &[0, 1, 2])
        .await
        .expect("all three partitions exist on this topic")
        .into_iter()
        .map(|(partition, watermark)| {
            (
                partition,
                watermark.expect("every requested partition exists on this topic"),
            )
        })
        .collect();

    assert_eq!(
        watermarks,
        vec![(0, 1), (1, 3), (2, 0)],
        "must report each partition's own watermark, in the order requested"
    );
}

/// Regression test: an earlier version of `high_watermarks` collected the whole batch into one
/// `Result`, so the first out-of-range partition discarded every watermark already resolved for
/// the call. Mixes a valid and an out-of-range partition in one call and asserts the valid one's
/// watermark still comes back.
#[tokio::test]
#[serial]
async fn high_watermarks_keeps_resolved_partitions_when_another_is_out_of_range() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 1)
        .await
        .expect("stream and topic must exist before checking watermarks");

    let results = bridge
        .high_watermarks("orders", &[0, 5])
        .await
        .expect("the call itself succeeds - only partition 5 is out of range, not the topic");

    assert_eq!(
        results.len(),
        2,
        "must report one entry per requested partition"
    );
    // BridgeError has no PartialEq (it wraps IggyError, itself not comparable), so the per-partition
    // Results are checked by hand rather than via assert_eq! on the whole tuple.
    let (partition0, watermark0) = &results[0];
    assert_eq!(*partition0, 0);
    assert_eq!(
        *watermark0
            .as_ref()
            .expect("partition 0 exists and must not be discarded by partition 5's error"),
        0
    );
    let (partition5, watermark5) = &results[1];
    assert_eq!(*partition5, 5);
    assert!(
        matches!(watermark5, Err(BridgeError::PartitionOutOfRange { .. })),
        "partition 5 does not exist on a 1-partition topic: {watermark5:?}"
    );
}

/// Kafka-client-observable regression test for topic-name validation: a whitespace-padded name is
/// not just unlikely for a real Kafka client to send, it's impossible - `Topic.legalChars` has no
/// space in it - so this exercises the defense-in-depth path a non-conformant client could still
/// reach, and asserts the wire code (`INVALID_TOPIC_EXCEPTION`, 17) a real client library would
/// recognize as "the topic name itself is the problem," not a generic server error.
#[tokio::test]
#[serial]
async fn ensure_stream_and_topic_rejects_a_padded_kafka_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    let err = bridge
        .ensure_stream_and_topic(" orders ", 1)
        .await
        .expect_err("a padded Kafka topic name must not silently create a padded Iggy topic");

    assert!(
        matches!(err, BridgeError::InvalidKafkaTopicName { .. }),
        "expected InvalidKafkaTopicName, got {err:?}"
    );
    assert_eq!(
        err.to_kafka_error_code(),
        iggy_gateway_kafka::protocol::api::ERROR_INVALID_TOPIC_EXCEPTION
    );
}

#[tokio::test]
#[serial]
async fn high_watermark_rejects_a_padded_kafka_topic_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    let err = bridge
        .high_watermark(" orders ", 0)
        .await
        .expect_err("a padded Kafka topic name must be rejected before any Iggy lookup");
    assert!(matches!(err, BridgeError::InvalidKafkaTopicName { .. }));
}

// ── get_kafka_topic / list_kafka_topics (#3534 Metadata) ───────────────────

#[tokio::test]
#[serial]
async fn get_kafka_topic_returns_none_for_a_topic_that_does_not_exist() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    let result = bridge
        .get_kafka_topic("never-created")
        .await
        .expect("lookup itself must not fail just because the topic is missing");
    assert!(
        result.is_none(),
        "a never-created topic must read as absent, not auto-create it \
         (get_kafka_topic is read-only discovery, unlike ensure_stream_and_topic)"
    );
}

#[tokio::test]
#[serial]
async fn get_kafka_topic_reports_partitions_count_after_creation() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 4)
        .await
        .expect("stream and topic must exist before the lookup");

    let metadata = bridge
        .get_kafka_topic("orders")
        .await
        .expect("lookup call")
        .expect("topic exists after ensure_stream_and_topic");
    assert_eq!(metadata.kafka_topic, "orders");
    assert_eq!(metadata.partitions_count, 4);
}

/// Regression test for the Kafka-side vs Iggy-side name distinction `get_kafka_topic` shares
/// with `ensure_stream_and_topic`/`high_watermark`: a mapping override renames the Iggy side, so
/// looking the topic up by its *Kafka* name must still resolve, even though no Iggy topic is
/// literally named that.
#[tokio::test]
#[serial]
async fn get_kafka_topic_resolves_through_a_topic_mapping_override() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut topics = HashMap::new();
    topics.insert(
        "orders".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "orders_v2".to_string(),
        },
    );
    config.topic_mapping = TopicMapping::new("kafka".to_string(), topics)
        .expect("valid mapping for this test's fixture data");
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 2)
        .await
        .expect("must create the mapped Iggy stream/topic");

    let metadata = bridge
        .get_kafka_topic("orders")
        .await
        .expect("lookup call")
        .expect("topic exists under its Iggy-side name");
    assert_eq!(
        metadata.kafka_topic, "orders",
        "must report the Kafka-side name the caller asked about, not the Iggy-side 'orders_v2'"
    );
    assert_eq!(metadata.partitions_count, 2);
}

#[tokio::test]
#[serial]
async fn list_kafka_topics_is_empty_for_a_fresh_default_stream() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    // No ensure_stream_and_topic call at all: the default_stream ("kafka") has never been
    // created, so list_kafka_topics must not error just because get_stream finds nothing.
    let topics = bridge
        .list_kafka_topics()
        .await
        .expect("listing must succeed even when the default stream doesn't exist yet");
    assert!(topics.is_empty());
}

#[tokio::test]
#[serial]
async fn list_kafka_topics_reports_every_topic_in_the_default_stream() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 2)
        .await
        .expect("create orders");
    bridge
        .ensure_stream_and_topic("payments", 1)
        .await
        .expect("create payments");

    let mut topics = bridge.list_kafka_topics().await.expect("listing call");
    topics.sort_by(|a, b| a.kafka_topic.cmp(&b.kafka_topic));

    assert_eq!(topics.len(), 2, "must report both topics, not just one");
    assert_eq!(topics[0].kafka_topic, "orders");
    assert_eq!(topics[0].partitions_count, 2);
    assert_eq!(topics[1].kafka_topic, "payments");
    assert_eq!(topics[1].partitions_count, 1);
}

/// `list_kafka_topics` must also surface topics reachable only through a mapping override -
/// its own doc comment explains why one bulk `get_topics(default_stream)` call cannot see these
/// (a different target stream, or a different Iggy topic name under the same stream).
#[tokio::test]
#[serial]
async fn list_kafka_topics_includes_a_created_override_target_but_not_an_uncreated_one() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut topics = HashMap::new();
    topics.insert(
        "orders".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "orders_v2".to_string(),
        },
    );
    topics.insert(
        "never-created".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "never_created_v2".to_string(),
        },
    );
    config.topic_mapping = TopicMapping::new("kafka".to_string(), topics)
        .expect("valid mapping for this test's fixture data");
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("create the override target for 'orders'");
    // Deliberately never call ensure_stream_and_topic("never-created", ..): real Kafka Metadata
    // lists existing topics, not configured-but-uncreated ones.

    let listed = bridge.list_kafka_topics().await.expect("listing call");
    assert_eq!(
        listed.len(),
        1,
        "must report only the override target that was actually created: {listed:?}"
    );
    assert_eq!(listed[0].kafka_topic, "orders");
    assert_eq!(listed[0].partitions_count, 3);
}

/// `list_kafka_topics`'s bulk `get_topics(default_stream)` branch must not report a Kafka topic
/// name that is itself another override's key - `resolve()` on such a name always uses that
/// override, never falls back to identity, so listing it under its raw Iggy-side name would
/// report a Kafka topic whose own individual lookup answers about a completely different
/// physical topic.
#[tokio::test]
#[serial]
async fn list_kafka_topics_excludes_a_default_stream_name_that_collides_with_another_overrides_key()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut topics = HashMap::new();
    // "reports" physically creates (kafka/orders_internal) - the literal name a naive bulk
    // listing would report.
    topics.insert(
        "reports".to_string(),
        TopicOverride {
            stream: "kafka".to_string(),
            topic: "orders_internal".to_string(),
        },
    );
    // "orders_internal" is itself an override key redirecting elsewhere, so the literal string
    // "orders_internal" does not identity-resolve to (kafka, orders_internal) even though a
    // physical topic with that exact name lives there.
    topics.insert(
        "orders_internal".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "whatever".to_string(),
        },
    );
    config.topic_mapping = TopicMapping::new("kafka".to_string(), topics)
        .expect("valid mapping for this test's fixture data");
    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");

    bridge
        .ensure_stream_and_topic("reports", 2)
        .await
        .expect("create the override target for 'reports'");
    // Deliberately never create the "orders_internal" override's own target (billing/whatever).

    let listed = bridge.list_kafka_topics().await.expect("listing call");
    assert_eq!(
        listed.len(),
        1,
        "must report 'reports' once, never the raw Iggy-side name 'orders_internal': {listed:?}"
    );
    assert_eq!(listed[0].kafka_topic, "reports");
    assert_eq!(listed[0].partitions_count, 2);
}
