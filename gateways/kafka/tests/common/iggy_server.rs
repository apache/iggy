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

//! Real `iggy-server` process harness, shared by every suite in this crate that needs one.
//!
//! Included with `#[path]`, so its unit tests run once per including binary. They are pure
//! functions over a synthetic range, so the repeat is free.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::time::Duration;

use iggy::prelude::{AutoLogin, Client, Credentials, IggyClient, IggyClientBuilder};
use secrecy::SecretString;

use iggy_gateway_kafka::bridge::{IggyBridgeConfig, TopicMapping};

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
    // Below this floor both allocators fall back to `ceiling + 1`, and they lock different
    // files (`{port}.lock` here, `slot-{slot}.lock` in `core/integration`), so neither sees the
    // other's reservation. A port handed out here collides silently, so refuse instead.
    panic!(
        "kernel ephemeral range [{floor}, {ceiling}] leaves no room for a test port band below \
         {MIN_CANDIDATE_PORT}, and the band above {ceiling} is shared with core/integration's \
         own allocator under a different lock - narrow the range, e.g. \
         sysctl -w net.ipv4.ip_local_port_range='32768 60999'"
    );
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

/// `ip_local_port_range = "1024 60999"` leaves room above the ceiling, but `core/integration`
/// falls back to the same anchor under a lock this one cannot see.
#[test]
#[should_panic(expected = "shared with core/integration")]
fn given_no_room_below_the_floor_when_choosing_a_band_should_refuse() {
    band_for(1024, 60999);
}

/// A ceiling at the `u16` maximum leaves nothing above it, and must not overflow while saying
/// so.
#[test]
#[should_panic(expected = "leaves no room")]
fn given_a_ceiling_at_the_u16_maximum_when_choosing_a_band_should_refuse() {
    band_for(1024, u16::MAX);
}

/// Exclusive claim on one port, released (and the port freed for reuse) when dropped - including
/// on an unclean process exit, since the OS drops the `flock` with the file descriptor. Unlike
/// bind-then-drop, this process itself never binds the port before `iggy-server` does; see
/// `port_band`'s doc comment for the limits of that guarantee against *other* processes.
pub struct PortGuard {
    pub port: u16,
    _lock: File,
}

impl PortGuard {
    pub fn acquire() -> Self {
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

pub struct TestServer {
    child: Child,
    address: String,
    password: String,
    _port_guard: PortGuard,
}

impl TestServer {
    /// Spawns `iggy-server` with the default `iggy`/`iggy` root credentials. See
    /// [`Self::spawn_with_password`] for the general form.
    pub async fn spawn(data_dir: &std::path::Path) -> Self {
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
    pub async fn spawn_with_password(data_dir: &std::path::Path, password: &str) -> Self {
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

    pub fn test_config(&self) -> IggyBridgeConfig {
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
pub async fn raw_client(server: &TestServer) -> IggyClient {
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
