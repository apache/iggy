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

//! Runtime-level tests for the HTTP state storage backend.
//!
//! Each test runs the real connectors runtime against an in-process wiremock
//! state server that enforces the protocol contract: conditional writes
//! (`If-None-Match: *` / `If-Match`), strong ETags per committed write, and
//! injectable failure modes (`412` conflicts, `503` bursts). The scenarios:
//!   * state store unreachable at boot with an enabled source -> the runtime
//!     process exits instead of minting an idle `FailedPlugin`,
//!   * `404` at boot -> the source runs from its default state and its
//!     checkpoints land on the stub,
//!   * `412` mid-stream -> the provider latches, the checkpoint stops
//!     advancing, and no further PUTs reach the server,
//!   * `503` burst mid-stream -> saves Nack, then recover once the store is
//!     healthy again,
//!   * API-driven restart -> the source reloads the served state and resumes
//!     the ETag chain.

use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::config::TestServerConfig;
use integration::harness::{TestBinaryError, TestFixture, TestHarness, seeds};
use integration::iggy_harness;
use reqwest::header::{ETAG, HeaderName, IF_MATCH, IF_NONE_MATCH};
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const SOURCE_KEY: &str = "random_http_state";
const RESOURCE_PATH: &str = "/source_random_http_state";
const RUNTIME_CONFIG_PATH: &str = "tests/connectors/runtime/http_state.toml";
const WAIT_DEADLINE: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const IDEMPOTENCY_KEY_HEADER: HeaderName = HeaderName::from_static("idempotency-key");

/// In-memory state server backing the wiremock responder. Enforces the
/// conditional-write contract and exposes counters plus injectable failure
/// modes for the tests.
#[derive(Default)]
struct SharedStore {
    version: AtomicU64,
    body: StdMutex<Vec<u8>>,
    committed_requests: StdMutex<HashMap<String, (Vec<u8>, String)>>,
    conflict_mode: AtomicBool,
    fail_next_puts: AtomicU64,
    get_count: AtomicU64,
    put_count: AtomicU64,
}

impl SharedStore {
    fn etag(version: u64) -> String {
        format!("\"v{version}\"")
    }
}

struct StateStoreResponder(Arc<SharedStore>);

impl Respond for StateStoreResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let store = &self.0;
        if request.method == Method::GET {
            store.get_count.fetch_add(1, Ordering::SeqCst);
            let version = store.version.load(Ordering::SeqCst);
            if version == 0 {
                return ResponseTemplate::new(404);
            }
            let body = store.body.lock().expect("store lock").clone();
            return ResponseTemplate::new(200)
                .insert_header(ETAG, SharedStore::etag(version).as_str())
                .set_body_bytes(body);
        }
        if request.method != Method::PUT
            && request.method != Method::POST
            && request.method != Method::PATCH
        {
            return ResponseTemplate::new(405);
        }

        store.put_count.fetch_add(1, Ordering::SeqCst);
        let Some(idempotency_key) = request
            .headers
            .get(&IDEMPOTENCY_KEY_HEADER)
            .and_then(|value| value.to_str().ok())
        else {
            return ResponseTemplate::new(400);
        };
        if let Some((committed_body, etag)) = store
            .committed_requests
            .lock()
            .expect("idempotency lock")
            .get(idempotency_key)
            .cloned()
        {
            if committed_body != request.body {
                return ResponseTemplate::new(409);
            }
            return ResponseTemplate::new(200).insert_header(ETAG, etag.as_str());
        }
        if store
            .fail_next_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return ResponseTemplate::new(503);
        }
        if store.conflict_mode.load(Ordering::SeqCst) {
            return ResponseTemplate::new(412);
        }
        let version = store.version.load(Ordering::SeqCst);
        let condition_ok = if version == 0 {
            request
                .headers
                .get(IF_NONE_MATCH)
                .map(|value| value.as_bytes() == b"*")
                .unwrap_or(false)
        } else {
            request
                .headers
                .get(IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .map(|value| value == SharedStore::etag(version))
                .unwrap_or(false)
        };
        if !condition_ok {
            return ResponseTemplate::new(412);
        }
        let next = version + 1;
        *store.body.lock().expect("store lock") = request.body.clone();
        store.version.store(next, Ordering::SeqCst);
        let etag = SharedStore::etag(next);
        store
            .committed_requests
            .lock()
            .expect("idempotency lock")
            .insert(
                idempotency_key.to_string(),
                (request.body.clone(), etag.clone()),
            );
        ResponseTemplate::new(200).insert_header(ETAG, etag.as_str())
    }
}

pub struct HttpStateStoreFixture {
    server: MockServer,
    store: Arc<SharedStore>,
}

#[async_trait]
impl TestFixture for HttpStateStoreFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let server = MockServer::start().await;
        let store = Arc::new(SharedStore::default());
        Mock::given(path(RESOURCE_PATH))
            .respond_with(StateStoreResponder(store.clone()))
            .mount(&server)
            .await;
        Ok(Self { server, store })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([(
            "IGGY_CONNECTORS_STATE_HTTP_URL".to_string(),
            self.server.uri(),
        )])
    }
}

async fn wait_until<F: Fn() -> bool>(what: &str, condition: F) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    while !condition() {
        if Instant::now() >= deadline {
            panic!("timed out after {WAIT_DEADLINE:?} waiting for {what}");
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn fetch_source(harness: &TestHarness) -> SourceInfoResponse {
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available")
        .http_url();
    let sources: Vec<SourceInfoResponse> = Client::new()
        .get(format!("{api_url}/sources"))
        .send()
        .await
        .expect("Failed to query /sources")
        .json()
        .await
        .expect("Failed to parse sources");
    sources
        .into_iter()
        .find(|source| source.key == SOURCE_KEY)
        .expect("HTTP-state source should be reported")
}

async fn wait_for_status(harness: &TestHarness, expected: ConnectorStatus) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        let source = fetch_source(harness).await;
        if source.status == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out after {WAIT_DEADLINE:?} waiting for source status {expected:?}, last seen {:?} ({:?})",
                source.status, source.last_error
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn given_unavailable_state_store_when_booting_should_fail_startup() {
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::default())
        .build()
        .expect("harness should build");
    harness
        .start_with_seed(|client| async move { seeds::connector_stream(&client).await })
        .await
        .expect("iggy-server should start");
    let iggy_address = harness
        .server()
        .tcp_addr()
        .expect("server TCP address should be known");

    // Bound but never accepted, so every state request times out instead of
    // racing other tests for a recycled port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let state_url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());

    let mut command = Command::cargo_bin("iggy-connectors").expect("iggy-connectors binary");
    command
        .env("IGGY_CONNECTORS_CONFIG_PATH", RUNTIME_CONFIG_PATH)
        .env("IGGY_CONNECTORS_IGGY_ADDRESS", iggy_address.to_string())
        .env("IGGY_CONNECTORS_HTTP_ADDRESS", "127.0.0.1:0")
        .env("IGGY_CONNECTORS_STATE_HTTP_URL", state_url)
        .env("IGGY_CONNECTORS_STATE_HTTP_TIMEOUT", "200ms")
        .stdin(Stdio::null());

    let output = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || command.output()),
    )
    .await
    .expect("the runtime must exit instead of running with an unavailable state store")
    .expect("join spawned command")
    .expect("spawn iggy-connectors");

    assert!(
        !output.status.success(),
        "boot must fail when the state store is unreachable for an enabled source"
    );
    let logs = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        logs.contains("failed to load state") || logs.contains("StateLoadFailed"),
        "startup failure should point at the state load, got:\n{logs}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/http_state.toml")),
    seed = seeds::connector_stream
)]
async fn given_empty_state_store_when_booted_should_run_and_checkpoint(
    harness: &TestHarness,
    fixture: HttpStateStoreFixture,
) {
    crate::connectors::random_source_liveness::assert_produces_messages(harness).await;
    wait_for_status(harness, ConnectorStatus::Running).await;
    wait_until("the first checkpoint PUT to reach the state server", || {
        fixture.store.version.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(
        !fixture.store.body.lock().expect("store lock").is_empty(),
        "the stored checkpoint must carry the serialized source state"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/http_state.toml")),
    seed = seeds::connector_stream
)]
async fn given_conflict_mid_stream_should_nack_and_latch(
    harness: &TestHarness,
    fixture: HttpStateStoreFixture,
) {
    wait_until("the first committed checkpoint", || {
        fixture.store.version.load(Ordering::SeqCst) > 0
    })
    .await;

    fixture.store.conflict_mode.store(true, Ordering::SeqCst);
    wait_for_status(harness, ConnectorStatus::Error).await;

    let version_after_conflict = fixture.store.version.load(Ordering::SeqCst);
    let puts_after_conflict = fixture.store.put_count.load(Ordering::SeqCst);
    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        fixture.store.put_count.load(Ordering::SeqCst),
        puts_after_conflict,
        "a latched provider must not send further PUTs"
    );
    assert_eq!(
        fixture.store.version.load(Ordering::SeqCst),
        version_after_conflict,
        "the checkpoint must not advance after a 412"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/http_state.toml")),
    seed = seeds::connector_stream
)]
async fn given_unavailable_burst_mid_stream_should_nack_then_recover(
    harness: &TestHarness,
    fixture: HttpStateStoreFixture,
) {
    wait_until("the first committed checkpoint", || {
        fixture.store.version.load(Ordering::SeqCst) > 0
    })
    .await;

    // 6 failed PUTs = 3 failed saves at max_attempts = 1, i.e. 3 Nacks -
    // safely below the plugin's 5-consecutive-Nack cutoff.
    let version_before_burst = fixture.store.version.load(Ordering::SeqCst);
    fixture.store.fail_next_puts.store(6, Ordering::SeqCst);
    wait_until("checkpoints to resume after the 503 burst", || {
        fixture.store.version.load(Ordering::SeqCst) >= version_before_burst + 2
    })
    .await;
    let _ = harness;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/http_state.toml")),
    seed = seeds::connector_stream
)]
async fn given_restart_when_state_exists_should_resume_from_served_state(
    harness: &TestHarness,
    fixture: HttpStateStoreFixture,
) {
    wait_until("the first committed checkpoint", || {
        fixture.store.version.load(Ordering::SeqCst) > 0
    })
    .await;

    let gets_before_restart = fixture.store.get_count.load(Ordering::SeqCst);
    let version_before_restart = fixture.store.version.load(Ordering::SeqCst);

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available")
        .http_url();
    let response = Client::new()
        .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
        .send()
        .await
        .expect("restart request should be sent");
    assert!(
        response.status().is_success(),
        "restart should succeed, got {}",
        response.status()
    );

    wait_until("the restarted source to load state from the server", || {
        fixture.store.get_count.load(Ordering::SeqCst) > gets_before_restart
    })
    .await;
    wait_until("the restarted source to resume the ETag chain", || {
        fixture.store.version.load(Ordering::SeqCst) > version_before_restart
    })
    .await;

    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available");
    let (stdout, stderr) = runtime.collect_logs();
    let logs = format!("{stdout}\n{stderr}");
    assert!(
        logs.contains("Restored state for Random source"),
        "the plugin should restore the served state on restart"
    );
}
