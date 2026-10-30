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
//!     the ETag chain,
//!   * exactly-once source whose save after a send fails -> the batch that the
//!     plugin produces again lands once,
//!   * exactly-once source restarted with a batch in flight -> the batch lands
//!     once, and new batches use the next epoch.

use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy::prelude::IggyClient;
use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus, SourceInfoResponse};
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
const STATE_URL_QUERY_SECRET: &str = "state-query-secret-not-for-logs";
const EXACTLY_ONCE_KEY: &str = "random_exactly_once";
const EXACTLY_ONCE_RESOURCE_PATH: &str = "/source_random_exactly_once";
/// Starts the state of an exactly-once source.
const EXACTLY_ONCE_MAGIC: &[u8] = b"\0IGGYEO\x01";
/// The flag after the magic, producer id, epoch, partition id and next sequence. It is 1 while
/// the state names a batch in flight.
const EXACTLY_ONCE_IN_FLIGHT_AT: usize = 26;
/// The `messages_range` and `max_count` of the exactly-once source.
const EXACTLY_ONCE_BATCH: usize = 3;
const EXACTLY_ONCE_MESSAGES: usize = 60;
/// Both attempts of one save at `max_attempts = 1`.
const PUTS_PER_FAILED_SAVE: u64 = 2;
/// Time for a batch that a source writes twice to reach the topic.
const SETTLE_TIME: Duration = Duration::from_millis(500);
const POLL_BATCH: u32 = 1_000;

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
    /// Fails writes of an exactly-once state with no batch in flight, which the runtime saves
    /// after a send.
    fail_next_settled_puts: AtomicU64,
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
        if names_batch_in_flight(&request.body) == Some(false)
            && store
                .fail_next_settled_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return ResponseTemplate::new(503);
        }
        if store
            .fail_next_puts
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
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
        let (server, store) = start_state_server(RESOURCE_PATH, SharedStore::default()).await;
        Ok(Self { server, store })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        state_server_envs(&self.server)
    }
}

/// The state server of the exactly-once source. Its first `FAILED_SETTLED_PUTS` writes of a
/// state with no batch in flight answer `503`, so the save after a send fails and the runtime
/// Nacks a batch that Iggy holds.
pub struct ExactlyOnceStateStore<const FAILED_SETTLED_PUTS: u64> {
    server: MockServer,
    store: Arc<SharedStore>,
}

#[async_trait]
impl<const FAILED_SETTLED_PUTS: u64> TestFixture for ExactlyOnceStateStore<FAILED_SETTLED_PUTS> {
    async fn setup() -> Result<Self, TestBinaryError> {
        let store = SharedStore {
            fail_next_settled_puts: AtomicU64::new(FAILED_SETTLED_PUTS),
            ..SharedStore::default()
        };
        let (server, store) = start_state_server(EXACTLY_ONCE_RESOURCE_PATH, store).await;
        Ok(Self { server, store })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        state_server_envs(&self.server)
    }
}

/// The save after the first send fails.
type LostSaveFixture = ExactlyOnceStateStore<PUTS_PER_FAILED_SAVE>;
/// Every save after a send fails until the test clears the count.
type FailingSavesFixture = ExactlyOnceStateStore<{ u64::MAX }>;

async fn start_state_server(
    resource_path: &str,
    store: SharedStore,
) -> (MockServer, Arc<SharedStore>) {
    let server = MockServer::start().await;
    let store = Arc::new(store);
    Mock::given(path(resource_path))
        .respond_with(StateStoreResponder(store.clone()))
        .mount(&server)
        .await;
    (server, store)
}

fn state_server_envs(server: &MockServer) -> HashMap<String, String> {
    HashMap::from([("IGGY_CONNECTORS_STATE_HTTP_URL".to_string(), server.uri())])
}

/// Whether the state of an exactly-once source names a batch in flight. `None` for any other
/// state.
fn names_batch_in_flight(body: &[u8]) -> Option<bool> {
    body.starts_with(EXACTLY_ONCE_MAGIC)
        .then(|| body.get(EXACTLY_ONCE_IN_FLIGHT_AT) == Some(&1))
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

async fn fetch_source(harness: &TestHarness, key: &str) -> SourceInfoResponse {
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
        .find(|source| source.key == key)
        .expect("HTTP-state source should be reported")
}

async fn wait_for_status(harness: &TestHarness, key: &str, expected: ConnectorStatus) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        let source = fetch_source(harness, key).await;
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

async fn wait_for_last_error(harness: &TestHarness, key: &str, expected: &str) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        let source = fetch_source(harness, key).await;
        if source
            .last_error
            .as_ref()
            .is_some_and(|error| error.message.contains(expected))
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out after {WAIT_DEADLINE:?} waiting for source error {expected:?}, last seen {:?}",
                source.last_error
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn restart_source(harness: &TestHarness, key: &str) {
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available")
        .http_url();
    let response = Client::new()
        .post(format!("{api_url}/sources/{key}/restart"))
        .send()
        .await
        .expect("restart request should be sent");
    assert!(
        response.status().is_success(),
        "restart should succeed, got {}",
        response.status()
    );
}

/// The messages that partition 0 of the seeded topic holds.
async fn stored_messages(client: &IggyClient) -> usize {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let mut stored = 0;
    loop {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(stored as u64),
                POLL_BATCH,
                false,
            )
            .await
            .expect("poll the source topic");
        if polled.messages.is_empty() {
            return stored;
        }
        stored += polled.messages.len();
    }
}

async fn wait_for_messages(client: &IggyClient, expected: usize) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        let stored = stored_messages(client).await;
        if stored >= expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out after {WAIT_DEADLINE:?} waiting for {expected} messages, the topic holds {stored}"
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
    let state_url = format!(
        "http://127.0.0.1:{}?token={STATE_URL_QUERY_SECRET}",
        listener.local_addr().unwrap().port()
    );

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
    assert!(
        !logs.contains(STATE_URL_QUERY_SECRET),
        "runtime startup and state-load failures must not log URL query secrets:\n{logs}"
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
    wait_for_status(harness, SOURCE_KEY, ConnectorStatus::Running).await;
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
    wait_for_status(harness, SOURCE_KEY, ConnectorStatus::Error).await;

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

    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        let source = fetch_source(harness, SOURCE_KEY).await;
        if source
            .last_error
            .as_ref()
            .is_some_and(|error| error.message.contains("consecutive NACK limit reached"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "source did not report a restart-required NACK-limit stop"
        );
        sleep(POLL_INTERVAL).await;
    }

    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let stats: ConnectorRuntimeStats = Client::new()
        .get(format!("{api_url}/stats"))
        .send()
        .await
        .expect("stats request should complete")
        .error_for_status()
        .expect("stats endpoint should succeed")
        .json()
        .await
        .expect("stats response should decode");
    assert_eq!(stats.sources_total, 1);
    assert_eq!(
        stats.sources_running, 0,
        "a latched source has Error status and must not count as running"
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

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/http_state_exactly_once.toml"
    )),
    seed = seeds::connector_stream
)]
async fn given_a_failed_save_after_a_send_when_the_source_sends_again_should_write_the_batch_once(
    harness: &TestHarness,
    fixture: LostSaveFixture,
) {
    let client = harness.root_client().await.expect("root client");

    wait_for_messages(&client, EXACTLY_ONCE_MESSAGES).await;
    sleep(SETTLE_TIME).await;

    assert_eq!(
        fixture.store.fail_next_settled_puts.load(Ordering::SeqCst),
        0,
        "the save after the first send must fail"
    );
    assert_eq!(
        stored_messages(&client).await,
        EXACTLY_ONCE_MESSAGES,
        "the Nacked batch went out again and must land once"
    );
    assert_eq!(
        names_batch_in_flight(&fixture.store.body.lock().expect("store lock")),
        Some(false),
        "the last save must settle the last batch"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/http_state_exactly_once.toml"
    )),
    seed = seeds::connector_stream
)]
async fn given_a_batch_in_flight_when_the_source_restarts_should_write_the_batch_once(
    harness: &TestHarness,
    fixture: FailingSavesFixture,
) {
    // No save after a send lands, so the stored state names the first batch in flight until the
    // source stops at its Nack limit.
    wait_for_last_error(harness, EXACTLY_ONCE_KEY, "consecutive NACK limit reached").await;
    let client = harness.root_client().await.expect("root client");
    assert_eq!(stored_messages(&client).await, EXACTLY_ONCE_BATCH);
    assert_eq!(
        names_batch_in_flight(&fixture.store.body.lock().expect("store lock")),
        Some(true),
        "the stored state must name the first batch in flight"
    );
    fixture
        .store
        .fail_next_settled_puts
        .store(0, Ordering::SeqCst);

    restart_source(harness, EXACTLY_ONCE_KEY).await;
    wait_for_messages(&client, EXACTLY_ONCE_MESSAGES).await;
    sleep(SETTLE_TIME).await;

    assert_eq!(
        stored_messages(&client).await,
        EXACTLY_ONCE_MESSAGES,
        "the batch in flight went out again after the restart and must land once"
    );
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available")
        .collect_logs();
    assert!(
        format!("{stdout}\n{stderr}").contains("at epoch 1 to partition 0"),
        "the restarted source must write at the next epoch"
    );
}
