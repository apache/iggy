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

//! HTTP data-plane gate for server-ng: produce, poll, and consumer-offset
//! routes exercised over raw `reqwest` (not the SDK HTTP client) so the wire
//! contract itself is under test - exact status codes, the
//! `x-iggy-durability` header, the body-size cap, and cross-request isolation
//! of concurrent produces on one login session.

use futures::future::join_all;
use iggy::prelude::*;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{ConsumerOffsetInfo, IggyMessagesBatch};
use integration::harness::TestHarness;
use integration::iggy_harness;
use reqwest::{Response, StatusCode};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;

/// Explicit consumer id shared by the offset store body and the read/delete
/// query, so both sides address the same offset-table key
/// (`Consumer::default()` would carry numeric id 0, not 1).
const CONSUMER_ID: u32 = 1;

const DURABILITY_HEADER: &str = "x-iggy-durability";
const DURABILITY_REPLICATED_MEMORY: &str = "replicated-memory";
const DURABILITY_NONE: &str = "none";

/// The harness readiness gate waits for the dumped runtime config, which the
/// server writes before binding the HTTP listener; a bounded login retry
/// bridges that gap plus single-node consensus warmup.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15);
const LOGIN_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// `?ack=none` answers before the commit; poll until visible, never unbounded.
const ASYNC_COMMIT_TIMEOUT: Duration = Duration::from_secs(15);
const ASYNC_COMMIT_RETRY_INTERVAL: Duration = Duration::from_millis(100);

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One authenticated root session against the test server's HTTP listener.
struct HttpSession {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl HttpSession {
    async fn login_root(harness: &TestHarness) -> Self {
        let addr = harness
            .server()
            .http_addr()
            .expect("HTTP transport not configured on test server");
        let base_url = format!("http://{addr}");
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("build reqwest client");

        let body = serde_json::json!({
            "username": DEFAULT_ROOT_USERNAME,
            "password": DEFAULT_ROOT_PASSWORD,
        });
        let deadline = Instant::now() + LOGIN_TIMEOUT;
        loop {
            let attempt = client
                .post(format!("{base_url}/users/login"))
                .json(&body)
                .send()
                .await;
            match attempt {
                Ok(response) if response.status().is_success() => {
                    let identity: IdentityInfo =
                        response.json().await.expect("decode IdentityInfo");
                    let token = identity
                        .access_token
                        .expect("login must return an access token")
                        .token;
                    return Self {
                        client,
                        base_url,
                        token,
                    };
                }
                // Listener not bound yet or consensus still warming up; any
                // non-5xx rejection (e.g. 401) is a real failure.
                Ok(response) if response.status().is_server_error() => {}
                Ok(response) => panic!("login rejected: {}", response.status()),
                Err(error) if error.is_connect() => {}
                Err(error) => panic!("login request failed: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "HTTP login did not succeed within {LOGIN_TIMEOUT:?}"
            );
            sleep(LOGIN_RETRY_INTERVAL).await;
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn create_stream_and_topic(&self, stream: &str, topic: &str, partitions: u32) {
        let response = self
            .client
            .post(self.url("/streams"))
            .bearer_auth(&self.token)
            .json(&CreateStream {
                name: stream.to_string(),
            })
            .send()
            .await
            .expect("create stream request");
        assert!(
            response.status().is_success(),
            "create stream failed: {}",
            response.status()
        );

        let command = CreateTopic {
            stream_id: Identifier::default(),
            partitions_count: partitions,
            compression_algorithm: CompressionAlgorithm::None,
            message_expiry: IggyExpiry::NeverExpire,
            max_topic_size: MaxTopicSize::ServerDefault,
            replication_factor: None,
            name: topic.to_string(),
        };
        let response = self
            .client
            .post(self.url(&format!("/streams/{stream}/topics")))
            .bearer_auth(&self.token)
            .json(&command)
            .send()
            .await
            .expect("create topic request");
        assert!(
            response.status().is_success(),
            "create topic failed: {}",
            response.status()
        );
    }

    async fn produce(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<IggyMessage>,
    ) -> Response {
        self.produce_with_query(stream, topic, partition_id, messages, "")
            .await
    }

    async fn produce_with_query(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<IggyMessage>,
        query: &str,
    ) -> Response {
        let body = SendMessages {
            metadata_length: 0,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::partition_id(partition_id),
            batch: IggyMessagesBatch::from(&messages),
        };
        self.client
            .post(self.url(&format!("/streams/{stream}/topics/{topic}/messages{query}")))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .expect("produce request")
    }

    async fn poll(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> PolledMessages {
        let path = format!(
            "/streams/{stream}/topics/{topic}/messages\
             ?consumer_id={CONSUMER_ID}&partition_id={partition_id}\
             &kind=offset&value={offset}&count={count}&auto_commit=false"
        );
        let response = self
            .client
            .get(self.url(&path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("poll request");
        assert_eq!(response.status(), StatusCode::OK, "poll must answer 200");
        response.json().await.expect("decode PolledMessages")
    }

    /// Log in as an already-created user, reusing this session's client and
    /// base URL but swapping the bearer for the new credential. Single attempt:
    /// the server is already warm once the root session exists.
    async fn login(&self, username: &str, password: &str) -> Self {
        let body = serde_json::json!({ "username": username, "password": password });
        let response = self
            .client
            .post(self.url("/users/login"))
            .json(&body)
            .send()
            .await
            .expect("login request");
        assert!(
            response.status().is_success(),
            "login for {username} failed: {}",
            response.status()
        );
        let identity: IdentityInfo = response.json().await.expect("decode IdentityInfo");
        let token = identity
            .access_token
            .expect("login must return an access token")
            .token;
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            token,
        }
    }

    /// Create an ungranted user (no permissions -> the permissioner holds no
    /// entry, so every RBAC rule denies). Runs as this session's user.
    async fn create_user(&self, username: &str, password: &str) {
        let body = serde_json::json!({
            "username": username,
            "password": password,
            "status": "active",
            "permissions": null,
        });
        let response = self
            .client
            .post(self.url("/users"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .expect("create user request");
        assert!(
            response.status().is_success(),
            "create user failed: {}",
            response.status()
        );
    }

    /// GET `path` with this session's bearer.
    async fn get(&self, path: &str) -> Response {
        self.client
            .get(self.url(path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("get request")
    }
}

fn durability(response: &Response) -> &str {
    response
        .headers()
        .get(DURABILITY_HEADER)
        .expect("produce response must carry the durability header")
        .to_str()
        .expect("durability header is ASCII")
}

fn text_message(id: u128, payload: String) -> IggyMessage {
    IggyMessage::builder()
        .id(id)
        .payload(payload.into())
        .build()
        .expect("message build")
}

#[iggy_harness]
async fn given_http_session_when_producing_and_polling_should_round_trip(harness: &TestHarness) {
    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-stream", "http-topic", 1)
        .await;

    let header_key = HeaderKey::try_from("trace-id").expect("header key");
    let header_value = HeaderValue::from_str("trace-42").expect("header value");
    let messages: Vec<IggyMessage> = (0..3u32)
        .map(|i| {
            let builder = IggyMessage::builder()
                .id(u128::from(i + 1))
                .payload(format!("round-trip-{i}").into());
            if i == 0 {
                let user_headers = BTreeMap::from([(header_key.clone(), header_value.clone())]);
                builder
                    .user_headers(user_headers)
                    .build()
                    .expect("message build")
            } else {
                builder.build().expect("message build")
            }
        })
        .collect();

    let response = http
        .produce("http-stream", "http-topic", PARTITION_ID, messages)
        .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "produce must commit"
    );
    assert_eq!(durability(&response), DURABILITY_REPLICATED_MEMORY);

    let polled = http
        .poll("http-stream", "http-topic", PARTITION_ID, 0, 10)
        .await;
    assert_eq!(polled.messages.len(), 3, "all sent messages must come back");
    for (i, message) in polled.messages.iter().enumerate() {
        assert_eq!(message.header.offset, i as u64, "offsets must be dense");
        assert_eq!(
            message.payload,
            bytes::Bytes::from(format!("round-trip-{i}")),
            "payload round trip for message {i}"
        );
    }
    let user_headers = polled.messages[0]
        .user_headers_map()
        .expect("headers decode")
        .expect("first message carries user headers");
    assert_eq!(
        user_headers.get(&header_key),
        Some(&header_value),
        "user header must survive the round trip"
    );
}

const CONCURRENT_PRODUCES: usize = 16;
const CONCURRENT_PARTITIONS: u32 = 4;

#[iggy_harness]
async fn given_one_session_when_producing_concurrently_should_not_cross_talk(
    harness: &TestHarness,
) {
    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-concurrent", "hammer", CONCURRENT_PARTITIONS)
        .await;

    let produces = (0..CONCURRENT_PRODUCES).map(|i| {
        let http = &http;
        let partition_id = (i as u32) % CONCURRENT_PARTITIONS;
        async move {
            let message = text_message(i as u128 + 1, format!("concurrent-{i}"));
            let response = http
                .produce("http-concurrent", "hammer", partition_id, vec![message])
                .await;
            (i, response.status(), durability(&response).to_string())
        }
    });
    let outcomes = join_all(produces).await;

    for (i, status, durability) in &outcomes {
        assert_eq!(
            *status,
            StatusCode::CREATED,
            "concurrent produce {i} must commit"
        );
        assert_eq!(
            durability, DURABILITY_REPLICATED_MEMORY,
            "concurrent produce {i} must attest a replicated commit"
        );
    }

    let mut produced: HashMap<u32, HashSet<String>> = HashMap::new();
    for i in 0..CONCURRENT_PRODUCES {
        produced
            .entry((i as u32) % CONCURRENT_PARTITIONS)
            .or_default()
            .insert(format!("concurrent-{i}"));
    }

    // Every partition must return exactly its own produced payloads: nothing
    // lost, nothing foreign. The produce path is at-least-once, so duplicates
    // are tolerated but must be byte-identical to a produced payload.
    let mut polled_union: HashSet<String> = HashSet::new();
    for partition_id in 0..CONCURRENT_PARTITIONS {
        let polled = http
            .poll("http-concurrent", "hammer", partition_id, 0, 100)
            .await;
        let expected = &produced[&partition_id];
        let mut seen: HashSet<String> = HashSet::new();
        for message in &polled.messages {
            let payload = String::from_utf8(message.payload.to_vec()).expect("payload is UTF-8");
            assert!(
                expected.contains(&payload),
                "partition {partition_id} returned foreign payload {payload:?}"
            );
            seen.insert(payload);
        }
        assert_eq!(
            &seen, expected,
            "partition {partition_id} must return exactly its own produced payloads"
        );
        polled_union.extend(seen);
    }
    assert_eq!(
        polled_union.len(),
        CONCURRENT_PRODUCES,
        "every produced payload must be polled back"
    );
}

#[iggy_harness]
async fn given_consumer_offset_when_stored_and_deleted_should_round_trip(harness: &TestHarness) {
    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-offsets", "offsets", 1)
        .await;

    // Two committed messages so offset 1 names a real position.
    let messages = (0..2u32)
        .map(|i| text_message(u128::from(i + 1), format!("offset-fodder-{i}")))
        .collect();
    let response = http
        .produce("http-offsets", "offsets", PARTITION_ID, messages)
        .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "produce must commit"
    );

    let offsets_path = "/streams/http-offsets/topics/offsets/consumer-offsets";
    let store = StoreConsumerOffset {
        consumer: Consumer::new(Identifier::numeric(CONSUMER_ID).expect("consumer id")),
        partition_id: Some(PARTITION_ID),
        offset: 1,
    };
    let response = http
        .client
        .put(http.url(offsets_path))
        .bearer_auth(&http.token)
        .json(&store)
        .send()
        .await
        .expect("store offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "store must commit"
    );

    let get_path = format!("{offsets_path}?consumer_id={CONSUMER_ID}&partition_id={PARTITION_ID}");
    let response = http
        .client
        .get(http.url(&get_path))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("get offset request");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "stored offset must be readable"
    );
    let info: ConsumerOffsetInfo = response.json().await.expect("decode ConsumerOffsetInfo");
    assert_eq!(info.stored_offset, 1, "stored offset must round trip");
    assert_eq!(info.partition_id, PARTITION_ID);

    let delete_path = format!("{offsets_path}/{CONSUMER_ID}?partition_id={PARTITION_ID}");
    let response = http
        .client
        .delete(http.url(&delete_path))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("delete offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "delete must commit"
    );

    let response = http
        .client
        .get(http.url(&get_path))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("get offset request after delete");
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "deleted offset must be gone"
    );
}

/// A DELETE of a never-stored consumer offset is denied by the partition
/// primary before consensus (`ReplyHeader.status`), so the route must answer
/// a typed 404 immediately - not wait out the 10s partition-write reply
/// timeout into a 504 - and the deny gate must not break a store-then-delete
/// of an offset that does exist.
#[iggy_harness]
async fn given_missing_consumer_offset_when_deleting_should_reject_404_fast(harness: &TestHarness) {
    /// Well under the partition-write reply timeout: a deny is a single
    /// round trip, a silent drop only answers at the full 10s.
    const DENY_LATENCY_BUDGET: Duration = Duration::from_secs(5);

    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-missing-offset", "offsets", 1)
        .await;

    let offsets_path = "/streams/http-missing-offset/topics/offsets/consumer-offsets";
    let delete_path = format!("{offsets_path}/{CONSUMER_ID}?partition_id={PARTITION_ID}");
    let started = Instant::now();
    let response = http
        .client
        .delete(http.url(&delete_path))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("delete missing offset request");
    let elapsed = started.elapsed();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "missing offset delete must reject 404"
    );
    assert!(
        elapsed < DENY_LATENCY_BUDGET,
        "deny must answer fast, not ride the reply timeout (took {elapsed:?})"
    );
    // The typed deny body, not the generic pre-dispatch not-found shape.
    let body: serde_json::Value = response.json().await.expect("decode error body");
    assert_eq!(
        body["code"], "consumer_offset_not_found",
        "deny must carry the typed error code, got {body}"
    );

    // The deny gate must not reject offsets that exist: store, then delete.
    let messages = vec![text_message(1, "offset-fodder".to_string())];
    let response = http
        .produce("http-missing-offset", "offsets", PARTITION_ID, messages)
        .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "produce must commit"
    );
    let store = StoreConsumerOffset {
        consumer: Consumer::new(Identifier::numeric(CONSUMER_ID).expect("consumer id")),
        partition_id: Some(PARTITION_ID),
        offset: 0,
    };
    let response = http
        .client
        .put(http.url(offsets_path))
        .bearer_auth(&http.token)
        .json(&store)
        .send()
        .await
        .expect("store offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "store must commit"
    );
    let response = http
        .client
        .delete(http.url(&delete_path))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("delete stored offset request");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "existing offset delete must commit"
    );
}

#[iggy_harness]
async fn given_oversized_body_when_producing_should_reject_413(harness: &TestHarness) {
    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-oversized", "big", 1)
        .await;

    // 3 MiB payload -> ~4 MiB JSON after base64: over the 2 MB body cap under
    // either decimal or binary megabyte semantics.
    let oversized = IggyMessage::builder()
        .payload(vec![0x42u8; 3 * 1024 * 1024].into())
        .build()
        .expect("message build");
    let response = http
        .produce("http-oversized", "big", PARTITION_ID, vec![oversized])
        .await;
    assert_eq!(
        response.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "oversized body must be rejected"
    );

    // The rejection must not poison the listener: a normal produce still commits.
    let normal = text_message(1, "small-after-large".to_string());
    let response = http
        .produce("http-oversized", "big", PARTITION_ID, vec![normal])
        .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "server must stay healthy after a 413"
    );
    let polled = http
        .poll("http-oversized", "big", PARTITION_ID, 0, 10)
        .await;
    assert_eq!(polled.messages.len(), 1, "normal produce must be pollable");
    assert_eq!(
        polled.messages[0].payload,
        bytes::Bytes::from("small-after-large")
    );
}

#[iggy_harness]
async fn given_ack_none_when_producing_should_return_202_and_commit(harness: &TestHarness) {
    let http = HttpSession::login_root(harness).await;
    http.create_stream_and_topic("http-ack-none", "fire", 1)
        .await;

    let message = text_message(1, "fire-and-forget".to_string());
    let response = http
        .produce_with_query(
            "http-ack-none",
            "fire",
            PARTITION_ID,
            vec![message],
            "?ack=none",
        )
        .await;
    assert_eq!(
        response.status(),
        StatusCode::ACCEPTED,
        "ack=none must answer before the commit"
    );
    assert_eq!(durability(&response), DURABILITY_NONE);

    // The commit still happens, just asynchronously: poll bounded until the
    // message becomes visible.
    let deadline = Instant::now() + ASYNC_COMMIT_TIMEOUT;
    loop {
        let polled = http
            .poll("http-ack-none", "fire", PARTITION_ID, 0, 10)
            .await;
        if !polled.messages.is_empty() {
            assert_eq!(polled.messages.len(), 1, "exactly one message was produced");
            assert_eq!(
                polled.messages[0].payload,
                bytes::Bytes::from("fire-and-forget"),
                "ack=none payload must round trip"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ack=none produce did not become pollable within {ASYNC_COMMIT_TIMEOUT:?}"
        );
        sleep(ASYNC_COMMIT_RETRY_INTERVAL).await;
    }
}

/// End-to-end RBAC proof: an ungranted user is 403 on a metadata read and on a
/// data-plane produce, root stays 200/201, and the auth-only cluster-metadata
/// route is never gated. Exercises the HTTP per-op gates (read + partition
/// write) and the in-apply control gate (user creation as root) in one path.
#[iggy_harness]
async fn given_ungranted_user_when_reading_or_producing_should_be_forbidden(harness: &TestHarness) {
    let root = HttpSession::login_root(harness).await;
    root.create_stream_and_topic("rbac-stream", "rbac-topic", 1)
        .await;
    // Root provisions a user with no permissions, then we log in as them.
    root.create_user("rbac-nobody", "rbac-nobody-pass").await;
    let nobody = root.login("rbac-nobody", "rbac-nobody-pass").await;

    // A metadata read: 403 for the ungranted user, 200 for root.
    assert_eq!(
        nobody.get("/streams").await.status(),
        StatusCode::FORBIDDEN,
        "ungranted read must be 403"
    );
    assert_eq!(
        root.get("/streams").await.status(),
        StatusCode::OK,
        "root read must be 200"
    );

    // A data-plane produce: 403 for the ungranted user, 201 for root.
    let denied = nobody
        .produce(
            "rbac-stream",
            "rbac-topic",
            PARTITION_ID,
            vec![text_message(1, "denied".to_string())],
        )
        .await;
    assert_eq!(
        denied.status(),
        StatusCode::FORBIDDEN,
        "ungranted produce must be 403"
    );
    let allowed = root
        .produce(
            "rbac-stream",
            "rbac-topic",
            PARTITION_ID,
            vec![text_message(2, "allowed".to_string())],
        )
        .await;
    assert_eq!(
        allowed.status(),
        StatusCode::CREATED,
        "root produce must commit"
    );

    // The auth-only cluster-metadata route carries no rule: any authenticated
    // user reaches it, ungranted included.
    assert_eq!(
        nobody.get("/cluster/metadata").await.status(),
        StatusCode::OK,
        "cluster metadata is auth-only, never RBAC-gated"
    );
}
