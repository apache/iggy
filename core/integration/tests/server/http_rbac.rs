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

//! End-to-end RBAC matrix for server-ng, exercised over raw `reqwest` against
//! the shard-0 HTTP listener plus one SDK/TCP case. Proves the whole
//! authorization surface a client can reach:
//!
//! - dispatch-time per-op gates on reads (`get_streams`/`get_stream`/... ) and
//!   the data plane (produce, poll, consumer offsets) -> HTTP 403;
//! - the in-apply control gate whose `Unauthorized` denial rides the metadata
//!   result section back to an HTTP 4xx with a typed error body;
//! - business-rule rejections (`CannotDeleteUser`/`CannotChangePermissions`)
//!   that fire only AFTER the RBAC gate admits the acting user;
//! - the denormalized "all-streams" indexes populated from a user's GLOBAL
//!   `poll_messages`/`send_messages` bits (the trap case);
//! - live revocation visibility (re-permission + user delete) on a single node;
//! - self-scoped PAT ops that bypass RBAC, and PAT-bound identity;
//! - the TCP typed-`Unauthorized` deny surfaced through the SDK.
//!
//! Personas are provisioned by root over HTTP; each test owns its own server
//! (one spawn per `#[iggy_harness]` fn) so the mutating cases stay isolated.

use iggy::prelude::*;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{GlobalPermissions, IggyMessagesBatch, Permissions, StreamPermissions};
use integration::harness::TestHarness;
use integration::iggy_harness;
use reqwest::{Response, StatusCode};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
// Explicit consumer id shared by the offset body and query so both address the
// same offset-table key (`Consumer::default()` carries numeric id 0, not 1).
const CONSUMER_ID: u32 = 1;

// `IggyError` discriminants (== the numeric `id` in the JSON error body).
const UNAUTHORIZED_ID: u64 = 41;
const INVALID_CREDENTIALS_ID: u64 = 42;
const CANNOT_DELETE_USER_ID: u64 = 48;
const CANNOT_CHANGE_PERMISSIONS_ID: u64 = 49;
// The apply's `ChangePasswordResult::UserNotFound` maps to `ResourceNotFound`.
const RESOURCE_NOT_FOUND_ID: u64 = 20;
// Snake-case `IggyError` names (== the `code` string in the JSON error body).
const UNAUTHORIZED_CODE: &str = "unauthorized";
const INVALID_CREDENTIALS_CODE: &str = "invalid_credentials";
const CANNOT_DELETE_USER_CODE: &str = "cannot_delete_user";
const CANNOT_CHANGE_PERMISSIONS_CODE: &str = "cannot_change_permissions";
const RESOURCE_NOT_FOUND_CODE: &str = "resource_not_found";

/// Root is the first user (slab id 0) and is undeletable / permission-locked.
const ROOT_USER_PATH: &str = "0";

/// The harness readiness gate waits for the dumped runtime config, which the
/// server writes before binding the HTTP listener; a bounded login retry
/// bridges that gap plus single-node consensus warmup.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15);
const LOGIN_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One authenticated HTTP session: a `reqwest` client plus the bearer to send.
/// The bearer is either a JWT (from a login) or a raw personal access token
/// (server-ng resolves either on the `Authorization: Bearer` header).
struct Client {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl Client {
    /// Log in as root, retrying while the listener binds and consensus warms up.
    async fn login_root(harness: &TestHarness) -> Self {
        let addr = harness
            .server()
            .http_addr()
            .expect("HTTP transport not configured on test server");
        let base_url = format!("http://{addr}");
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("build reqwest client");

        let body = json!({
            "username": DEFAULT_ROOT_USERNAME,
            "password": DEFAULT_ROOT_PASSWORD,
        });
        let deadline = Instant::now() + LOGIN_TIMEOUT;
        loop {
            let attempt = http
                .post(format!("{base_url}/users/login"))
                .json(&body)
                .send()
                .await;
            match attempt {
                Ok(response) if response.status().is_success() => {
                    return Self {
                        http,
                        base_url,
                        token: access_token(response).await,
                    };
                }
                // Listener not bound yet or consensus still warming up; any
                // non-5xx rejection (e.g. 401) is a real failure.
                Ok(response) if response.status().is_server_error() => {}
                Ok(response) => panic!("root login rejected: {}", response.status()),
                Err(error) if error.is_connect() => {}
                Err(error) => panic!("root login request failed: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "HTTP root login did not succeed within {LOGIN_TIMEOUT:?}"
            );
            sleep(LOGIN_RETRY_INTERVAL).await;
        }
    }

    /// Log in as an already-created user; single attempt (the server is warm
    /// once root exists). Reuses this session's client and base URL.
    async fn login(&self, username: &str, password: &str) -> Self {
        let body = json!({ "username": username, "password": password });
        let response = self
            .http
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
        Self {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            token: access_token(response).await,
        }
    }

    /// A sibling session authenticating with an arbitrary bearer (a raw PAT),
    /// reusing this session's client and base URL.
    fn with_bearer(&self, token: String) -> Self {
        Self {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            token,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn get(&self, path: &str) -> Response {
        self.http
            .get(self.url(path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("get request")
    }

    /// Unauthenticated GET (no bearer) for the public `/ping` probe.
    async fn get_anonymous(&self, path: &str) -> Response {
        self.http
            .get(self.url(path))
            .send()
            .await
            .expect("anonymous get request")
    }

    async fn post_json(&self, path: &str, body: &Value) -> Response {
        self.http
            .post(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .expect("post request")
    }

    async fn put_json(&self, path: &str, body: &Value) -> Response {
        self.http
            .put(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .expect("put request")
    }

    async fn delete(&self, path: &str) -> Response {
        self.http
            .delete(self.url(path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("delete request")
    }

    /// Create a stream and return its server-assigned numeric id, which is both
    /// the id clients address and the key the permissioner uses for per-stream
    /// grants (`resolve_stream_id` maps name -> that same id).
    async fn create_stream(&self, name: &str) -> u64 {
        let response = self
            .post_json(
                "/streams",
                &serde_json::to_value(CreateStream { name: name.into() }).unwrap(),
            )
            .await;
        assert!(
            response.status().is_success(),
            "root create stream {name} failed: {}",
            response.status()
        );
        let details: Value = response.json().await.expect("stream details json");
        details["id"]
            .as_u64()
            .expect("stream id in create response")
    }

    async fn create_topic(&self, stream: &str, topic: &str, partitions: u32) {
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
            .post_json(
                &format!("/streams/{stream}/topics"),
                &serde_json::to_value(command).unwrap(),
            )
            .await;
        assert!(
            response.status().is_success(),
            "root create topic {topic} failed: {}",
            response.status()
        );
    }

    /// Create a user with the given permissions (`Value::Null` = no grants).
    async fn create_user(&self, username: &str, password: &str, permissions: Value) -> Response {
        self.post_json(
            "/users",
            &json!({
                "username": username,
                "password": password,
                "status": "active",
                "permissions": permissions,
            }),
        )
        .await
    }

    /// Change a user's password via `PUT /users/{user_id}/password`.
    async fn change_password(&self, user_id: &str, current: &str, new: &str) -> Response {
        self.put_json(
            &format!("/users/{user_id}/password"),
            &json!({ "current_password": current, "new_password": new }),
        )
        .await
    }

    /// Anonymous login attempt returning the raw response, for exercising a
    /// rejected login (the asserting [`Client::login`] cannot express failure).
    async fn login_attempt(&self, username: &str, password: &str) -> Response {
        self.http
            .post(self.url("/users/login"))
            .json(&json!({ "username": username, "password": password }))
            .send()
            .await
            .expect("login request")
    }

    async fn produce(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<IggyMessage>,
    ) -> Response {
        let body = SendMessages {
            metadata_length: 0,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::partition_id(partition_id),
            batch: IggyMessagesBatch::from(&messages),
        };
        self.http
            .post(self.url(&format!("/streams/{stream}/topics/{topic}/messages")))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .expect("produce request")
    }

    async fn poll(&self, stream: &str, topic: &str, partition_id: u32) -> Response {
        let path = format!(
            "/streams/{stream}/topics/{topic}/messages\
             ?consumer_id={CONSUMER_ID}&partition_id={partition_id}\
             &kind=offset&value=0&count=10&auto_commit=false"
        );
        self.get(&path).await
    }

    async fn store_offset(&self, stream: &str, topic: &str) -> Response {
        let store = StoreConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(CONSUMER_ID).expect("consumer id")),
            partition_id: Some(PARTITION_ID),
            offset: 0,
        };
        self.put_json(
            &format!("/streams/{stream}/topics/{topic}/consumer-offsets"),
            &serde_json::to_value(store).unwrap(),
        )
        .await
    }

    async fn get_offset(&self, stream: &str, topic: &str) -> Response {
        self.get(&format!(
            "/streams/{stream}/topics/{topic}/consumer-offsets\
             ?consumer_id={CONSUMER_ID}&partition_id={PARTITION_ID}"
        ))
        .await
    }

    async fn delete_offset(&self, stream: &str, topic: &str) -> Response {
        self.delete(&format!(
            "/streams/{stream}/topics/{topic}/consumer-offsets/{CONSUMER_ID}\
             ?partition_id={PARTITION_ID}"
        ))
        .await
    }
}

/// Extract the JWT from a successful login / PAT-login response.
async fn access_token(response: Response) -> String {
    let identity: IdentityInfo = response.json().await.expect("decode IdentityInfo");
    identity
        .access_token
        .expect("login must return an access token")
        .token
}

fn text_message(id: u128, payload: &str) -> IggyMessage {
    IggyMessage::builder()
        .id(id)
        .payload(payload.to_string().into())
        .build()
        .expect("message build")
}

// --- permission-body builders (the legacy-parity common types) --------------

/// No permissions at all: the permissioner holds no entry, so every rule denies.
fn no_permissions() -> Value {
    Value::Null
}

/// Global-only grants (no per-stream map).
fn global_permissions(global: GlobalPermissions) -> Value {
    serde_json::to_value(Permissions {
        global,
        streams: None,
    })
    .expect("serialize permissions")
}

/// A single per-stream grant keyed by the stream's numeric id, no global grants.
fn stream_scoped_permissions(stream_id: u64, stream: StreamPermissions) -> Value {
    serde_json::to_value(Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(stream_id as usize, stream)])),
    })
    .expect("serialize permissions")
}

/// Streams S1/S2 (each with one topic) provisioned by root for every test.
struct Fixture {
    s1: &'static str,
    s2: &'static str,
    // S1's id keys bob's per-stream grant; S2 is only ever addressed by name.
    s1_id: u64,
    topic: &'static str,
}

/// Root login + the shared S1/S2 + topic fixture. Returns the root session and
/// S1's id for keying per-stream grants.
async fn setup(harness: &TestHarness) -> (Client, Fixture) {
    let root = Client::login_root(harness).await;
    let (s1, s2, topic) = ("rbac-s1", "rbac-s2", "rbac-t");
    let s1_id = root.create_stream(s1).await;
    root.create_topic(s1, topic, 1).await;
    root.create_stream(s2).await;
    root.create_topic(s2, topic, 1).await;
    (
        root,
        Fixture {
            s1,
            s2,
            s1_id,
            topic,
        },
    )
}

/// Assert a JSON error body carries the expected typed `id` + `code`. Proves the
/// error crossed the wire as a real `IggyError` code, not a bare status.
async fn assert_error_body(response: Response, expected_id: u64, expected_code: &str, ctx: &str) {
    let body: Value = response.json().await.expect("error body json");
    assert_eq!(
        body["id"].as_u64(),
        Some(expected_id),
        "{ctx}: error id (body={body})"
    );
    assert_eq!(
        body["code"].as_str(),
        Some(expected_code),
        "{ctx}: error code (body={body})"
    );
}

// === Case family 1: alice, NO permissions ===================================

#[iggy_harness]
async fn given_ungranted_user_when_touching_any_surface_should_deny_except_self_scoped(
    harness: &TestHarness,
) {
    let (root, fx) = setup(harness).await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;
    let alice = root.login("alice", "alice-pass").await;

    // Reads: every metadata read is 403 for the ungranted user.
    let reads = [
        ("GET /streams", "/streams".to_string()),
        ("GET /streams/{s}", format!("/streams/{}", fx.s1)),
        (
            "GET /streams/{s}/topics/{t}",
            format!("/streams/{}/topics/{}", fx.s1, fx.topic),
        ),
        ("GET /users", "/users".to_string()),
        ("GET /stats", "/stats".to_string()),
        ("GET /clients", "/clients".to_string()),
    ];
    for (label, path) in &reads {
        assert_eq!(
            alice.get(path).await.status(),
            StatusCode::FORBIDDEN,
            "alice {label} must be 403"
        );
    }

    // Data plane: produce, poll, and all three consumer-offset ops are 403.
    assert_eq!(
        alice
            .produce(fx.s1, fx.topic, PARTITION_ID, vec![text_message(1, "x")])
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "alice produce must be 403"
    );
    assert_eq!(
        alice.poll(fx.s1, fx.topic, PARTITION_ID).await.status(),
        StatusCode::FORBIDDEN,
        "alice poll must be 403"
    );
    assert_eq!(
        alice.store_offset(fx.s1, fx.topic).await.status(),
        StatusCode::FORBIDDEN,
        "alice store-offset must be 403"
    );
    assert_eq!(
        alice.get_offset(fx.s1, fx.topic).await.status(),
        StatusCode::FORBIDDEN,
        "alice get-offset must be 403"
    );
    assert_eq!(
        alice.delete_offset(fx.s1, fx.topic).await.status(),
        StatusCode::FORBIDDEN,
        "alice delete-offset must be 403"
    );

    // Control write via the in-apply gate: the denial rides the result section
    // back as a typed `Unauthorized` body under a 403.
    let denied = alice
        .post_json("/streams", &json!({ "name": "alice-stream" }))
        .await;
    assert_eq!(
        denied.status(),
        StatusCode::FORBIDDEN,
        "alice create-stream must be 403"
    );
    assert_error_body(
        denied,
        UNAUTHORIZED_ID,
        UNAUTHORIZED_CODE,
        "alice create-stream",
    )
    .await;

    // Self-scoped ops bypass RBAC: alice manages her own PAT and reads herself.
    let pat = alice
        .post_json(
            "/personal-access-tokens",
            &json!({ "name": "alice-pat", "expiry": 0 }),
        )
        .await;
    assert_eq!(
        pat.status(),
        StatusCode::OK,
        "alice create own PAT must be 200"
    );
    assert_eq!(
        alice
            .delete("/personal-access-tokens/alice-pat")
            .await
            .status(),
        StatusCode::NO_CONTENT,
        "alice delete own PAT must be 204"
    );
    let me = alice.get("/users/me").await;
    assert_eq!(
        me.status(),
        StatusCode::OK,
        "alice GET /users/me must be 200"
    );
    let me_body: Value = me.json().await.expect("users/me body must be JSON");
    assert_eq!(
        me_body["username"], "alice",
        "the me alias must resolve to the caller's own user"
    );

    // Auth-only cluster metadata (no rule) and the public ping.
    assert_eq!(
        alice.get("/cluster/metadata").await.status(),
        StatusCode::OK,
        "cluster metadata is auth-only, never RBAC-gated"
    );
    assert_eq!(
        alice.get_anonymous("/ping").await.status(),
        StatusCode::OK,
        "ping is public"
    );
}

// === Case family 2: bob, per-stream grant on S1 only ========================

#[iggy_harness]
async fn given_stream_scoped_user_when_crossing_the_stream_boundary_should_deny(
    harness: &TestHarness,
) {
    let (root, fx) = setup(harness).await;
    // read_stream + poll_messages on S1 only; no send, no global read_streams.
    let permissions = stream_scoped_permissions(
        fx.s1_id,
        StreamPermissions {
            read_stream: true,
            poll_messages: true,
            ..Default::default()
        },
    );
    root.create_user("bob", "bob-pass", permissions).await;
    let bob = root.login("bob", "bob-pass").await;

    assert_eq!(
        bob.poll(fx.s1, fx.topic, PARTITION_ID).await.status(),
        StatusCode::OK,
        "bob poll on granted S1 must be 200"
    );
    assert_eq!(
        bob.poll(fx.s2, fx.topic, PARTITION_ID).await.status(),
        StatusCode::FORBIDDEN,
        "bob poll on ungranted S2 must be 403 (cross-stream deny)"
    );
    assert_eq!(
        bob.produce(fx.s1, fx.topic, PARTITION_ID, vec![text_message(1, "x")])
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "bob produce on S1 must be 403 (read grant does not imply send)"
    );
    assert_eq!(
        bob.get(&format!("/streams/{}", fx.s1)).await.status(),
        StatusCode::OK,
        "bob GET single granted stream S1 must be 200"
    );
    assert_eq!(
        bob.get("/streams").await.status(),
        StatusCode::FORBIDDEN,
        "bob GET /streams must be 403 (no global read_streams)"
    );
}

// === Case family 3: carol, GLOBAL message bits, ZERO per-stream (the trap) ===

#[iggy_harness]
async fn given_global_message_bits_without_stream_entries_when_polling_or_producing_should_allow(
    harness: &TestHarness,
) {
    let (root, fx) = setup(harness).await;
    // Global poll+send only; the all-streams indexes are the ONLY thing that can
    // admit her (her global bits set no read_streams/manage_*).
    let permissions = global_permissions(GlobalPermissions {
        poll_messages: true,
        send_messages: true,
        ..Default::default()
    });
    root.create_user("carol", "carol-pass", permissions).await;
    let carol = root.login("carol", "carol-pass").await;

    for (label, stream) in [("S1", fx.s1), ("S2", fx.s2)] {
        assert_eq!(
            carol
                .produce(stream, fx.topic, PARTITION_ID, vec![text_message(1, "x")])
                .await
                .status(),
            StatusCode::CREATED,
            "carol produce on {label} must be 201 (global send_messages index)"
        );
        assert_eq!(
            carol.poll(stream, fx.topic, PARTITION_ID).await.status(),
            StatusCode::OK,
            "carol poll on {label} must be 200 (global poll_messages index)"
        );
    }
}

// === Case family 4: revocation takes effect immediately (single node) =======

#[iggy_harness]
async fn given_permission_revocation_when_reapplied_should_deny_next_request(
    harness: &TestHarness,
) {
    let (root, fx) = setup(harness).await;

    // carol: global poll+send. bob: global read_streams+poll. Both start allowed.
    root.create_user(
        "carol",
        "carol-pass",
        global_permissions(GlobalPermissions {
            poll_messages: true,
            send_messages: true,
            ..Default::default()
        }),
    )
    .await;
    root.create_user(
        "bob",
        "bob-pass",
        global_permissions(GlobalPermissions {
            read_streams: true,
            poll_messages: true,
            ..Default::default()
        }),
    )
    .await;
    let carol = root.login("carol", "carol-pass").await;
    let bob = root.login("bob", "bob-pass").await;

    assert_eq!(
        carol
            .produce(fx.s1, fx.topic, PARTITION_ID, vec![text_message(1, "x")])
            .await
            .status(),
        StatusCode::CREATED,
        "carol produce must be 201 before revocation"
    );
    assert_eq!(
        bob.get("/streams").await.status(),
        StatusCode::OK,
        "bob read must be 200 before deletion"
    );

    // Strip carol's send (keep poll): her all-streams send index entry clears.
    let reperm = root
        .put_json(
            "/users/carol/permissions",
            &json!({
                "permissions": global_permissions(GlobalPermissions {
                    poll_messages: true,
                    send_messages: false,
                    ..Default::default()
                })
            }),
        )
        .await;
    assert_eq!(
        reperm.status(),
        StatusCode::NO_CONTENT,
        "root re-permission must be 204"
    );
    assert_eq!(
        carol
            .produce(fx.s1, fx.topic, PARTITION_ID, vec![text_message(2, "y")])
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "carol produce must be 403 after send stripped"
    );
    assert_eq!(
        carol.poll(fx.s1, fx.topic, PARTITION_ID).await.status(),
        StatusCode::OK,
        "carol poll must still be 200 (poll grant retained)"
    );

    // Delete bob entirely: his permissioner indexes clear though his JWT is live.
    assert_eq!(
        root.delete("/users/bob").await.status(),
        StatusCode::NO_CONTENT,
        "root delete bob must be 204"
    );
    assert_eq!(
        bob.get("/streams").await.status(),
        StatusCode::FORBIDDEN,
        "deleted bob read must be 403 (indexes cleared)"
    );
    assert_eq!(
        bob.poll(fx.s1, fx.topic, PARTITION_ID).await.status(),
        StatusCode::FORBIDDEN,
        "deleted bob poll must be 403 (indexes cleared)"
    );
}

// === Case family 5: dave, global manage_users admin (non-root) ==============

#[iggy_harness]
async fn given_non_root_manage_users_admin_when_managing_should_honor_business_rules(
    harness: &TestHarness,
) {
    let (root, _fx) = setup(harness).await;
    root.create_user(
        "dave",
        "dave-pass",
        global_permissions(GlobalPermissions {
            manage_users: true,
            ..Default::default()
        }),
    )
    .await;
    let dave = root.login("dave", "dave-pass").await;

    // F8: a non-root admin may create users (in-apply gate admits manage_users).
    let created = dave
        .create_user("dave-made", "made-pass", no_permissions())
        .await;
    assert_eq!(
        created.status(),
        StatusCode::OK,
        "dave create user must be 200 (non-root manage_users admin)"
    );

    // RBAC admits dave, then the business invariant rejects touching root. The
    // typed code rides the body; the status is whatever the IggyError map yields
    // (400 for these, not 403).
    let delete_root = dave.delete(&format!("/users/{ROOT_USER_PATH}")).await;
    assert_eq!(
        delete_root.status(),
        StatusCode::BAD_REQUEST,
        "dave delete root status"
    );
    assert_error_body(
        delete_root,
        CANNOT_DELETE_USER_ID,
        CANNOT_DELETE_USER_CODE,
        "dave delete root",
    )
    .await;

    let change_root = dave
        .put_json(
            &format!("/users/{ROOT_USER_PATH}/permissions"),
            &json!({ "permissions": no_permissions() }),
        )
        .await;
    assert_eq!(
        change_root.status(),
        StatusCode::BAD_REQUEST,
        "dave change root permissions status"
    );
    assert_error_body(
        change_root,
        CANNOT_CHANGE_PERMISSIONS_ID,
        CANNOT_CHANGE_PERMISSIONS_CODE,
        "dave change root permissions",
    )
    .await;

    // dave holds no stream grant: creating a stream is denied by the in-apply gate.
    let denied = dave
        .post_json("/streams", &json!({ "name": "dave-stream" }))
        .await;
    assert_eq!(
        denied.status(),
        StatusCode::FORBIDDEN,
        "dave create stream must be 403 (no manage_streams)"
    );
    assert_error_body(
        denied,
        UNAUTHORIZED_ID,
        UNAUTHORIZED_CODE,
        "dave create stream",
    )
    .await;
}

// === Case family 6: root full-surface canary ================================

#[iggy_harness]
async fn given_root_when_exercising_the_full_surface_should_allow(harness: &TestHarness) {
    let root = Client::login_root(harness).await;
    let root_id = root.create_stream("root-canary").await;
    assert!(root_id < u64::MAX, "root create stream must return an id");
    root.create_topic("root-canary", "canary-topic", 1).await;

    assert_eq!(
        root.produce(
            "root-canary",
            "canary-topic",
            PARTITION_ID,
            vec![text_message(1, "root-msg")],
        )
        .await
        .status(),
        StatusCode::CREATED,
        "root produce must be 201"
    );
    assert_eq!(
        root.poll("root-canary", "canary-topic", PARTITION_ID)
            .await
            .status(),
        StatusCode::OK,
        "root poll must be 200"
    );
    assert_eq!(
        root.get("/stats").await.status(),
        StatusCode::OK,
        "root GET /stats must be 200"
    );
}

// === Case family 7: PAT-bound identity ======================================

#[iggy_harness]
async fn given_personal_access_token_when_authenticating_should_bind_owner_rules(
    harness: &TestHarness,
) {
    let (root, fx) = setup(harness).await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;
    let alice = root.login("alice", "alice-pass").await;

    // alice mints a PAT (self-scoped) and authenticates WITH it (raw PAT bearer).
    let pat_response = alice
        .post_json(
            "/personal-access-tokens",
            &json!({ "name": "alice-id", "expiry": 0 }),
        )
        .await;
    assert_eq!(
        pat_response.status(),
        StatusCode::OK,
        "alice create PAT must be 200"
    );
    let raw: Value = pat_response.json().await.expect("PAT json");
    let token = raw["token"].as_str().expect("raw PAT token").to_string();
    let via_pat = alice.with_bearer(token);

    // The PAT resolves to alice's user id, so it inherits her (empty) rules: the
    // same denials she gets via her JWT.
    assert_eq!(
        via_pat.get("/streams").await.status(),
        StatusCode::FORBIDDEN,
        "PAT-authenticated read must be 403 (bound to alice's rules)"
    );
    assert_eq!(
        via_pat
            .produce(fx.s1, fx.topic, PARTITION_ID, vec![text_message(1, "x")])
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "PAT-authenticated produce must be 403 (bound to alice's rules)"
    );
}

// === Case family 8: TCP typed Unauthorized through the SDK ==================

#[iggy_harness(server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true))]
async fn given_non_root_tcp_session_when_polling_denied_should_surface_typed_unauthorized(
    harness: &TestHarness,
) {
    // Provision alice + the stream over HTTP-as-root (same single-node metadata
    // STM the TCP dispatch reads).
    let (root, fx) = setup(harness).await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;

    // A fresh TCP SDK client, logged in as the non-root alice.
    let client = harness.tcp_new_client().await.expect("tcp client");
    client
        .login_user("alice", "alice-pass")
        .await
        .expect("alice tcp login");

    let stream_id = Identifier::from_str_value(fx.s1).expect("stream identifier");
    let topic_id = Identifier::from_str_value(fx.topic).expect("topic identifier");
    let result = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await;

    assert!(
        matches!(&result, Err(error) if error.as_code() == IggyError::Unauthorized.as_code()),
        "non-root TCP poll must surface Err(Unauthorized) via the reply status peek, got {result:?}"
    );
}

// === Case family 9: PAT list is caller-scoped ================================

/// Listing is self-scoped like the other PAT ops: any authenticated user sees
/// exactly their own tokens (name + expiry, name-sorted), never another
/// user's. Asserted over HTTP and via the SDK on TCP, which share the read
/// builder.
#[iggy_harness]
async fn given_two_users_with_tokens_when_listing_should_return_only_callers(
    harness: &TestHarness,
) {
    let root = Client::login_root(harness).await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;
    root.create_user("bob", "bob-pass", no_permissions()).await;
    let alice = root.login("alice", "alice-pass").await;
    let bob = root.login("bob", "bob-pass").await;

    // Created in reverse name order to prove the listing sorts by name.
    for name in ["alice-zeta", "alice-alpha"] {
        let created = alice
            .post_json(
                "/personal-access-tokens",
                &json!({ "name": name, "expiry": 0 }),
            )
            .await;
        assert_eq!(
            created.status(),
            StatusCode::OK,
            "alice create PAT {name} must be 200"
        );
    }
    let created = bob
        .post_json(
            "/personal-access-tokens",
            &json!({ "name": "bob-pat", "expiry": 0 }),
        )
        .await;
    assert_eq!(
        created.status(),
        StatusCode::OK,
        "bob create PAT must be 200"
    );

    assert_eq!(
        pat_names(alice.get("/personal-access-tokens").await).await,
        ["alice-alpha", "alice-zeta"],
        "alice must list exactly her own tokens, name-sorted"
    );
    assert_eq!(
        pat_names(bob.get("/personal-access-tokens").await).await,
        ["bob-pat"],
        "bob must never see alice's tokens"
    );

    // Same caller-scoped view over native TCP through the SDK.
    let client = harness.tcp_new_client().await.expect("tcp client");
    client
        .login_user("alice", "alice-pass")
        .await
        .expect("alice tcp login");
    let tokens = client
        .get_personal_access_tokens()
        .await
        .expect("tcp PAT list");
    let tcp_names: Vec<&str> = tokens.iter().map(|token| token.name.as_str()).collect();
    assert_eq!(
        tcp_names,
        ["alice-alpha", "alice-zeta"],
        "TCP list must match the HTTP caller-scoped view"
    );
}

/// Decode a PAT-list response body into its token names, asserting 200 first.
async fn pat_names(response: Response) -> Vec<String> {
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PAT list must be 200 for an authenticated caller"
    );
    let body: Value = response.json().await.expect("PAT list body must be JSON");
    body.as_array()
        .expect("PAT list must be a JSON array")
        .iter()
        .map(|token| {
            token["name"]
                .as_str()
                .expect("every token must carry a name")
                .to_owned()
        })
        .collect()
}

/// The current-password check gates a self-service rotation: a wrong current
/// password is rejected pre-consensus as 400 `InvalidCredentials` (never
/// reaching the replicated apply), and only the correct one rotates the
/// credential so the old password stops working and the new one starts.
#[iggy_harness]
async fn given_own_password_when_current_wrong_should_reject_and_correct_should_rotate(
    harness: &TestHarness,
) {
    let root = Client::login_root(harness).await;
    root.create_user("carol", "carol-pass-1", no_permissions())
        .await;
    let carol = root.login("carol", "carol-pass-1").await;

    let wrong = carol
        .change_password("carol", "not-my-password", "carol-pass-2")
        .await;
    assert_eq!(
        wrong.status(),
        StatusCode::BAD_REQUEST,
        "wrong current password must be 400"
    );
    assert_error_body(
        wrong,
        INVALID_CREDENTIALS_ID,
        INVALID_CREDENTIALS_CODE,
        "carol change-password wrong current",
    )
    .await;

    let correct = carol
        .change_password("carol", "carol-pass-1", "carol-pass-2")
        .await;
    assert_eq!(
        correct.status(),
        StatusCode::NO_CONTENT,
        "correct current password must be 204"
    );

    assert!(
        !carol
            .login_attempt("carol", "carol-pass-1")
            .await
            .status()
            .is_success(),
        "old password must stop working after a successful rotation"
    );
    assert!(
        carol
            .login_attempt("carol", "carol-pass-2")
            .await
            .status()
            .is_success(),
        "new password must work after a successful rotation"
    );
}

/// Admin-initiated change (target != caller): the current password is checked
/// against the TARGET's stored hash, so a wrong one is 400 `InvalidCredentials`;
/// a target that does not resolve is left to the replicated apply, which
/// answers 404 `UserNotFound` (the plaintext is still stripped + hashed first).
#[iggy_harness]
async fn given_admin_change_password_when_target_current_wrong_or_missing_should_reject(
    harness: &TestHarness,
) {
    let root = Client::login_root(harness).await;
    root.create_user("dave", "dave-pass", no_permissions())
        .await;

    let wrong = root
        .change_password("dave", "not-daves-password", "new-dave-pass")
        .await;
    assert_eq!(
        wrong.status(),
        StatusCode::BAD_REQUEST,
        "admin change with the wrong target current password must be 400"
    );
    assert_error_body(
        wrong,
        INVALID_CREDENTIALS_ID,
        INVALID_CREDENTIALS_CODE,
        "admin change dave wrong current",
    )
    .await;

    let missing = root
        .change_password("ghost-does-not-exist", "whatever", "whatever-else")
        .await;
    assert_eq!(
        missing.status(),
        StatusCode::NOT_FOUND,
        "change for an unknown target must be 404 from the apply"
    );
    assert_error_body(
        missing,
        RESOURCE_NOT_FOUND_ID,
        RESOURCE_NOT_FOUND_CODE,
        "admin change missing target",
    )
    .await;
}
