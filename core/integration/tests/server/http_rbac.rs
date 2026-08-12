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

//! HTTP wire-contract residue for server-ng's shard-0 REST listener. The RBAC
//! authorization matrix itself (who may do what, over every transport) lives in
//! the cross-transport `permissions_scenario` suite; this file keeps only what
//! the SDK abstracts away and only raw HTTP can show:
//!
//! - exact HTTP status codes paired with the typed `{id, code}` JSON error body
//!   (the SDK collapses every non-401/403/404 status into a generic error);
//! - the SDK-hidden success codes (produce 201, reads 200, mutations 204, and
//!   the server-assigned id in a 200 create-stream body);
//! - HTTP-only route shapes: the `/users/me` self alias, the caller-scoped
//!   name-sorted PAT-list array, auth-only cluster metadata, and the public ping;
//! - the change-password status/body shape: a wrong current password renders a
//!   typed 400 `InvalidCredentials` and a missing target a 404 `ResourceNotFound`,
//!   both bodies the SDK hides. Change-password commits over every transport, so
//!   its authz is covered in `permissions_scenario`; only the wire shape is here.
//!
//! Each test owns its own server (one spawn per `#[iggy_harness]` fn).

use crate::server::http_client::HttpClient;
use iggy::prelude::*;
use iggy_common::IggyMessagesBatch;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use integration::iggy_harness;
use reqwest::{Response, StatusCode};
use serde_json::{Value, json};

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
// Explicit consumer id in the poll query (`Consumer::default()` is numeric 0).
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

/// Wire-contract request shapes layered on the shared [`HttpClient`]: this suite
/// pins raw HTTP status codes and typed `{id, code}` error bodies, so its
/// helpers return the raw [`Response`] for the caller to assert on.
trait ClientExt {
    async fn create_stream(&self, name: &str) -> u64;
    async fn create_topic(&self, stream: &str, topic: &str, partitions: u32);
    async fn create_user(&self, username: &str, password: &str, permissions: Value) -> Response;
    async fn change_password(&self, user_id: &str, current: &str, new: &str) -> Response;
    async fn login_attempt(&self, username: &str, password: &str) -> Response;
    async fn produce(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<IggyMessage>,
    ) -> Response;
    async fn poll(&self, stream: &str, topic: &str, partition_id: u32) -> Response;
}

impl ClientExt for HttpClient {
    /// Create a stream and return its server-assigned numeric id (proves the
    /// SDK-hidden `id` field of a 200 create-stream body).
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
    /// rejected login (the asserting [`HttpClient::login`] cannot express failure).
    async fn login_attempt(&self, username: &str, password: &str) -> Response {
        self.client
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
        self.client
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
}

fn text_message(id: u128, payload: &str) -> IggyMessage {
    IggyMessage::builder()
        .id(id)
        .payload(payload.to_string().into())
        .build()
        .expect("message build")
}

/// No permissions at all: the permissioner holds no entry, so every rule denies.
fn no_permissions() -> Value {
    Value::Null
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

// === Denials carry the exact HTTP status + typed JSON error body ============

/// The SDK reconstructs only 401/403/404 as typed errors and collapses every
/// other status; raw HTTP is the only place the paired status + `{id, code}`
/// body is observable. Covers both denial gates and both root-protection rules.
#[iggy_harness]
async fn given_http_requests_when_denied_should_carry_typed_status_and_body(harness: &TestHarness) {
    let root = HttpClient::login_root(harness).await;
    // A populated stream so the read gate denies over a real listing surface.
    root.create_stream("rbac-s1").await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;
    let alice = root.login("alice", "alice-pass").await;

    // Dispatch read gate: an ungranted read is 403 with a typed Unauthorized body.
    let read = alice.get("/streams").await;
    assert_eq!(
        read.status(),
        StatusCode::FORBIDDEN,
        "alice GET /streams must be 403"
    );
    assert_error_body(
        read,
        UNAUTHORIZED_ID,
        UNAUTHORIZED_CODE,
        "alice GET /streams",
    )
    .await;

    // In-apply control gate: the denial rides the metadata result section back as
    // a typed Unauthorized body, still under a 403.
    let create = alice
        .post_json("/streams", &json!({ "name": "alice-stream" }))
        .await;
    assert_eq!(
        create.status(),
        StatusCode::FORBIDDEN,
        "alice POST /streams must be 403"
    );
    assert_error_body(
        create,
        UNAUTHORIZED_ID,
        UNAUTHORIZED_CODE,
        "alice POST /streams",
    )
    .await;

    // Business-rule rejections fire in the STM apply AFTER RBAC admits root
    // (both guards are target-based on slab id 0). The typed code rides the body
    // and the `IggyError` map yields 400, not 403.
    let delete_root = root.delete(&format!("/users/{ROOT_USER_PATH}")).await;
    assert_eq!(
        delete_root.status(),
        StatusCode::BAD_REQUEST,
        "delete root status"
    );
    assert_error_body(
        delete_root,
        CANNOT_DELETE_USER_ID,
        CANNOT_DELETE_USER_CODE,
        "delete root",
    )
    .await;

    let change_root = root
        .put_json(
            &format!("/users/{ROOT_USER_PATH}/permissions"),
            &json!({ "permissions": no_permissions() }),
        )
        .await;
    assert_eq!(
        change_root.status(),
        StatusCode::BAD_REQUEST,
        "re-permission root status"
    );
    assert_error_body(
        change_root,
        CANNOT_CHANGE_PERMISSIONS_ID,
        CANNOT_CHANGE_PERMISSIONS_CODE,
        "re-permission root",
    )
    .await;
}

// === Authorized requests carry the SDK-hidden success status codes ==========

/// Root exercises the happy path; the SDK hides these exact status codes behind
/// `Ok(...)`, so only raw HTTP can pin produce 201 / read 200 / create-stream
/// 200 + the id body.
#[iggy_harness]
async fn given_http_requests_when_authorized_should_carry_sdk_hidden_status_codes(
    harness: &TestHarness,
) {
    let root = HttpClient::login_root(harness).await;
    let stream_id = root.create_stream("root-canary").await;
    assert!(stream_id < u64::MAX, "root create stream must return an id");
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

// === HTTP-only route shapes the SDK does not expose =========================

/// Routes with no SDK equivalent: the `/users/me` self alias, auth-only cluster
/// metadata (readable by an ungranted caller, so never RBAC-gated), the public
/// ping, and the caller-scoped name-sorted PAT-list JSON array.
#[iggy_harness]
async fn given_http_only_routes_when_requested_should_return_expected_shapes(
    harness: &TestHarness,
) {
    let root = HttpClient::login_root(harness).await;
    root.create_user("alice", "alice-pass", no_permissions())
        .await;
    let alice = root.login("alice", "alice-pass").await;

    // The `me` alias resolves to the caller's own user (self-scoped, no rule).
    let me = alice.get("/users/me").await;
    assert_eq!(me.status(), StatusCode::OK, "GET /users/me must be 200");
    let me_body: Value = me.json().await.expect("users/me body must be JSON");
    assert_eq!(
        me_body["username"], "alice",
        "the me alias must resolve to the caller's own user"
    );

    // Cluster metadata is auth-only: an ungranted caller still reads it, proving
    // the route is never RBAC-gated.
    assert_eq!(
        alice.get("/cluster/metadata").await.status(),
        StatusCode::OK,
        "cluster metadata is auth-only, never RBAC-gated"
    );

    // Ping is public (no bearer).
    assert_eq!(
        alice.get_anonymous("/ping").await.status(),
        StatusCode::OK,
        "ping is public"
    );

    // PAT listing is a JSON array of the caller's own tokens, name-sorted
    // (created in reverse order to prove the sort). Create returns 200.
    for name in ["alice-zeta", "alice-alpha"] {
        assert_eq!(
            alice
                .post_json(
                    "/personal-access-tokens",
                    &json!({ "name": name, "expiry": 0 }),
                )
                .await
                .status(),
            StatusCode::OK,
            "alice create PAT {name} must be 200"
        );
    }
    assert_eq!(
        pat_names(alice.get("/personal-access-tokens").await).await,
        ["alice-alpha", "alice-zeta"],
        "PAT list must be a name-sorted JSON array of the caller's tokens"
    );
}

// === Change-password wire contract (status/body shape) ==========================

/// The current-password check gates a self-service rotation: a wrong current
/// password is verified against the stored hash on shard 0, commits as a
/// rejecting no-op, and surfaces as 400 `InvalidCredentials` from the
/// replicated apply; only the correct one rotates the credential so the old
/// password stops working and the new one starts.
#[iggy_harness]
async fn given_own_password_when_current_wrong_should_reject_and_correct_should_rotate(
    harness: &TestHarness,
) {
    let root = HttpClient::login_root(harness).await;
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
    let root = HttpClient::login_root(harness).await;
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
