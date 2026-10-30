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

//! Single-node external auth callout integration tests.
//!
//! Each test starts a mock ext-auth HTTP server, configures the Iggy server
//! to use it, pre-provisions Iggy users via the root client, and then
//! verifies the ext-auth login flow across all transport protocols.

use std::collections::HashMap;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::ext_auth_server::{
    EXT_AUTH_IGGY_USERNAME, EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, EXT_DENIED_PASSWORD,
    EXT_DENIED_USERNAME, ExtAuthServer,
};
use integration::harness::{TestHarness, TestServerConfig, USER_PASSWORD, create_user, login_root};
use test_case::test_case;

fn ext_auth_envs(url: &str) -> HashMap<String, String> {
    HashMap::from([
        ("IGGY_EXTERNAL_AUTH_ENABLED".to_owned(), "true".to_owned()),
        ("IGGY_EXTERNAL_AUTH_URL".to_owned(), url.to_owned()),
        ("IGGY_EXTERNAL_AUTH_TIMEOUT".to_owned(), "5 s".to_owned()),
    ])
}

async fn start_ext_auth_harness(ext_server: &ExtAuthServer) -> TestHarness {
    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(ext_auth_envs(&ext_server.url()))
                .build(),
        )
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");
    harness
}

// ── allow login (all transports) ───────────────────────────────────

#[test_case(TransportProtocol::Tcp ; "tcp")]
#[test_case(TransportProtocol::Http ; "http")]
#[test_case(TransportProtocol::Quic ; "quic")]
#[test_case(TransportProtocol::WebSocket ; "websocket")]
#[tokio::test]
async fn given_ext_auth_when_allow_login_should_authorize(transport: TransportProtocol) {
    let ext_server = ExtAuthServer::start().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    create_user(&root_client, EXT_AUTH_IGGY_USERNAME).await;

    let ext_client = harness
        .client_builder_for(transport)
        .expect("client builder")
        .connect()
        .await
        .expect("ext connect");
    ext_client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await
        .expect("ext-auth login should succeed");

    let streams = ext_client
        .get_streams()
        .await
        .expect("ext-auth user should be able to read streams");
    assert!(streams.is_empty());
}

// ── deny login (all transports) ────────────────────────────────────

#[test_case(TransportProtocol::Tcp ; "tcp")]
#[test_case(TransportProtocol::Http ; "http")]
#[test_case(TransportProtocol::Quic ; "quic")]
#[test_case(TransportProtocol::WebSocket ; "websocket")]
#[tokio::test]
async fn given_ext_auth_when_deny_login_should_reject(transport: TransportProtocol) {
    let ext_server = ExtAuthServer::start().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let client = harness
        .client_builder_for(transport)
        .expect("client builder")
        .connect()
        .await
        .expect("connect");
    let err = client
        .login_user(EXT_DENIED_USERNAME, EXT_DENIED_PASSWORD)
        .await
        .expect_err("denied ext-auth login should fail");
    match transport {
        // Binary transports surface the eviction reason as InvalidCredentials.
        TransportProtocol::Tcp | TransportProtocol::Quic | TransportProtocol::WebSocket => {
            assert!(
                matches!(err, IggyError::InvalidCredentials),
                "expected InvalidCredentials, got: {err}"
            );
        }
        // HTTP maps InvalidCredentials to 400; the SDK wraps it as
        // HttpResponseError since InvalidCredentials is not in the
        // typed_refusal set.
        TransportProtocol::Http => {
            assert!(
                matches!(err, IggyError::HttpResponseError(400, _)),
                "expected HTTP 400, got: {err}"
            );
        }
    }
}

// ── allow with unknown iggy user (all transports) ──────────────────

#[test_case(TransportProtocol::Tcp ; "tcp")]
#[test_case(TransportProtocol::Http ; "http")]
#[test_case(TransportProtocol::Quic ; "quic")]
#[test_case(TransportProtocol::WebSocket ; "websocket")]
#[tokio::test]
async fn given_ext_auth_when_allow_unknown_iggy_user_should_reject(transport: TransportProtocol) {
    let ext_server = ExtAuthServer::start().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let client = harness
        .client_builder_for(transport)
        .expect("client builder")
        .connect()
        .await
        .expect("connect");
    let result = client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await;
    assert!(
        result.is_err(),
        "ext-auth allow for missing Iggy user should fail"
    );
}

// ── local user wrong password (all transports) ─────────────────────

#[test_case(TransportProtocol::Tcp ; "tcp")]
#[test_case(TransportProtocol::Http ; "http")]
#[test_case(TransportProtocol::Quic ; "quic")]
#[test_case(TransportProtocol::WebSocket ; "websocket")]
#[tokio::test]
async fn given_ext_auth_when_local_user_wrong_password_should_reject_without_callout(
    transport: TransportProtocol,
) {
    let ext_server = ExtAuthServer::start().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    create_user(&root_client, "local-user").await;

    let client = harness
        .client_builder_for(transport)
        .expect("client builder")
        .connect()
        .await
        .expect("connect");
    let result = client
        .login_user("local-user", "wrong-password-here!!")
        .await;
    assert!(
        result.is_err(),
        "local user with wrong password should be rejected"
    );
    assert_eq!(
        ext_server.request_count(),
        0,
        "local user login must not reach the ext-auth service"
    );
}

// ── local user correct password (all transports) ───────────────────

#[test_case(TransportProtocol::Tcp ; "tcp")]
#[test_case(TransportProtocol::Http ; "http")]
#[test_case(TransportProtocol::Quic ; "quic")]
#[test_case(TransportProtocol::WebSocket ; "websocket")]
#[tokio::test]
async fn given_ext_auth_when_local_user_correct_password_should_succeed(
    transport: TransportProtocol,
) {
    let ext_server = ExtAuthServer::start().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    create_user(&root_client, "local-user").await;

    let client = harness
        .client_builder_for(transport)
        .expect("client builder")
        .connect()
        .await
        .expect("connect");
    client
        .login_user("local-user", USER_PASSWORD)
        .await
        .expect("local user with correct password should succeed");
}

// ── server failure (TCP only, transport-independent behavior) ──────

#[tokio::test]
async fn given_ext_auth_when_server_fails_should_deny_login() {
    let ext_server = ExtAuthServer::start_failing().await;
    let harness = start_ext_auth_harness(&ext_server).await;

    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect");
    let err = client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await
        .expect_err("ext-auth server failure should deny login");
    // 5xx is transient, so the server returns TransientNotAccepted.
    assert!(
        matches!(
            err,
            IggyError::TransientNotAccepted | IggyError::TransientNotCommitted
        ),
        "5xx callout failure should be retryable, got: {err}"
    );
}

// ── server timeout (TCP only, transport-independent behavior) ──────

#[tokio::test]
async fn given_ext_auth_when_server_timeout_should_deny_login() {
    let ext_server = ExtAuthServer::start_slow(std::time::Duration::from_secs(30)).await;
    let mut envs = ext_auth_envs(&ext_server.url());
    envs.insert("IGGY_EXTERNAL_AUTH_TIMEOUT".to_owned(), "1 s".to_owned());

    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");

    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect");
    let err = client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await
        .expect_err("ext-auth timeout should deny login");
    // Timeout is transient.
    assert!(
        matches!(
            err,
            IggyError::TransientNotAccepted | IggyError::TransientNotCommitted
        ),
        "timeout callout failure should be retryable, got: {err}"
    );
}

// ── multi-shard callout (TCP, verifies peer shards call out) ──────

#[tokio::test]
async fn given_ext_auth_multi_shard_when_logins_spread_should_callout_on_every_shard() {
    let ext_server = ExtAuthServer::start().await;
    let mut envs = ext_auth_envs(&ext_server.url());
    // Two shards: TCP connections round-robin, so successive logins
    // alternate between shard 0 (has consensus) and shard 1 (forwards
    // the IsMetadataPrimary check to shard 0 before calling out).
    envs.insert("IGGY_SHARDING_CPU_ALLOCATION".to_owned(), "2".to_owned());

    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    create_user(&root_client, EXT_AUTH_IGGY_USERNAME).await;

    // Four logins over round-robin across 2 shards: each shard sees ~2.
    // Every login must reach the mock, proving peer shards call out too.
    let login_count = 4u32;
    for i in 0..login_count {
        let client = harness
            .client_builder_for(TransportProtocol::Tcp)
            .expect("tcp builder")
            .connect()
            .await
            .unwrap_or_else(|e| panic!("connect {i}: {e}"));
        client
            .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
            .await
            .unwrap_or_else(|e| panic!("ext-auth login {i}: {e}"));
    }

    assert_eq!(
        ext_server.request_count(),
        login_count,
        "every ext-auth login must produce exactly one callout, \
         including logins that land on a non-metadata shard"
    );
}

/// Tests with timing-dependent assertions that flake under CPU contention.
/// Nextest serializes this module with `threads-required = "num-cpus"`.
mod serialized {
    use super::*;

    #[tokio::test]
    async fn given_ext_auth_when_callout_cap_saturated_should_return_retryable_error() {
        let ext_server = ExtAuthServer::start_slow(Duration::from_secs(30)).await;
        let mut envs = ext_auth_envs(&ext_server.url());
        envs.insert("IGGY_EXTERNAL_AUTH_TIMEOUT".to_owned(), "60 s".to_owned());
        envs.insert(
            "IGGY_EXTERNAL_AUTH_MAX_CONCURRENT_CALLOUTS".to_owned(),
            "2".to_owned(),
        );
        // Pin to one shard so all logins hit the same per-shard cap.
        envs.insert("IGGY_SHARDING_CPU_ALLOCATION".to_owned(), "1".to_owned());

        let mut harness = TestHarness::builder()
            .server(TestServerConfig::builder().extra_envs(envs).build())
            .cluster_nodes(1)
            .build()
            .expect("build harness");
        harness.start().await.expect("start server");

        let mut handles = Vec::new();
        for _ in 0..2 {
            let client = harness
                .client_builder_for(TransportProtocol::Tcp)
                .expect("tcp builder")
                .connect()
                .await
                .expect("connect");
            handles.push(tokio::spawn(async move {
                client
                    .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
                    .await
            }));
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // The two slow callouts above should have reached the mock.
        assert_eq!(
            ext_server.request_count(),
            2,
            "both slot-filling logins must reach the ext-auth service"
        );

        let overflow_client = harness
            .client_builder_for(TransportProtocol::Tcp)
            .expect("tcp builder")
            .connect()
            .await
            .expect("connect for overflow");
        let result = overflow_client
            .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
            .await;

        let err = result.expect_err("overloaded login must fail");
        assert!(
            matches!(
                err,
                IggyError::TransientNotAccepted | IggyError::TransientNotCommitted
            ),
            "overloaded callout must return a retryable error, got: {err}"
        );
        // The overflow login must have been rejected before reaching the service.
        assert_eq!(
            ext_server.request_count(),
            2,
            "the overflow login must not reach the ext-auth service"
        );
    }
}
