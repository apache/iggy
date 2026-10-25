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
//! verifies the ext-auth login flow.

use std::collections::HashMap;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::ext_auth_server::{
    EXT_AUTH_IGGY_USERNAME, EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, EXT_DENIED_PASSWORD,
    EXT_DENIED_USERNAME, ExtAuthServer,
};
use integration::harness::{TestHarness, TestServerConfig, USER_PASSWORD, create_user, login_root};

fn ext_auth_envs(url: &str) -> HashMap<String, String> {
    HashMap::from([
        ("IGGY_EXTERNAL_AUTH_ENABLED".to_owned(), "true".to_owned()),
        ("IGGY_EXTERNAL_AUTH_URL".to_owned(), url.to_owned()),
        ("IGGY_EXTERNAL_AUTH_TIMEOUT".to_owned(), "5 s".to_owned()),
    ])
}

/// Ext-auth callout returns `allow` with a pre-provisioned Iggy username.
/// The user logs in and can read streams.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_allow_login_should_authorize() {
    let ext_server = ExtAuthServer::start().await;
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

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    // Pre-provision the Iggy user the ext-auth service maps to.
    create_user(&root_client, EXT_AUTH_IGGY_USERNAME).await;

    // Login with ext-auth credentials.
    let ext_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("ext connect");
    ext_client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await
        .expect("ext-auth login should succeed");

    // The logged-in user inherits the Iggy user's permissions.
    let streams = ext_client
        .get_streams()
        .await
        .expect("ext-auth user should be able to read streams");
    assert!(streams.is_empty());
}

/// Ext-auth callout returns `deny`. The login fails.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_deny_login_should_reject() {
    let ext_server = ExtAuthServer::start().await;
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

    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect");
    let result = client
        .login_user(EXT_DENIED_USERNAME, EXT_DENIED_PASSWORD)
        .await;
    assert!(result.is_err(), "denied ext-auth login should fail");
}

/// Ext-auth callout returns `allow` with a username that does not exist
/// in Iggy. The login fails.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_allow_unknown_iggy_user_should_reject() {
    let ext_server = ExtAuthServer::start().await;
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

    // Do NOT create the Iggy user. The ext-auth callout will return `allow`
    // with EXT_AUTH_IGGY_USERNAME, but resolution should fail.
    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
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

/// A local user with wrong password must NOT fall through to ext-auth.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_local_user_wrong_password_should_reject_without_callout() {
    let ext_server = ExtAuthServer::start().await;
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

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    // Create a local user.
    create_user(&root_client, "local-user").await;

    // Attempt login with wrong password. Should fail without hitting
    // the ext-auth callout (LocalUserRejected path).
    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
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
}

/// Ext-auth server returns HTTP 500. The login fails (fail-closed).
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_server_fails_should_deny_login() {
    let ext_server = ExtAuthServer::start_failing().await;
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

    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect");
    let result = client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await;
    assert!(result.is_err(), "ext-auth server failure should deny login");
}

/// Ext-auth server times out. The login fails (fail-closed).
#[tokio::test]
#[serial_test::parallel]
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
    let result = client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await;
    assert!(result.is_err(), "ext-auth timeout should deny login");
}

/// When the per-shard callout cap is saturated, the next ext-auth login
/// gets a retryable error instead of a terminal InvalidCredentials.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_callout_cap_saturated_should_return_retryable_error() {
    // Slow server holds callouts open; cap of 2 is easy to saturate.
    let ext_server = ExtAuthServer::start_slow(Duration::from_secs(30)).await;
    let mut envs = ext_auth_envs(&ext_server.url());
    envs.insert("IGGY_EXTERNAL_AUTH_TIMEOUT".to_owned(), "60 s".to_owned());
    envs.insert(
        "IGGY_EXTERNAL_AUTH_MAX_CONCURRENT_CALLOUTS".to_owned(),
        "2".to_owned(),
    );

    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness.start().await.expect("start server");

    // Fire two logins that will block on the slow ext-auth server.
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

    // Give the server time to accept the two callouts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The third login hits the overloaded guard.
    let overflow_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect for overflow");
    let result = overflow_client
        .login_user(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .await;

    // The error must be retryable, not a terminal auth rejection.
    let err = result.expect_err("overloaded login must fail");
    assert!(
        matches!(
            err,
            IggyError::TransientNotAccepted | IggyError::TransientNotCommitted
        ),
        "overloaded callout must return a retryable error, got: {err}"
    );
}

/// A local user with correct password logs in through the built-in path,
/// never hitting the ext-auth callout.
#[tokio::test]
#[serial_test::parallel]
async fn given_ext_auth_when_local_user_correct_password_should_succeed() {
    let ext_server = ExtAuthServer::start().await;
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

    let root_client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("root connect");
    login_root(&root_client).await.expect("root login");

    create_user(&root_client, "local-user").await;

    let client = harness
        .client_builder_for(TransportProtocol::Tcp)
        .expect("tcp builder")
        .connect()
        .await
        .expect("connect");
    client
        .login_user("local-user", USER_PASSWORD)
        .await
        .expect("local user with correct password should succeed");
}
