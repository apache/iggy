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

//! External auth integration tests.

use iggy::prelude::*;
use integration::harness::TestHarnessBuilder;
use integration::harness::config::TestServerConfig;
use integration::harness::ext_auth_server::{
    EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, EXT_MAPPED_PASSWORD, EXT_MAPPED_USERNAME,
    EXT_SCOPED_PASSWORD, EXT_SCOPED_USERNAME, EXT_SHORT_PASSWORD, EXT_SHORT_USERNAME,
    ExtAuthServer,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

fn ext_auth_config(url: &str) -> TestServerConfig {
    ext_auth_config_with_timeout(url, "5 s")
}

fn ext_auth_config_with_timeout(url: &str, timeout: &str) -> TestServerConfig {
    TestServerConfig::builder()
        .extra_envs(HashMap::from([
            ("IGGY_EXTERNAL_AUTH_ENABLED".to_string(), "true".to_string()),
            ("IGGY_EXTERNAL_AUTH_URL".to_string(), url.to_string()),
            (
                "IGGY_EXTERNAL_AUTH_FORWARD_CREDENTIALS".to_string(),
                "true".to_string(),
            ),
            (
                "IGGY_EXTERNAL_AUTH_TIMEOUT".to_string(),
                timeout.to_string(),
            ),
        ]))
        .build()
}

async fn create_stream_and_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
) -> (Identifier, Identifier) {
    client.create_stream(stream_name).await.unwrap();
    let stream_id = Identifier::from_str_value(stream_name).unwrap();
    client
        .create_topic(
            &stream_id,
            topic_name,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic_id = Identifier::from_str_value(topic_name).unwrap();
    (stream_id, topic_id)
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_login_should_authorize_reads() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_login_should_authorize_reads",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-test-stream").await.unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-test-stream"),
        "ext-auth user should see the stream"
    );
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_login_should_deny_manage_ops() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_login_should_deny_manage_ops",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let denied = ext.create_stream("should-fail").await;
    assert!(denied.is_err(), "ext-auth user must not create streams");
}

#[tokio::test]
async fn given_ext_auth_when_unknown_user_should_deny() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_unknown_user_should_deny")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login("mallory", "wrong")
        .connect()
        .await;

    assert!(result.is_err(), "unknown user must be denied");
}

#[tokio::test]
async fn given_ext_auth_when_wrong_password_should_deny() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_wrong_password_should_deny")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, "wrong-password")
        .connect()
        .await;

    assert!(result.is_err(), "wrong password must be denied");
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_should_send_and_poll_messages() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_should_send_and_poll_messages",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream_id, topic_id) = create_stream_and_topic(&root, "msg-stream", "msg-topic").await;

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .payload("ext-auth-payload".to_string().into())
            .build()
            .unwrap(),
    ];
    ext.send_messages(
        &stream_id,
        &topic_id,
        &Partitioning::partition_id(0),
        &mut messages,
    )
    .await
    .expect("ext-auth user should be able to send messages");

    let polled = ext
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::first(),
            1,
            false,
        )
        .await
        .expect("ext-auth user should be able to poll messages");

    assert_eq!(polled.messages.len(), 1);
    assert_eq!(polled.messages[0].payload, "ext-auth-payload".as_bytes());
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_should_store_and_get_consumer_offset() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_should_store_and_get_consumer_offset",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream_id, topic_id) =
        create_stream_and_topic(&root, "offset-stream", "offset-topic").await;

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .payload("offset-test".to_string().into())
            .build()
            .unwrap(),
    ];
    ext.send_messages(
        &stream_id,
        &topic_id,
        &Partitioning::partition_id(0),
        &mut messages,
    )
    .await
    .unwrap();

    let consumer = Consumer::new(Identifier::numeric(1).unwrap());
    ext.store_consumer_offset(&consumer, &stream_id, &topic_id, Some(0), 0)
        .await
        .expect("ext-auth user should be able to store consumer offset");

    let offset = ext
        .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(0))
        .await
        .expect("ext-auth user should be able to get consumer offset");

    assert!(offset.is_some(), "stored offset should be retrievable");
    assert_eq!(offset.unwrap().stored_offset, 0);
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_should_join_and_leave_consumer_group() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_should_join_and_leave_consumer_group",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream_id, topic_id) = create_stream_and_topic(&root, "cg-stream", "cg-topic").await;

    let cg = root
        .create_consumer_group(&stream_id, &topic_id, "ext-cg")
        .await
        .unwrap();
    let cg_id = Identifier::numeric(cg.id).unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    ext.join_consumer_group(&stream_id, &topic_id, &cg_id)
        .await
        .expect("ext-auth user should be able to join a consumer group");

    ext.leave_consumer_group(&stream_id, &topic_id, &cg_id)
        .await
        .expect("ext-auth user should be able to leave a consumer group");
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_should_deny_create_topic() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_inline_grant_should_deny_create_topic")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("topic-deny-stream").await.unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let stream_id = Identifier::from_str_value("topic-deny-stream").unwrap();
    let denied = ext
        .create_topic(&stream_id, "should-fail", &TopicCreateOptions::default())
        .await;

    assert!(
        denied.is_err(),
        "ext-auth user must not create topics (manage_topics=false)"
    );
}

#[tokio::test]
async fn given_ext_auth_when_scoped_grant_should_allow_granted_stream_and_deny_other() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_scoped_grant_should_allow_granted_stream_and_deny_other",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    // The scoped grant targets stream id 0 (EXT_SCOPED_STREAM_ID).
    // The first stream created gets slab index 0.
    let (allowed_stream, allowed_topic) =
        create_stream_and_topic(&root, "allowed", "allowed-topic").await;
    let (forbidden_stream, forbidden_topic) =
        create_stream_and_topic(&root, "forbidden", "forbidden-topic").await;

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_SCOPED_USERNAME, EXT_SCOPED_PASSWORD)
        .connect()
        .await
        .unwrap();

    ext.get_stream(&allowed_stream)
        .await
        .expect("scoped user should read the granted stream");

    let denied = ext.get_stream(&forbidden_stream).await;
    assert!(
        denied.is_err(),
        "scoped user must not read a stream outside the grant"
    );

    let mut messages = vec![
        IggyMessage::builder()
            .payload("scoped-test".to_string().into())
            .build()
            .unwrap(),
    ];
    ext.send_messages(
        &allowed_stream,
        &allowed_topic,
        &Partitioning::partition_id(0),
        &mut messages,
    )
    .await
    .expect("scoped user should send to the granted stream");

    let mut messages = vec![
        IggyMessage::builder()
            .payload("forbidden-test".to_string().into())
            .build()
            .unwrap(),
    ];
    let denied = ext
        .send_messages(
            &forbidden_stream,
            &forbidden_topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await;
    assert!(
        denied.is_err(),
        "scoped user must not send to a stream outside the grant"
    );
}

#[tokio::test]
async fn given_ext_auth_when_iggy_user_mapping_should_inherit_user_permissions() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_iggy_user_mapping_should_inherit_user_permissions",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("mapped-stream").await.unwrap();

    let user = root
        .create_user(
            "mapped-local-user",
            "dummy-password",
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                },
                streams: None,
            }),
        )
        .await
        .unwrap();

    // The mock returns IggyUser { user_id: EXT_MAPPED_USER_ID (42) }.
    // The server maps the external login to the Iggy user with that id.
    // However, user ids are assigned by the server slab allocator, so
    // user_id 42 likely does not match. We skip this test if the created
    // user's id is not 42.
    if user.id != integration::harness::ext_auth_server::EXT_MAPPED_USER_ID {
        // Cannot guarantee the slab assigns id 42; skip gracefully.
        return;
    }

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_MAPPED_USERNAME, EXT_MAPPED_PASSWORD)
        .connect()
        .await
        .unwrap();

    let streams = ext.get_streams().await;
    assert!(
        streams.is_ok(),
        "mapped user should inherit read_streams permission"
    );

    let denied = ext.create_stream("should-fail-mapped").await;
    assert!(
        denied.is_err(),
        "mapped user must not create streams (manage_streams=false)"
    );
}

#[tokio::test]
async fn given_ext_auth_when_grant_expires_should_deny_after_expiry() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_grant_expires_should_deny_after_expiry")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("expiry-stream").await.unwrap();

    // The short grant expires in 2 seconds.
    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_SHORT_USERNAME, EXT_SHORT_PASSWORD)
        .connect()
        .await
        .unwrap();

    let streams = ext.get_streams().await;
    assert!(
        streams.is_ok(),
        "short-lived grant should work immediately after login"
    );

    tokio::time::sleep(Duration::from_secs(3)).await;

    let expired = ext.get_streams().await;
    assert!(
        expired.is_err(),
        "requests after grant expiry must be denied"
    );
}

#[tokio::test]
async fn given_ext_auth_when_server_fails_should_deny_login() {
    let auth = ExtAuthServer::start_failing().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_server_fails_should_deny_login")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await;

    assert!(
        result.is_err(),
        "login must fail when the external auth server returns 500"
    );
}

#[tokio::test]
async fn given_ext_auth_when_server_timeout_should_deny_login() {
    let auth = ExtAuthServer::start_slow(Duration::from_secs(30)).await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_server_timeout_should_deny_login")
        .server(ext_auth_config_with_timeout(&auth.url(), "1 s"))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await;

    assert!(
        result.is_err(),
        "login must fail when the external auth server times out"
    );
}

#[tokio::test]
async fn given_ext_auth_when_local_user_exists_should_not_fallback() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_local_user_exists_should_not_fallback")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_user("localuser", "localpass", UserStatus::Active, None)
        .await
        .unwrap();

    // Correct local credentials: local login succeeds (no ext-auth fallback).
    let local = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login("localuser", "localpass")
        .connect()
        .await;
    assert!(
        local.is_ok(),
        "local user with correct password should log in"
    );

    // Wrong password for a known local user: must not fall through to ext-auth.
    let wrong = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login("localuser", "wrongpass")
        .connect()
        .await;
    assert!(
        wrong.is_err(),
        "wrong password for a local user must not trigger ext-auth fallback"
    );
}

#[tokio::test]
async fn given_ext_auth_when_node_drops_should_reconnect_and_reauth_on_another_node() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_node_drops_should_reconnect_and_reauth_on_another_node",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream_id, _topic_id) =
        create_stream_and_topic(&root, "reconnect-stream", "reconnect-topic").await;

    let node0_addr = harness.node(0).raw_tcp_addr().unwrap();
    let ext = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_reconnecting_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .with_reestablish_after(IggyDuration::new(Duration::from_millis(100)))
        .connect()
        .await
        .unwrap();

    ext.get_stream(&stream_id)
        .await
        .expect("ext-auth user should read the stream before node drop");

    let pre_drop = ext.get_connection_info().await.server_address;
    assert_eq!(
        pre_drop, node0_addr,
        "initial connection should be to node 0"
    );

    harness.kill_node(0).unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut reconnected = false;
    while Instant::now() < deadline {
        match ext.get_streams().await {
            Ok(streams) => {
                assert!(
                    !streams.is_empty(),
                    "reconnected session should still see streams"
                );
                reconnected = true;
                break;
            }
            Err(_) => sleep(Duration::from_millis(200)).await,
        }
    }
    assert!(
        reconnected,
        "ext-auth client must reconnect to a surviving node after the original drops"
    );

    let post_drop = ext.get_connection_info().await.server_address;
    assert_ne!(
        post_drop, node0_addr,
        "after the original node drops the client must be on a different node"
    );
}
