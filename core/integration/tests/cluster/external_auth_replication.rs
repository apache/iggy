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

//! Cluster-mode external auth: the reserved `external_auth_user_id` rides
//! the snapshot so every replica's in-apply authorization gate produces
//! identical decisions regardless of node-local config.

use iggy::prelude::*;
use integration::harness::TestHarnessBuilder;
use integration::harness::config::TestServerConfig;
use integration::harness::ext_auth_server::{EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, ExtAuthServer};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const LOGIN_BUDGET: Duration = Duration::from_secs(60);
const LOGIN_RETRY: Duration = Duration::from_millis(250);
const PARTITION_ID: u32 = 0;

fn ext_auth_cluster_config(url: String) -> TestServerConfig {
    TestServerConfig::builder()
        .extra_envs(HashMap::from([
            ("IGGY_EXTERNAL_AUTH_ENABLED".to_string(), "true".to_string()),
            ("IGGY_EXTERNAL_AUTH_URL".to_string(), url),
            (
                "IGGY_EXTERNAL_AUTH_FORWARD_CREDENTIALS".to_string(),
                "true".to_string(),
            ),
            ("IGGY_EXTERNAL_AUTH_TIMEOUT".to_string(), "5 s".to_string()),
        ]))
        .build()
}

async fn wait_for_stream(client: &IggyClient, stream_name: &str) {
    let deadline = Instant::now() + LOGIN_BUDGET;
    loop {
        match client.get_streams().await {
            Ok(streams) if streams.iter().any(|s| s.name == stream_name) => return,
            Ok(_) => sleep(LOGIN_RETRY).await,
            Err(_) if Instant::now() < deadline => sleep(LOGIN_RETRY).await,
            Err(e) => panic!("ext-auth user cannot list streams: {e}"),
        }
    }
}

async fn wait_for_poll(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    expected_payload: &[u8],
) {
    let consumer = Consumer::default();
    let deadline = Instant::now() + LOGIN_BUDGET;
    loop {
        match client
            .poll_messages(
                stream,
                topic,
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::first(),
                1,
                false,
            )
            .await
        {
            Ok(polled) if !polled.messages.is_empty() => {
                assert_eq!(
                    polled.messages[0].payload.as_ref(),
                    expected_payload,
                    "polled message payload mismatch"
                );
                return;
            }
            Ok(_) if Instant::now() < deadline => sleep(LOGIN_RETRY).await,
            Err(_) if Instant::now() < deadline => sleep(LOGIN_RETRY).await,
            Ok(_) => panic!("no messages polled within budget"),
            Err(e) => panic!("poll_messages failed: {e}"),
        }
    }
}

async fn create_stream_and_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
) -> (Identifier, Identifier) {
    client.create_stream(stream_name).await.unwrap();
    let stream = Identifier::named(stream_name).unwrap();
    client
        .create_topic(
            &stream,
            topic_name,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named(topic_name).unwrap();
    (stream, topic)
}

// --- Existing tests ---

#[tokio::test]
async fn given_cluster_when_ext_auth_login_should_replicate_and_authorize() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_should_replicate_and_authorize")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-auth-cluster").await.unwrap();

    let ext = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-auth-cluster").await;

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-auth-cluster"),
        "ext-auth user should see the stream"
    );

    let denied = ext.create_stream("should-fail").await;
    assert!(denied.is_err(), "ext-auth user must not create streams");
}

#[tokio::test]
async fn given_cluster_when_ext_auth_login_on_follower_should_forward_and_authorize() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_on_follower_should_forward_and_authorize")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-auth-follower").await.unwrap();

    let ext = harness
        .node(2)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-auth-follower").await;

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-auth-follower"),
        "ext-auth user on follower should see the stream"
    );

    let denied = ext.create_stream("should-fail-follower").await;
    assert!(
        denied.is_err(),
        "ext-auth user on follower must not create streams"
    );
}

// --- Data-plane tests ---

#[tokio::test]
async fn given_cluster_when_ext_auth_login_should_send_and_poll_messages() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_should_send_and_poll_messages")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream, topic) =
        create_stream_and_topic(&root, "ext-send-poll-stream", "ext-send-poll-topic").await;

    let ext = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-send-poll-stream").await;

    let mut messages = vec![
        IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"cluster-ext-auth-msg"))
            .build()
            .expect("build message"),
    ];

    ext.send_messages(
        &stream,
        &topic,
        &Partitioning::partition_id(PARTITION_ID),
        &mut messages,
    )
    .await
    .expect("ext-auth user should be able to send messages");

    wait_for_poll(&ext, &stream, &topic, b"cluster-ext-auth-msg").await;
}

#[tokio::test]
async fn given_cluster_when_ext_auth_login_on_follower_should_send_messages() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_on_follower_should_send_messages")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream, topic) =
        create_stream_and_topic(&root, "ext-follower-send-stream", "ext-follower-send-topic").await;

    // Login on a follower; the Register and SendMessages ops are forwarded
    // through consensus to the primary.
    let ext = harness
        .node(2)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-follower-send-stream").await;

    let mut messages = vec![
        IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"follower-sent"))
            .build()
            .expect("build message"),
    ];

    ext.send_messages(
        &stream,
        &topic,
        &Partitioning::partition_id(PARTITION_ID),
        &mut messages,
    )
    .await
    .expect("ext-auth user on follower should send messages");

    wait_for_poll(&ext, &stream, &topic, b"follower-sent").await;
}

// --- Permission replication ---

#[tokio::test]
async fn given_cluster_when_ext_auth_register_should_replicate_permissions() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_register_should_replicate_permissions")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    let (stream, topic) = create_stream_and_topic(
        &root,
        "ext-replicate-perms-stream",
        "ext-replicate-perms-topic",
    )
    .await;

    // Ext-auth login on node 0 and send a message. The Register op carries
    // the inline-grant permissions in its body, so every replica's client
    // table entry has the grant after consensus commit.
    let ext = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-replicate-perms-stream").await;

    let mut messages = vec![
        IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"replicated-perm-msg"))
            .build()
            .expect("build message"),
    ];

    ext.send_messages(
        &stream,
        &topic,
        &Partitioning::partition_id(PARTITION_ID),
        &mut messages,
    )
    .await
    .expect("send on node 0");

    // Root on node 1 polls the same topic. If the message is there, the
    // ext-auth user's SendMessages op was authorized and applied on all
    // replicas, proving the permissions replicated via Register.
    let root_node1 = harness.root_client_for_node(1).await.unwrap();
    wait_for_poll(&root_node1, &stream, &topic, b"replicated-perm-msg").await;
}

// --- Denial across cluster ---

#[tokio::test]
async fn given_cluster_when_ext_auth_should_deny_management_on_all_nodes() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_should_deny_management_on_all_nodes")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    // Login on the primary (node 0).
    let ext_primary = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let denied = ext_primary.create_stream("mgmt-fail-primary").await;
    assert!(
        denied.is_err(),
        "ext-auth user on primary must not create streams"
    );

    // Login on a follower (node 2).
    let ext_follower = harness
        .node(2)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let denied = ext_follower.create_stream("mgmt-fail-follower").await;
    assert!(
        denied.is_err(),
        "ext-auth user on follower must not create streams"
    );
}
