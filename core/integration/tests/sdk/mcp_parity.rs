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

//! Plain-SDK reproduction of the `mcp::*` suite, without the MCP server. Each
//! MCP tool maps 1:1 to an SDK client method, so these tests exercise the same
//! server-ng behaviour under the `vsr` feature over TCP, without the MCP/HTTP
//! environment overhead. Assertions mirror the MCP counterparts.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use integration::{
    harness::{TestHarness, seeds},
    iggy_harness,
};

/// The consumer under which `seeds::mcp_standard` stores its offset (matches the
/// MCP server's default consumer name).
fn seed_consumer() -> Consumer {
    Consumer::new(Identifier::named(seeds::names::CONSUMER).unwrap())
}

async fn root_client(harness: &TestHarness) -> IggyClient {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client
}

// mcp::should_handle_ping
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_handle_ping(harness: &TestHarness) {
    let client = root_client(harness).await;
    client.ping().await.unwrap();
}

// mcp::should_return_cluster_metadata
// TODO: MCP polls `get_cluster_metadata` via a dedicated tool; there is no
// equivalent single SDK client method to assert cluster name + node count, so
// this case is intentionally not ported here.

// mcp::should_return_list_of_streams
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_list_of_streams(harness: &TestHarness) {
    let client = root_client(harness).await;
    let streams = client.get_streams().await.unwrap();

    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, seeds::names::STREAM);
    assert_eq!(streams[0].topics_count, 1);
}

// mcp::should_return_stream_details
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_stream_details(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let stream = client
        .get_stream(&stream_id)
        .await
        .unwrap()
        .expect("stream exists");

    assert_eq!(stream.name, seeds::names::STREAM);
    assert_eq!(stream.topics_count, 1);
    assert_eq!(stream.messages_count, 1);
}

// mcp::should_create_stream
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_create_stream(harness: &TestHarness) {
    let client = root_client(harness).await;
    let name = "new_stream";
    let stream = client.create_stream(name).await.unwrap();

    assert_eq!(stream.name, name);
    assert_eq!(stream.topics_count, 0);
    assert_eq!(stream.messages_count, 0);
}

// mcp::should_update_stream
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_update_stream(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    client
        .update_stream(&stream_id, "updated_stream")
        .await
        .unwrap();
}

// mcp::should_delete_stream
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_stream(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    client.delete_stream(&stream_id).await.unwrap();
}

// mcp::should_purge_stream
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_purge_stream(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    client.purge_stream(&stream_id).await.unwrap();
}

// mcp::should_return_list_of_topics
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_list_of_topics(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topics = client.get_topics(&stream_id).await.unwrap();

    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, seeds::names::TOPIC);
    assert_eq!(topics[0].partitions_count, 1);
    assert_eq!(topics[0].messages_count, 1);
}

// mcp::should_return_topic_details
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_topic_details(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let topic = client
        .get_topic(&stream_id, &topic_id)
        .await
        .unwrap()
        .expect("topic exists");

    assert_eq!(topic.id, 0);
    assert_eq!(topic.name, seeds::names::TOPIC);
    assert_eq!(topic.messages_count, 1);
}

// mcp::should_create_topic
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_create_topic(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let name = "new_topic";
    let topic = client
        .create_topic(
            &stream_id,
            name,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    assert_eq!(topic.id, 1);
    assert_eq!(topic.name, name);
    assert_eq!(topic.partitions_count, 1);
    assert_eq!(topic.messages_count, 0);
}

// mcp::should_update_topic
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_update_topic(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .update_topic(
            &stream_id,
            &topic_id,
            "updated_topic",
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

// mcp::should_delete_topic
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_topic(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client.delete_topic(&stream_id, &topic_id).await.unwrap();
}

// mcp::should_purge_topic
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_purge_topic(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client.purge_topic(&stream_id, &topic_id).await.unwrap();
}

// mcp::should_create_partitions
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_create_partitions(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .create_partitions(&stream_id, &topic_id, 3)
        .await
        .unwrap();
}

// mcp::should_delete_partitions
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_partitions(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .delete_partitions(&stream_id, &topic_id, 1)
        .await
        .unwrap();
}

// mcp::should_delete_segments
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_segments(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .delete_segments(&stream_id, &topic_id, 0, 1)
        .await
        .unwrap();
}

// mcp::should_poll_messages
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_poll_messages(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let messages = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &seed_consumer(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();

    assert_eq!(messages.messages.len(), 1);
    assert_eq!(messages.messages[0].header.offset, 0);
    let payload = messages.messages[0]
        .payload_as_string()
        .expect("Failed to parse payload");
    assert_eq!(payload, seeds::names::MESSAGE_PAYLOAD);
}

// mcp::should_send_messages
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_send_messages(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let mut messages = vec![
        IggyMessage::builder()
            .payload("test".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
}

// mcp::should_return_stats
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_stats(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stats = client.get_stats().await.unwrap();

    assert!(!stats.hostname.is_empty());
    assert_eq!(stats.messages_count, 1);
}

// mcp::should_return_me
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_return_me(harness: &TestHarness) {
    let client = root_client(harness).await;
    let me = client.get_me().await.unwrap();

    assert!(me.client_id > 0);
}

// mcp::should_return_clients
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_return_clients(harness: &TestHarness) {
    let client = root_client(harness).await;
    let clients = client.get_clients().await.unwrap();

    assert!(!clients.is_empty());
}

// mcp::should_handle_snapshot
// The diagnostic `snapshot` op (heap/config/process dump) is not implemented in
// server-ng; it falls through to the non-replicated catch-all and returns an
// empty body. Out of scope for metadata/stats parity -- un-ignore if server-ng
// grows the snapshot subsystem.
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
#[ignore = "vsr: server-ng does not implement the diagnostic snapshot op"]
async fn should_handle_snapshot(harness: &TestHarness) {
    let client = root_client(harness).await;
    let snapshot = client
        .snapshot(
            SnapshotCompression::default(),
            SystemSnapshotType::all_snapshot_types(),
        )
        .await
        .unwrap();

    assert!(!snapshot.0.is_empty());
}

// mcp::should_return_consumer_groups
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_consumer_groups(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let groups = client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .unwrap();

    assert!(!groups.is_empty());
}

// mcp::should_return_consumer_group_details
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_consumer_group_details(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let group_id = Identifier::named(seeds::names::CONSUMER_GROUP).unwrap();
    let group = client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("consumer group exists");

    assert_eq!(group.name, seeds::names::CONSUMER_GROUP);
    assert_eq!(group.partitions_count, 1);
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());
}

// mcp::should_create_consumer_group
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_create_consumer_group(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let name = "new_group";
    let group = client
        .create_consumer_group(&stream_id, &topic_id, name)
        .await
        .unwrap();

    assert_eq!(group.name, name);
    assert_eq!(group.partitions_count, 1);
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());
}

// mcp::should_delete_consumer_group
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_consumer_group(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let group_id = Identifier::named(seeds::names::CONSUMER_GROUP).unwrap();
    client
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();
}

// mcp::should_return_consumer_offset
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_consumer_offset(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let offset = client
        .get_consumer_offset(&seed_consumer(), &stream_id, &topic_id, Some(0))
        .await
        .unwrap()
        .expect("Expected consumer offset");

    assert_eq!(offset.partition_id, 0);
    assert_eq!(offset.stored_offset, 0);
    assert_eq!(offset.current_offset, 0);
}

// mcp::should_store_consumer_offset
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_store_consumer_offset(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .store_consumer_offset(&seed_consumer(), &stream_id, &topic_id, Some(0), 0)
        .await
        .unwrap();
}

// mcp::should_delete_consumer_offset
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_consumer_offset(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    client
        .delete_consumer_offset(&seed_consumer(), &stream_id, &topic_id, Some(0))
        .await
        .unwrap();
}

// Regression (no MCP counterpart): deleting an offset that was never stored
// must return a terminal `ConsumerOffsetNotFound`, NOT hang. The primary rejects
// at admission with an explicit error reply instead of dropping the request
// (which left the SDK replaying until its read-timeout).
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn delete_of_missing_consumer_offset_returns_not_found(harness: &TestHarness) {
    let client = root_client(harness).await;
    let stream_id = Identifier::named(seeds::names::STREAM).unwrap();
    let topic_id = Identifier::named(seeds::names::TOPIC).unwrap();
    let never_stored = Consumer::new(Identifier::named("never_stored").unwrap());
    let result = client
        .delete_consumer_offset(&never_stored, &stream_id, &topic_id, Some(0))
        .await;
    assert!(
        matches!(result, Err(IggyError::ConsumerOffsetNotFound(_))),
        "expected ConsumerOffsetNotFound, got {result:?}"
    );
}

// mcp::should_return_personal_access_tokens
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_personal_access_tokens(harness: &TestHarness) {
    let client = root_client(harness).await;
    let tokens = client.get_personal_access_tokens().await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].name, seeds::names::PERSONAL_ACCESS_TOKEN);
}

// mcp::should_create_personal_access_token
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_create_personal_access_token(harness: &TestHarness) {
    let client = root_client(harness).await;
    let name = "test_token";
    let token = client
        .create_personal_access_token(name, PersonalAccessTokenExpiry::NeverExpire)
        .await
        .unwrap();

    assert!(!token.token.is_empty());
}

// mcp::should_delete_personal_access_token
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_personal_access_token(harness: &TestHarness) {
    let client = root_client(harness).await;
    client
        .delete_personal_access_token(seeds::names::PERSONAL_ACCESS_TOKEN)
        .await
        .unwrap();
}

// mcp::should_return_users
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_users(harness: &TestHarness) {
    let client = root_client(harness).await;
    let users = client.get_users().await.unwrap();

    assert_eq!(users.len(), 2);
}

// mcp::should_return_user_details
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_return_user_details(harness: &TestHarness) {
    let client = root_client(harness).await;
    let user_id = Identifier::named(seeds::names::USER).unwrap();
    let user = client
        .get_user(&user_id)
        .await
        .unwrap()
        .expect("user exists");

    assert_eq!(user.username, seeds::names::USER);
}

// mcp::should_create_user
#[iggy_harness(test_client_transport = [Tcp])]
async fn should_create_user(harness: &TestHarness) {
    let client = root_client(harness).await;
    let username = "test-mcp-user";
    let user = client
        .create_user(username, "secret", UserStatus::Active, None)
        .await
        .unwrap();

    assert_eq!(user.username, username);
}

// mcp::should_update_user
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_update_user(harness: &TestHarness) {
    let client = root_client(harness).await;
    let user_id = Identifier::named(seeds::names::USER).unwrap();
    client
        .update_user(&user_id, Some("updated-user"), Some(UserStatus::Inactive))
        .await
        .unwrap();
}

// mcp::should_delete_user
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_delete_user(harness: &TestHarness) {
    let client = root_client(harness).await;
    let user_id = Identifier::named(seeds::names::USER).unwrap();
    client.delete_user(&user_id).await.unwrap();
}

// mcp::should_update_permissions
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_update_permissions(harness: &TestHarness) {
    let client = root_client(harness).await;
    let user_id = Identifier::named(seeds::names::USER).unwrap();

    let mut topics = std::collections::BTreeMap::new();
    topics.insert(
        1,
        TopicPermissions {
            manage_topic: true,
            read_topic: true,
            poll_messages: true,
            send_messages: true,
        },
    );
    let mut streams = std::collections::BTreeMap::new();
    streams.insert(
        1,
        StreamPermissions {
            manage_stream: true,
            manage_topics: true,
            topics: Some(topics),
            ..Default::default()
        },
    );
    let permissions = Permissions {
        global: GlobalPermissions {
            manage_servers: true,
            read_users: true,
            ..Default::default()
        },
        streams: Some(streams),
    };

    client
        .update_permissions(&user_id, Some(permissions))
        .await
        .unwrap();
}

// mcp::should_change_password
#[iggy_harness(test_client_transport = [Tcp], seed = seeds::mcp_standard)]
async fn should_change_password(harness: &TestHarness) {
    let client = root_client(harness).await;
    let user_id = Identifier::named(seeds::names::USER).unwrap();
    client
        .change_password(&user_id, seeds::names::USER_PASSWORD, "new_secret")
        .await
        .unwrap();
}
