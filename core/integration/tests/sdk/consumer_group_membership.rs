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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
use iggy_common::{BinaryTransport, ConsumerGroupClientState};
use integration::harness::wait_for_consumer_group_assignment;
use integration::iggy_harness;
use tokio::time::{sleep, timeout};

const STREAM_NAME: &str = "cg-membership-stream";
const TOPIC_NAME: &str = "cg-membership-topic";
const CONSUMER_GROUP_NAME: &str = "cg-membership-group";

// A member holding zero partitions polls empty forever, so a short wait is
// enough to let it sync (registering its membership) and then park.
const PARK_TIMEOUT: Duration = Duration::from_secs(2);
// Generous bound: on regression the poll hangs until this elapses.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);
const ASSIGNMENT_POLL_INTERVAL: Duration = Duration::from_millis(20);

// Legacy wire error codes for the join/leave failure ladder. Pinned as literals
// (not derived from `IggyError`) so renumbering the wire contract fails here
// loudly -- the same guarantee the non-Rust SDKs depend on.
const STREAM_ID_NOT_FOUND: u32 = 1009;
const TOPIC_ID_NOT_FOUND: u32 = 2010;
const CONSUMER_GROUP_ID_NOT_FOUND: u32 = 5000;
const CONSUMER_GROUP_MEMBER_NOT_FOUND: u32 = 5006;

// Every spec here pins a 60s server heartbeat because harness clients never
// ping on their own: the SDK pinger is spawned by `IggyClient::connect`, which
// the harness builder does not call. At the shipped 30s interval a group member
// that idles through another client's setup is reaped by the server's verifier,
// and the failure surfaces as a short member count instead of anything about
// the scenario under test.

// A consumer-group member holding zero partitions has the same empty client-side
// assignment as a non-member; only membership tells them apart. When the group
// is deleted under such a member, the poll must surface an error (driving a
// rejoin) rather than treating the empty assignment as "nothing to poll" and
// hanging forever.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_group_member_holds_no_partitions_when_group_deleted_should_surface_error_not_hang(
    harness: &TestHarness,
) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    // One connection administers the group and is the member that will hold the
    // single partition; the other backs the consumer under test.
    let mut clients = harness
        .root_clients(2)
        .await
        .expect("Failed to create root clients");
    let admin = clients.remove(0);
    let consumer_client = clients.remove(0);

    admin.create_stream(STREAM_NAME).await.unwrap();
    admin
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    admin
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();

    // The first member to join keeps the topic's only partition, leaving the
    // second member (the consumer under test) with none.
    admin
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let mut consumer = consumer_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .poll_interval(IggyDuration::new(Duration::from_millis(100)))
        .auto_join_consumer_group()
        .do_not_create_consumer_group_if_not_exists()
        .build();
    consumer.init().await.unwrap();

    wait_for_assignment(&admin, 2).await;
    let group = admin
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 2);
    let partition_counts: Vec<u32> = group
        .members
        .iter()
        .map(|member| member.partitions_count)
        .collect();
    assert!(
        partition_counts.contains(&0) && partition_counts.contains(&1),
        "expected one member to hold the single partition and one to hold none, got {partition_counts:?}"
    );

    // Alive group: a zero-partition member is a legitimate member, so its poll
    // parks (yields nothing) instead of surfacing an error or churning.
    match timeout(PARK_TIMEOUT, consumer.next()).await {
        Err(_elapsed) => {}
        Ok(Some(Ok(_))) => {
            panic!("a zero-partition member must not receive a message while its group is alive")
        }
        Ok(Some(Err(error))) => panic!(
            "a zero-partition member must not surface an error while its group is alive, got {error:?}"
        ),
        Ok(None) => panic!("consumer stream closed unexpectedly while the group is alive"),
    }

    admin
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    // Deleted group: the member is no longer registered, so the poll must fail
    // fast (the rejoin surfaces the missing group) rather than hang.
    let item = timeout(RESOLVE_TIMEOUT, consumer.next())
        .await
        .expect("poll must not hang after the group is deleted")
        .expect("consumer stream should remain open");
    match item {
        Err(IggyError::ConsumerGroupNameNotFound(..)) => {}
        Err(other) => {
            panic!("expected ConsumerGroupNameNotFound after group deletion, got error {other:?}")
        }
        Ok(_) => panic!("expected an error after group deletion, got a message"),
    }
}

// A member holding zero partitions gets an empty poll reply whose partition id
// is the `NO_ASSIGNED_PARTITION` sentinel, so a caller can tell it from an
// empty partition, which echoes its real id.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_member_holds_no_partitions_when_polled_should_report_no_assigned_partition(
    harness: &TestHarness,
) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    let mut clients = harness
        .root_clients(2)
        .await
        .expect("Failed to create root clients");
    let first_member = clients.remove(0);
    let second_member = clients.remove(0);

    first_member.create_stream(STREAM_NAME).await.unwrap();
    first_member
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    first_member
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();

    // The first member to join keeps the topic's only partition.
    first_member
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();
    second_member
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    wait_for_assignment(&first_member, 2).await;
    let consumer = Consumer::group(group_id.clone());
    let owner_poll = first_member
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(owner_poll.messages.is_empty());
    assert_eq!(
        owner_poll.partition_id, 0,
        "an empty poll of an owned partition must echo its id"
    );

    let surplus_poll = second_member
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(surplus_poll.messages.is_empty());
    assert_eq!(
        surplus_poll.partition_id, NO_ASSIGNED_PARTITION,
        "a member without partitions must report the sentinel"
    );
}

// A member built with `do_not_auto_join_consumer_group()` relies on the caller for the join, so
// polling must not wait for a join the consumer itself never performs.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_member_that_does_not_auto_join_when_joined_by_the_caller_should_receive_messages(
    harness: &TestHarness,
) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    let client = harness.new_client().await.expect("Failed to create client");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    let mut messages = vec![IggyMessage::from_str("message").unwrap()];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    // Membership is per connection, so the join goes through the client the consumer uses.
    client
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let mut consumer = client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .do_not_auto_join_consumer_group()
        .build();
    consumer.init().await.unwrap();

    let received = timeout(RESOLVE_TIMEOUT, consumer.next())
        .await
        .expect("a member joined by the caller must poll instead of waiting for a join")
        .expect("consumer stream should remain open")
        .expect("the member must receive the message from its assigned partition");
    assert_eq!(received.message.payload, "message");
}

// End-to-end wire pin for the consumer-group join/leave error ladder. The
// metadata STM unit tests pin the committed result codes; this pins that
// the server actually emits them over the wire, so a client observes the same
// codes the legacy server returns. Binary transports only: the HTTP client
// has no join/leave (stateless sessions carry no member identity, the SDK
// returns FeatureUnavailable client-side), so the ladder cannot run there.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_join_and_leave_failures_when_sent_over_the_wire_should_return_legacy_error_codes(
    harness: &TestHarness,
) {
    let root_client = harness.root_client().await.expect("root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    // Stands in for whichever level is absent; its position in the call marks
    // the level under test.
    let missing = Identifier::named("cg-membership-nonexistent").unwrap();

    // Each level is created only after the case proving its absence is
    // rejected, so no assertion resolves against pre-existing state.

    // Nothing exists yet: the stream miss is the first rejection.
    assert_rejected(
        root_client
            .join_consumer_group(&missing, &topic_id, &group_id)
            .await,
        STREAM_ID_NOT_FOUND,
        "join with a missing stream",
    );

    root_client.create_stream(STREAM_NAME).await.unwrap();
    assert_rejected(
        root_client
            .join_consumer_group(&stream_id, &missing, &group_id)
            .await,
        TOPIC_ID_NOT_FOUND,
        "join with a missing topic",
    );

    root_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    assert_rejected(
        root_client
            .join_consumer_group(&stream_id, &topic_id, &missing)
            .await,
        CONSUMER_GROUP_ID_NOT_FOUND,
        "join with a missing group",
    );

    let created = root_client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    // First group on the topic: ids are 0-based to match the legacy server,
    // pinned on the wire-visible response id.
    assert_eq!(created.id, 0, "first consumer group id must be 0-based");
    // The group resolves now, but this client never joined it: the member check
    // is the loud rejection, distinct from the missing-group case above.
    assert_rejected(
        root_client
            .leave_consumer_group(&stream_id, &topic_id, &group_id)
            .await,
        CONSUMER_GROUP_MEMBER_NOT_FOUND,
        "leave a group the client never joined",
    );

    // Non-vacuous control: the same client joins the group whose leave just
    // returned member-not-found, proving the setup is live.
    root_client
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join on an existing group must succeed");
}

fn assert_rejected(result: Result<(), IggyError>, expected_code: u32, context: &str) {
    let error = result.expect_err(context);
    assert_eq!(
        error.as_code(),
        expected_code,
        "{context}: expected wire error code {expected_code}, got {} ({error})",
        error.as_code(),
    );
}

// Membership is per connection: a reconnect registers a new client identity
// that the coordinator knows as a member of nothing, so the transport must not
// carry the old session's membership and assignment into the new one. Carried
// over, the first poll would run against a stale assignment instead of
// reporting the missing membership straight away.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_group_member_when_session_reset_should_forget_membership(harness: &TestHarness) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    let client = harness.new_client().await.expect("Failed to create client");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    client
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let consumer = Consumer::group(group_id.clone());
    // The first group poll syncs the assignment, which is what registers the
    // membership in the transport cache.
    wait_for_assignment(&client, 1).await;
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();

    let state = consumer_group_state(&client).await;
    // The key the join and the group poll build from the same identifiers.
    let key = format!("{stream_id}|{topic_id}|{group_id}");
    assert!(
        state.is_registered(&key),
        "the sync must register the membership"
    );
    assert!(
        state.has_assignment(&key),
        "the sole member must hold the topic's only partition"
    );

    client.disconnect().await.unwrap();
    // Reconnects through the transport rather than `IggyClient::connect`: the
    // heartbeat the latter spawns re-syncs every registered group on its own,
    // and this asserts on the reset alone.
    client.client().read().await.connect().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    assert!(
        state.registered_groups().is_empty(),
        "a reconnected client must not carry the old session's membership"
    );
    assert!(!state.is_registered(&key));
    assert!(!state.has_assignment(&key));

    // The new identity never joined, and the poll must say so instead of
    // running against the old assignment.
    let poll = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await;
    assert!(
        matches!(poll, Err(IggyError::ConsumerGroupMemberNotFound(..))),
        "expected ConsumerGroupMemberNotFound for a poll without a rejoin, got {poll:?}"
    );
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_weak_offset_durability_when_owner_changes_should_preserve_progress_after_crash(
    harness: &mut TestHarness,
) {
    const MESSAGE_COUNT: u32 = 3;
    const STORED_OFFSET: u64 = 1;
    let predecessor = harness.tcp_root_client().await.unwrap();
    let successor = harness.tcp_root_client().await.unwrap();
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    let consumer = Consumer::group(group_id.clone());
    predecessor.create_stream(STREAM_NAME).await.unwrap();
    predecessor
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Replicated,
                consumer_offset_durability: Durability::Replicated,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    predecessor
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    predecessor
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();
    wait_for_assignment(&predecessor, 1).await;
    let mut messages: Vec<_> = (0..MESSAGE_COUNT)
        .map(|offset| IggyMessage::from_str(&format!("record-{offset}")).unwrap())
        .collect();
    predecessor
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let polled = predecessor
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &consumer,
            &PollingStrategy::first(),
            2,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 2);
    let position = ConsumerPosition {
        partition_id: 0,
        offset: STORED_OFFSET,
        context: polled.context,
    };
    predecessor
        .store_consumer_position(&consumer, &stream_id, &topic_id, position)
        .await
        .unwrap();

    let primary = integration::harness::disk::leader_node_index(harness).await;
    harness
        .kill_node((primary + 1) % harness.cluster_size())
        .unwrap();
    predecessor
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();
    successor
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();
    wait_for_assignment(&successor, 1).await;
    let resumed = successor
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &consumer,
            &PollingStrategy::next(),
            MESSAGE_COUNT,
            false,
        )
        .await
        .unwrap();
    assert_eq!(resumed.messages.len(), 1);
    assert_eq!(resumed.messages[0].header.offset, STORED_OFFSET + 1);
    assert!(resumed.context.owner_generation > position.context.owner_generation);
    assert_eq!(resumed.context.incarnation, position.context.incarnation);
    assert!(matches!(
        successor
            .store_consumer_position(&consumer, &stream_id, &topic_id, position)
            .await,
        Err(IggyError::ConsumerGroupPartitionNotOwned(..))
    ));

    harness.kill_cluster().unwrap();
    harness.restart_cluster().await.unwrap();
    let recovered = harness.tcp_root_client().await.unwrap();
    timeout(RESOLVE_TIMEOUT, async {
        loop {
            let offset = recovered
                .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(0))
                .await
                .unwrap();
            if let Some(offset) = offset {
                assert_eq!(offset.stored_offset, STORED_OFFSET);
                break;
            }
            sleep(ASSIGNMENT_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the installed owner's committed offset must survive whole-cluster crash");
}

async fn wait_for_assignment(client: &IggyClient, members_count: u32) {
    wait_for_consumer_group_assignment(
        client,
        &Identifier::named(STREAM_NAME).unwrap(),
        &Identifier::named(TOPIC_NAME).unwrap(),
        &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        members_count,
        RESOLVE_TIMEOUT,
    )
    .await;
}

/// The consumer-group cache lives on the transport `IggyClient` wraps.
async fn consumer_group_state(client: &IggyClient) -> Arc<ConsumerGroupClientState> {
    match &*client.client().read().await {
        ClientWrapper::Tcp(client) => client.consumer_group_state(),
        ClientWrapper::Quic(client) => client.consumer_group_state(),
        ClientWrapper::WebSocket(client) => client.consumer_group_state(),
        ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => {
            panic!("the consumer-group cache is a binary-transport concern")
        }
    }
}
