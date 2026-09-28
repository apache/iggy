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

//! Metadata and partition consensus groups choose primaries independently.
//! New partitions must inherit the metadata view, while existing partitions
//! can retain a different primary after a metadata-only election. SDK clients
//! settle on the metadata leader, so both writes to new partitions and group
//! polls of existing partitions must reach an owner that can commit progress.

use std::str::FromStr;
use std::time::Duration;

use super::register_forwarding::connect_without_login;
use futures::StreamExt;
use iggy::prelude::*;
use iggy_binary_protocol::codes::{
    ATTACH_CONSUMER_SESSION_CODE, GET_POLL_ROUTING_CODE, POLL_MESSAGES_ON_PRIMARY_CODE,
    STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::responses::messages::PollRoutingResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::{
    consumer_to_wire, identifier_to_wire, polling_strategy_to_wire,
};
use iggy_common::{BinaryTransport, RESYNC_REQUIRED_PARTITION_SENTINEL};
use integration::harness::TestHarness;
use integration::harness::disk::{
    leader_node_index_via, read_metadata_superblock_state, read_partition_superblock_state,
};
use integration::iggy_harness;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use tokio::time::{Instant, sleep, timeout};

const STREAM_NAME: &str = "partition-routing-stream";
const TOPIC_NAME: &str = "partition-routing-topic";
const PARTITION_ID: u32 = 0;
const GROUP_NAME: &str = "partition-routing-group";
const OFFSET_GROUP_NAME: &str = "offset-routing-group";
const GROUP_PAYLOADS: [&str; 2] = ["group-poll-first", "group-poll-second"];
const GROUP_POLL_BUDGET: Duration = Duration::from_secs(20);
// QUIC may use the full 30-second response budget before a dead data connection
// reports an uncertain outcome; recovery then needs a fresh routing attempt.
const DATA_FAILOVER_BUDGET: Duration = Duration::from_secs(60);
const INITIAL_VIEW: u32 = 0;
const METADATA_CHECKPOINT_REQUESTS: usize = 256;

/// Long enough for the backups to miss `cluster.heartbeat_timeout` (5s by
/// default) and conclude an election.
const ELECTION_SETTLE: Duration = Duration::from_secs(15);
/// Long enough for the restarted node 0 to rejoin at the new view.
const REJOIN_SETTLE: Duration = Duration::from_secs(10);
/// Under the SDK's own `RESPONSE_READ_TIMEOUT` (30s), so this fires first and
/// names the failure. Above it the SDK's timeout always wins and the budget is
/// dead code.
const SEND_BUDGET: Duration = Duration::from_secs(20);
/// How long the metadata plane gets to settle on a leader that is not node 0,
/// the state this test needs before it can observe anything.
const PRECONDITION_BUDGET: Duration = Duration::from_secs(20);
const PRECONDITION_POLL: Duration = Duration::from_millis(500);

fn message(payload: &str) -> IggyMessage {
    IggyMessage::from_str(payload).expect("build message")
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_metadata_view_moved_when_producing_to_a_fresh_topic_should_reach_the_advertised_leader(
    harness: &mut TestHarness,
) {
    // Kill node 0: it is the view-0 primary of BOTH planes, so the metadata
    // plane must elect someone else. Nothing has been written yet, so no
    // partition group exists to move with it. Fixed waits rather than polling:
    // dialing a leaderless cluster blocks for the SDK's own budget, and a poll
    // loop that opens a fresh connection each round never converges.
    harness.kill_node(0).expect("kill node 0");
    sleep(ELECTION_SETTLE).await;
    harness.restart_node(0).expect("restart node 0");
    sleep(REJOIN_SETTLE).await;

    // Read through node 1: node 0 has only just restarted, and the roster read
    // is auth-gated, so it needs a node that can complete a login now.
    //
    // A SETUP PRECONDITION, not an invariant of the system. `primary_index` is
    // `view % replica_count` with no `Status::Normal` gate, so a cluster that
    // elected three times is back to advertising node 0 while perfectly
    // healthy. Polled rather than asserted once: the split this test is about
    // is only observable while the planes disagree, and one kill normally
    // lands view 1 immediately.
    let leader = {
        let deadline = Instant::now() + PRECONDITION_BUDGET;
        loop {
            let index = leader_node_index_via(harness, 1).await;
            if index != 0 {
                break index;
            }
            assert!(
                Instant::now() < deadline,
                "the metadata plane never settled on a leader other than node 0 within \
                 {PRECONDITION_BUDGET:?}; with the leader at node 0 both planes agree and \
                 the split this test is about cannot show"
            );
            sleep(PRECONDITION_POLL).await;
        }
    };

    // A brand-new topic. Its partition group is seeded from the metadata view,
    // so its primary is the advertised leader; left at view 0 it would be
    // replica 0, the node that was just killed and restarted.
    let setup = harness
        .root_client_for_node(leader)
        .await
        .expect("root client on the metadata leader");
    setup
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    setup
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
        .expect("create topic");
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    // Where a client asking for node 0 actually ends up.
    let leader_address = harness
        .node(leader)
        .tcp_addr()
        .expect("the leader exposes a TCP endpoint")
        .to_string();
    let on_node_zero = harness
        .root_client_for_node(0)
        .await
        .expect("root client on node 0");
    let landed_on = on_node_zero.get_connection_info().await.server_address;

    // Asserted, not printed. `root_client_for_node` signs in, and sign-in ends
    // in the SDK's leader check, so this client is on the LEADER whatever node
    // it dialed. Pinning that down is what stops the send below from being
    // read as "node 0 accepted it": nothing here ever reaches node 0, and a
    // reader who assumes otherwise draws the opposite conclusion from a pass.
    assert_eq!(
        landed_on, leader_address,
        "a signed-in client follows the roster's leader, so one dialing node 0 must settle on \
         node {leader}; landing anywhere else means the redirect did not run and the send below \
         is testing a different node than this test claims"
    );

    // The contract: the node the roster advertises accepts a partition write.
    // Seeded from the metadata view the group's primary IS that node; left at
    // view 0 it would be replica 0, and every client would be steered away
    // from the only node that could accept.
    let accepted_by_leader = send_once(&on_node_zero, &stream_id, &topic_id, &partitioning).await;
    assert!(
        accepted_by_leader.is_ok(),
        "node {leader} is advertised as the cluster leader, so a partition write sent there must \
         be accepted (or forwarded), got {accepted_by_leader:?}"
    );
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_different_metadata_and_partition_primaries_when_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::Tcp).await;
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_different_metadata_and_partition_primaries_when_quic_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::Quic).await;
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_different_metadata_and_partition_primaries_when_websocket_group_auto_commits_should_return_messages(
    harness: &mut TestHarness,
) {
    assert_group_auto_commit_routing(harness, TransportProtocol::WebSocket).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    metadata.journal_slots = "256",
    http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
    http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
))]
async fn given_split_primaries_when_http_auto_commits_on_a_backup_should_replicate_offsets(
    harness: &mut TestHarness,
) {
    let (partition_primary, metadata_primary, _) = seed_split_primaries(harness).await;
    assert_ne!(partition_primary, metadata_primary);
    let client = harness
        .node(metadata_primary)
        .http_client()
        .unwrap()
        .with_root_login()
        .connect()
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::default();
    let response = timeout(
        GROUP_POLL_BUDGET,
        client.poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::first(),
            GROUP_PAYLOADS.len() as u32,
            true,
        ),
    )
    .await
    .expect("HTTP auto-commit must forward to the partition primary")
    .unwrap();
    assert_eq!(response.messages.len(), GROUP_PAYLOADS.len());
    for (message, expected) in response.messages.iter().zip(GROUP_PAYLOADS) {
        assert_eq!(message.payload.as_ref(), expected.as_bytes());
    }
    assert_replicated_offset(harness, &consumer, Some((GROUP_PAYLOADS.len() - 1) as u64)).await;
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
#[ignore = "requires Go; run this test explicitly with --ignored"]
async fn given_split_primaries_when_go_group_auto_commits_should_preserve_membership(
    harness: &mut TestHarness,
) {
    run_go_split_primary_test(
        harness,
        "^TestE2E_SplitPrimaryPollsPreserveCoordinatorMembership$",
    )
    .await;
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
#[ignore = "requires Go; run this test explicitly with --ignored"]
async fn given_split_primaries_when_go_group_commits_manually_should_preserve_membership(
    harness: &mut TestHarness,
) {
    run_go_split_primary_test(
        harness,
        "^TestE2E_SplitPrimaryManualCommitPreservesMembership$",
    )
    .await;
}

async fn run_go_split_primary_test(harness: &mut TestHarness, test: &str) {
    let (_, metadata_primary, _) = seed_split_primaries(harness).await;
    let output = tokio::process::Command::new("go")
        .args(["test", "./tests", "-run", test, "-count=1", "-v"])
        .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../foreign/go"))
        .env(
            "IGGY_TCP_ADDRESS",
            harness
                .node(metadata_primary)
                .tcp_addr()
                .unwrap()
                .to_string(),
        )
        .env("IGGY_POLL_ROUTING_STREAM", STREAM_NAME)
        .env("IGGY_POLL_ROUTING_TOPIC", TOPIC_NAME)
        .env(
            "IGGY_POLL_ROUTING_MESSAGES_PER_PARTITION",
            GROUP_PAYLOADS.len().to_string(),
        )
        .kill_on_drop(true)
        .output()
        .await
        .expect("run Go SDK test with the seeded cluster");
    assert!(
        output.status.success(),
        "Go SDK routing regression failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn assert_group_offset_routing(
    harness: &TestHarness,
    member: &IggyClient,
    metadata_address: &str,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let group = Identifier::named(OFFSET_GROUP_NAME).unwrap();
    let consumer = Consumer::group(group.clone());
    member
        .create_consumer_group(&stream, &topic, OFFSET_GROUP_NAME)
        .await
        .unwrap();
    member
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    let before = member.get_me().await.unwrap();
    for offset in 0..GROUP_PAYLOADS.len() as u64 {
        timeout(
            GROUP_POLL_BUDGET,
            member.store_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID), offset),
        )
        .await
        .expect("manual group commit must reach the partition primary")
        .expect("manual group commit must preserve its coordinator membership");
        assert_eq!(
            member.get_connection_info().await.server_address,
            metadata_address
        );
        assert_eq!(member.get_me().await.unwrap().client_id, before.client_id);
    }
    let expected_offset = (GROUP_PAYLOADS.len() - 1) as u64;
    assert_replicated_offset(harness, &consumer, Some(expected_offset)).await;
    member
        .delete_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap();
    assert_eq!(
        member.get_connection_info().await.server_address,
        metadata_address
    );
    let membership = member
        .get_consumer_group(&stream, &topic, &group)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.members_count, 1);
    assert_eq!(membership.members[0].partitions, [PARTITION_ID]);
    member
        .leave_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    assert_replicated_offset(harness, &consumer, None).await;
    for (group_name, policy) in [
        (
            "interval-offset-group",
            AutoCommit::Interval(NonZeroIggyDuration::ONE_SECOND),
        ),
        (
            "default-offset-group",
            AutoCommit::IntervalOrWhen(
                NonZeroIggyDuration::ONE_SECOND,
                AutoCommitWhen::PollingMessages,
            ),
        ),
    ] {
        let mut interval_consumer = member
            .consumer_group(group_name, STREAM_NAME, TOPIC_NAME)
            .unwrap()
            .batch_length(GROUP_PAYLOADS.len() as u32)
            .auto_commit(policy)
            .build();
        interval_consumer.init().await.unwrap();
        for _ in GROUP_PAYLOADS {
            timeout(GROUP_POLL_BUDGET, interval_consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let consumer = Consumer::group(Identifier::named(group_name).unwrap());
        assert_replicated_offset(harness, &consumer, Some(expected_offset)).await;
        assert_eq!(
            member.get_connection_info().await.server_address,
            metadata_address
        );
        assert_eq!(member.get_me().await.unwrap().client_id, before.client_id);
        interval_consumer.shutdown().await.unwrap();
    }
}

async fn assert_replicated_offset(
    harness: &TestHarness,
    consumer: &Consumer,
    expected: Option<u64>,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    for node in 0..harness.cluster_size() {
        let address = harness.node(node).tcp_addr().unwrap();
        let replica = connect_without_login(address).await;
        replica
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        timeout(PRECONDITION_BUDGET, async {
            loop {
                let offset = replica
                    .get_consumer_offset(consumer, &stream, &topic, Some(PARTITION_ID))
                    .await
                    .unwrap();
                if offset.map(|offset| offset.stored_offset) == expected {
                    break;
                }
                sleep(PRECONDITION_POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {node} must observe replicated offset {expected:?}"));
        assert_eq!(
            ClientWrapper::Tcp(replica)
                .get_connection_info()
                .await
                .server_address,
            address.to_string()
        );
    }
}

async fn seed_split_primaries(harness: &mut TestHarness) -> (usize, usize, u32) {
    let partition_primary = leader_node_index_via(harness, 0).await;
    let producer = harness
        .root_client_for_node(partition_primary)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    producer.create_stream(STREAM_NAME).await.unwrap();
    producer
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut messages: Vec<_> = GROUP_PAYLOADS.into_iter().map(message).collect();
    producer
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("seed messages before separating the consensus views");
    let partition_view =
        read_partition_superblock_state(&harness.node(partition_primary).data_path())
            .map_or(INITIAL_VIEW, |state| state.view);
    assert_eq!(
        partition_view as usize % harness.cluster_size(),
        partition_primary,
        "the seed producer must be on the partition primary"
    );

    // Checkpoint before editing the stopped backup's view, so the fixture
    // preserves a real durable state instead of fabricating its other fields.
    for index in 0..METADATA_CHECKPOINT_REQUESTS {
        producer
            .create_stream(&format!("{STREAM_NAME}-{index}"))
            .await
            .expect("fill the metadata journal to trigger its checkpoint");
    }
    let backup = (partition_primary + 1) % harness.cluster_size();
    timeout(PRECONDITION_BUDGET, async {
        while read_metadata_superblock_state(&harness.node(backup).data_path()).is_none() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the backup must checkpoint before the metadata-only view change");
    advance_backup_metadata_view(harness, backup);
    let metadata_primary = timeout(PRECONDITION_BUDGET, async {
        loop {
            let leader = leader_node_index_via(harness, partition_primary).await;
            if leader != partition_primary {
                break leader;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the metadata-only election must move leadership off the live partition primary");

    (partition_primary, metadata_primary, partition_view)
}

async fn assert_group_auto_commit_routing(harness: &mut TestHarness, transport: TransportProtocol) {
    let (partition_primary, metadata_primary, partition_view) = seed_split_primaries(harness).await;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let metadata_node = harness.node(metadata_primary);
    let (builder, address) = match transport {
        TransportProtocol::Tcp => (metadata_node.tcp_client(), metadata_node.tcp_addr()),
        TransportProtocol::Quic => (metadata_node.quic_client(), metadata_node.quic_addr()),
        TransportProtocol::WebSocket => (
            metadata_node.websocket_client(),
            metadata_node.websocket_addr(),
        ),
        TransportProtocol::Http => panic!("this test requires a binary transport"),
    };
    let member = builder
        .unwrap()
        .with_reconnecting_root_login()
        .connect()
        .await
        .unwrap();
    let metadata_address = address.unwrap().to_string();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let polled = member
                .poll_messages(
                    &stream,
                    &topic,
                    Some(PARTITION_ID),
                    &Consumer::default(),
                    &PollingStrategy::first(),
                    u32::try_from(GROUP_PAYLOADS.len()).unwrap(),
                    false,
                )
                .await
                .expect("the metadata leader can read the backup without committing offsets");
            if polled.messages.len() == GROUP_PAYLOADS.len() {
                break;
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the metadata leader's backup must hold both seeded messages");
    assert_group_offset_routing(harness, &member, &metadata_address).await;
    member
        .create_consumer_group(&stream, &topic, GROUP_NAME)
        .await
        .unwrap();
    let group = Identifier::named(GROUP_NAME).unwrap();
    member
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    let membership = member
        .get_consumer_group(&stream, &topic, &group)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.members_count, 1);
    assert_eq!(membership.members[0].partitions, [PARTITION_ID]);
    assert_eq!(
        member.get_connection_info().await.server_address,
        metadata_address
    );
    let backup_view = read_partition_superblock_state(&harness.node(metadata_primary).data_path())
        .map_or(INITIAL_VIEW, |state| state.view);
    assert_eq!(
        backup_view, partition_view,
        "the partition view must not follow the metadata-only election"
    );

    let consumer = Consumer::group(group);
    for expected in GROUP_PAYLOADS {
        let result = timeout(
            GROUP_POLL_BUDGET,
            member.poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            ),
        )
        .await
        .expect("an auto-commit group poll must complete within its routing budget");
        let endpoint = member.get_connection_info().await.server_address;
        let polled = result.unwrap_or_else(|error| {
            panic!(
                "a joined group must poll across different primaries: {error:?}; \
                 metadata primary={metadata_primary}, partition primary={partition_primary}, \
                 connection before={metadata_address}, connection after={endpoint}"
            )
        });
        assert_eq!(
            polled.messages.len(),
            1,
            "the assigned partition has unread messages"
        );
        assert_eq!(polled.messages[0].payload.as_ref(), expected.as_bytes());
        assert_eq!(
            endpoint, metadata_address,
            "a data poll must retain the metadata connection"
        );
        let current_membership = member
            .get_consumer_group(&stream, &topic, &consumer.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current_membership.members_count, 1);
        assert_eq!(current_membership.members[0].id, membership.members[0].id);
    }

    let expected_offset = u64::try_from(GROUP_PAYLOADS.len() - 1).unwrap();
    assert_replicated_offset(harness, &consumer, Some(expected_offset)).await;
    let partition_primary = assert_cached_route_after_primary_loss(
        harness,
        &member,
        partition_primary,
        &stream,
        &topic,
        &consumer,
    )
    .await;
    assert_data_session_fences(
        harness,
        &member,
        partition_primary,
        &stream,
        &topic,
        &consumer,
    )
    .await;
}

async fn assert_cached_route_after_primary_loss(
    harness: &mut TestHarness,
    member: &IggyClient,
    previous_primary: usize,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
) -> usize {
    const PAYLOAD: &str = "message-after-partition-failover";
    let mut receiver = member
        .consumer_group(GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .build();
    receiver.init().await.unwrap();
    assert!(
        member
            .poll_messages(
                stream,
                topic,
                None,
                consumer,
                &PollingStrategy::next(),
                1,
                true
            )
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let membership = member
        .get_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap()
        .unwrap();
    let coordinator = member.get_connection_info().await.server_address;
    harness.kill_node(previous_primary).unwrap();
    let poll = PollMessagesRequest {
        consumer: consumer_to_wire(consumer).unwrap(),
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: polling_strategy_to_wire(&PollingStrategy::next()),
        count: 1,
        auto_commit: true,
    }
    .to_bytes();
    let primary = timeout(PRECONDITION_BUDGET, async {
        loop {
            match member
                .send_binary_request(GET_POLL_ROUTING_CODE, poll.clone())
                .await
            {
                Ok(response) => {
                    let route = PollRoutingResponse::decode_from(&response).unwrap();
                    let node = (0..harness.cluster_size())
                        .find(|&node| {
                            harness.node(node).tcp_addr().unwrap().port() == route.primary.tcp_port
                        })
                        .unwrap();
                    if node != previous_primary {
                        break node;
                    }
                }
                Err(IggyError::TransientNotAccepted) => {}
                Err(error) => panic!("route discovery failed during partition election: {error:?}"),
            }
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the live metadata coordinator must discover the new partition primary");
    let producer = connect_without_login(harness.node(primary).tcp_addr().unwrap()).await;
    producer
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    producer
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut [message(PAYLOAD)],
        )
        .await
        .unwrap();
    let received = timeout(DATA_FAILOVER_BUDGET, async {
        loop {
            match receiver.next().await.expect("the consumer must stay open") {
                Ok(message) => break message,
                // The fixture killed the old owner before polling, so the
                // uncertain reply cannot hide admission of this new message.
                Err(IggyError::TransientNotCommitted) => {}
                Err(error) => panic!("poll could not recover its cached data route: {error:?}"),
            }
        }
    })
    .await
    .expect("IggyConsumer must resume after data loss without coordinator reconnect events");
    assert_eq!(received.message.payload.as_ref(), PAYLOAD.as_bytes());
    assert_eq!(
        member.get_connection_info().await.server_address,
        coordinator
    );
    let current = member
        .get_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.members_count, 1);
    assert_eq!(current.members[0].id, membership.members[0].id);
    primary
}

async fn assert_data_session_fences(
    harness: &TestHarness,
    member: &IggyClient,
    partition_primary: usize,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
) {
    const OTHER_USER: &str = "routing-other-user";
    const OTHER_PASSWORD: &str = "routing-other-password";
    let poll = PollMessagesRequest {
        consumer: consumer_to_wire(consumer).unwrap(),
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: polling_strategy_to_wire(&PollingStrategy::next()),
        count: 1,
        auto_commit: true,
    }
    .to_bytes();
    let response = member
        .send_binary_request(GET_POLL_ROUTING_CODE, poll.clone())
        .await
        .unwrap();
    let route = PollRoutingResponse::decode_from(&response).unwrap();
    let address = harness.node(partition_primary).tcp_addr().unwrap();
    let data = connect_without_login(address).await;
    data.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    member
        .create_user(OTHER_USER, OTHER_PASSWORD, UserStatus::Active, None)
        .await
        .unwrap();
    let other_user = Identifier::named(OTHER_USER).unwrap();
    timeout(PRECONDITION_BUDGET, async {
        while data.get_user(&other_user).await.unwrap().is_none() {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the data node must observe the new user before login");
    let other = connect_without_login(address).await;
    other.login_user(OTHER_USER, OTHER_PASSWORD).await.unwrap();
    assert!(
        matches!(
            other
                .send_raw_with_response(
                    ATTACH_CONSUMER_SESSION_CODE,
                    route.consumer_session.to_bytes()
                )
                .await,
            Err(IggyError::StaleClient)
        ),
        "authentication as another user must not authorize attachment"
    );
    let anonymous = connect_without_login(address).await;
    assert!(matches!(
        anonymous
            .send_raw_with_response(
                ATTACH_CONSUMER_SESSION_CODE,
                route.consumer_session.to_bytes()
            )
            .await,
        Err(IggyError::Unauthenticated)
    ));
    let mut wrong_epoch = route.consumer_session;
    wrong_epoch.session += 1;
    assert!(
        matches!(
            data.send_raw_with_response(ATTACH_CONSUMER_SESSION_CODE, wrong_epoch.to_bytes())
                .await,
            Err(IggyError::StaleClient)
        ),
        "attachment must require the exact parent epoch"
    );
    data.send_raw_with_response(
        ATTACH_CONSUMER_SESSION_CODE,
        route.consumer_session.to_bytes(),
    )
    .await
    .unwrap();
    data.send_raw_with_response(POLL_MESSAGES_ON_PRIMARY_CODE, poll.clone())
        .await
        .unwrap();

    member
        .leave_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap();
    timeout(PRECONDITION_BUDGET, async {
        while data
            .get_consumer_group(stream, topic, &consumer.id)
            .await
            .unwrap()
            .unwrap()
            .members_count
            != 0
        {
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("the data node must observe the member leaving");
    let response = data
        .send_raw_with_response(POLL_MESSAGES_ON_PRIMARY_CODE, poll.clone())
        .await
        .unwrap();
    let polled = PolledMessages::from_bytes(response).unwrap();
    assert_eq!(
        polled.partition_id, RESYNC_REQUIRED_PARTITION_SENTINEL,
        "the attached session must respect the member leaving"
    );
    assert!(polled.messages.is_empty());
    let offset_write = StoreConsumerOffsetRequest {
        consumer: consumer_to_wire(consumer).unwrap(),
        stream_id: identifier_to_wire(stream).unwrap(),
        topic_id: identifier_to_wire(topic).unwrap(),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: iggy_binary_protocol::AckLevel::Quorum,
    }
    .to_bytes();
    assert!(
        matches!(
            data.send_raw_with_response(STORE_CONSUMER_OFFSET_CODE, offset_write.clone())
                .await,
            Err(IggyError::ConsumerGroupPartitionNotOwned(..))
        ),
        "a departed member must not commit through its attachment"
    );

    member
        .join_consumer_group(stream, topic, &consumer.id)
        .await
        .unwrap();
    member
        .poll_messages(
            stream,
            topic,
            None,
            consumer,
            &PollingStrategy::next(),
            1,
            true,
        )
        .await
        .unwrap();
    let response = member
        .send_binary_request(GET_POLL_ROUTING_CODE, poll.clone())
        .await
        .unwrap();
    let route = PollRoutingResponse::decode_from(&response).unwrap();
    data.send_raw_with_response(
        ATTACH_CONSUMER_SESSION_CODE,
        route.consumer_session.to_bytes(),
    )
    .await
    .unwrap();
    member.logout_user().await.unwrap();
    timeout(PRECONDITION_BUDGET, async {
        loop {
            let result = data
                .send_raw_with_response(POLL_MESSAGES_ON_PRIMARY_CODE, poll.clone())
                .await;
            if matches!(result, Err(IggyError::StaleClient)) {
                break;
            }
            assert!(
                result.is_ok() || matches!(result, Err(IggyError::TransientNotAccepted)),
                "unexpected response while logout replicates: {result:?}"
            );
            sleep(PRECONDITION_POLL).await;
        }
    })
    .await
    .expect("parent logout must fence the independent data session");
    assert_eq!(
        ClientWrapper::Tcp(data)
            .get_connection_info()
            .await
            .server_address,
        address.to_string()
    );
}

fn advance_backup_metadata_view(harness: &mut TestHarness, backup: usize) {
    harness.stop_node(backup).expect("stop only a backup");
    let data_path = harness.node(backup).data_path();
    let mut state = read_metadata_superblock_state(&data_path).expect("backup metadata state");
    // Model a crash after persisting a metadata view change. Keeping log_view
    // and every partition file intact makes the two consensus planes diverge.
    state.view += 1;
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("superblock I/O runtime")
            .block_on(async move {
                PingPongSuperblock::open(data_path.join("metadata"))
                    .await
                    .expect("open the stopped backup's metadata superblock")
                    .write(&state.to_bytes())
                    .await
                    .expect("persist the metadata-only view change");
            });
    })
    .join()
    .expect("superblock writer thread");
    harness.restart_node(backup).expect("restart the backup");
}

/// One send, bounded. The SDK replays `TransientNotAccepted` and then hands the
/// request to its failover path, which re-reads the same roster and returns to
/// the same wrong node, so with the defect present the send burns its whole
/// budget. The timeout fires before the SDK's own and names which it was.
async fn send_once(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
) -> Result<(), String> {
    let mut messages = vec![message("probe")];
    match tokio::time::timeout(
        SEND_BUDGET,
        client.send_messages(stream_id, topic_id, partitioning, &mut messages),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(format!("{error:?}")),
        Err(_) => Err(format!(
            "no answer within {SEND_BUDGET:?} (client livelocked)"
        )),
    }
}
