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

use iggy::prelude::*;
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
const GROUP_PAYLOADS: [&str; 2] = ["group-poll-first", "group-poll-second"];
const GROUP_POLL_BUDGET: Duration = Duration::from_secs(20);
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

    let member = harness
        .node(metadata_primary)
        .tcp_client()
        .unwrap()
        .with_reconnecting_root_login()
        .connect()
        .await
        .unwrap();
    let metadata_address = harness
        .node(metadata_primary)
        .tcp_addr()
        .unwrap()
        .to_string();
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
    }
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
