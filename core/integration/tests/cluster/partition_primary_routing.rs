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

//! The node a client is told is "the leader" must be the node that accepts a
//! partition write.
//!
//! `get_cluster_metadata` marks a node `Leader` from the METADATA plane's
//! `primary_index` alone, while a partition write is only accepted by the
//! primary of that partition's OWN consensus group. Both planes pick their
//! primary as `view % replica_count`, but their views are independent
//! counters, so the two answers agree only while the views are congruent mod
//! the replica count. Every other 3-node test happens to run with both planes
//! at view 0, where node 0 is leader and partition primary at once, so none of
//! them can see the split.
//!
//! This test forces the views apart: it moves the metadata plane off view 0,
//! brings every node back, then creates a topic whose partition group is brand
//! new and therefore still at view 0. The two assertions are deliberately
//! separate, because they distinguish the two candidate defects:
//!
//! - Node 0 accepts the write. A partition primary EXISTS; "partition groups
//!   never elect" is refuted.
//! - The advertised leader accepts the write. If this fails while the first
//!   passes, the defect is purely that the roster advertises the metadata
//!   leader as the destination for partition traffic.

use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "partition-routing-stream";
const TOPIC_NAME: &str = "partition-routing-topic";
const PARTITION_ID: u32 = 0;

/// Long enough for the backups to miss `cluster.heartbeat_timeout` (5s by
/// default) and conclude an election.
const ELECTION_SETTLE: Duration = Duration::from_secs(15);
/// Long enough for the restarted node 0 to rejoin at the new view.
const REJOIN_SETTLE: Duration = Duration::from_secs(10);
/// Well past a healthy send and past the SDK's own transient replay window, so
/// exceeding it means the client is not going to recover on its own.
const SEND_BUDGET: Duration = Duration::from_secs(45);

fn message(payload: &str) -> IggyMessage {
    IggyMessage::from_str(payload).expect("build message")
}

/// The roster's current leader as a node index, read through an already
/// connected client so no new connection is attempted while the cluster may
/// still be settling.
async fn read_leader_index(harness: &TestHarness, client: &IggyClient) -> Option<usize> {
    let metadata = client.get_cluster_metadata().await.ok()?;
    let leader_port = metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)?
        .endpoints
        .tcp;
    (0..harness.cluster_size()).find(|index| {
        harness
            .node(*index)
            .tcp_addr()
            .is_some_and(|address| address.port() == leader_port)
    })
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

    let probe = harness
        .root_client_for_node(1)
        .await
        .expect("root client on node 1 after the election");
    let leader = read_leader_index(harness, &probe)
        .await
        .expect("the cluster must name a leader once the election settled");
    assert_ne!(
        leader, 0,
        "killing node 0 must have moved the metadata plane off view 0; \
         with the leader back at node 0 the two planes agree and the split cannot show"
    );

    // A brand-new topic: its partition consensus group starts at view 0, so
    // its primary is replica 0, while the metadata leader is not node 0.
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

    // Where a client dialing node 0 actually ends up. If the SDK's leader
    // check moves it to the metadata leader, that redirect is itself the
    // defect: it walks the client off the only node that can accept the write.
    let node_zero_address = harness
        .node(0)
        .tcp_addr()
        .expect("node 0 exposes a TCP endpoint")
        .to_string();
    let on_node_zero = harness
        .root_client_for_node(0)
        .await
        .expect("root client on node 0");
    let landed_on = on_node_zero.get_connection_info().await.server_address;
    println!(
        "client dialed node 0 ({node_zero_address}); it settled on {landed_on}; \
         metadata leader is node {leader}"
    );

    // Assertion 1: node 0 accepts. Proves a partition primary exists, so the
    // failure mode is NOT "partition groups never elect". Retried briefly so a
    // post-CreateTopic materialisation window is not mistaken for the defect.
    let accepted_by_node_zero =
        send_once(&on_node_zero, &stream_id, &topic_id, &partitioning).await;
    assert!(
        accepted_by_node_zero.is_ok(),
        "a client dialing node 0 (the view-0 primary of this fresh partition group) must land on \
         a node that accepts the write; it settled on {landed_on} and got \
         {accepted_by_node_zero:?}"
    );

    // Assertion 2: the advertised leader accepts. This is the contract a
    // leader-aware SDK relies on, and the one the live cluster violated.
    let accepted_by_leader = send_once(&setup, &stream_id, &topic_id, &partitioning).await;
    assert!(
        accepted_by_leader.is_ok(),
        "node {leader} is advertised as the cluster leader, so a partition write sent there must \
         be accepted (or forwarded), got {accepted_by_leader:?}"
    );
}

/// One send, bounded. The SDK replays `TransientNotAccepted` and then hands the
/// request to its failover path, which re-reads the same roster and returns to
/// the same wrong node, so an unbounded send never returns while the defect is
/// present. The timeout turns that livelock into a readable failure.
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
