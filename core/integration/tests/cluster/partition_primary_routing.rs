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
//! new. Seeded from the metadata view it lands on the advertised leader; left
//! at view 0 it would name node 0, and no client could be told to go there.
//!
//! Both assertions are about the SAME node, and deliberately so. The SDK
//! follows the roster's leader on connect, so a client asking for node 0 does
//! not stay there - which is itself worth pinning down, since a test that
//! believes it is exercising node 0 while sitting on the leader proves
//! nothing. One assertion records where the client actually lands, the other
//! is the contract: the node the roster advertises accepts a partition write.

use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::disk::leader_node_index_via;
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "partition-routing-stream";
const TOPIC_NAME: &str = "partition-routing-topic";
const PARTITION_ID: u32 = 0;

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
