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

//! A cluster whose replica 0 arrives after the others have already elected.
//!
//! Nothing in the server's bootstrap waits for peers: `await_bootstrap_complete`
//! is intra-node (shard 0 waiting on its sibling shards), and each node binds
//! its listeners as soon as its own shards load. So a slow replica 0 does not
//! hold the cluster back. Replicas 1 and 2 miss its heartbeats, conclude an
//! election, and start serving at a view whose primary is not replica 0.
//!
//! Every other cluster test starts through `TestHarness::start`, which waits
//! for all nodes to mesh before the first client op, so none of them can reach
//! this state. `partition_primary_routing` reaches the same view split by
//! killing node 0 mid-test; this reaches it the way production does, and the
//! two entry points exercise different code (a group materialised on a node
//! that was never in the founding quorum, versus one materialised on a node
//! that was).
//!
//! Two independent things are asserted, because they can fail separately:
//!
//! - A topic created while replica 0 is absent is writable at the advertised
//!   leader. This is the routing contract, and it holds only because a fresh
//!   partition group seeds its view from the metadata plane rather than
//!   starting at view 0 (which would name replica 0, the node that was late).
//! - The late replica converges. It missed the ops committed before it
//!   arrived, and the harness's usual all-nodes mesh gate is what normally
//!   guarantees no node is ever in that position.

use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "staggered-bootstrap-stream";
const TOPIC_NAME: &str = "staggered-bootstrap-topic";
const PARTITION_ID: u32 = 0;

/// Long enough for replicas 1 and 2 to miss `cluster.heartbeat_timeout` (5s by
/// default) and conclude an election without replica 0.
const ELECTION_SETTLE: Duration = Duration::from_secs(15);
/// Long enough for the late replica 0 to probe for the live view and rejoin.
const REJOIN_SETTLE: Duration = Duration::from_secs(10);
/// Well past a healthy send, so exceeding it means the client is not going to
/// recover on its own.
const SEND_BUDGET: Duration = Duration::from_secs(45);
/// How long the late replica gets to show it has caught up.
const CONVERGE_BUDGET: Duration = Duration::from_secs(30);
const MARKER_POLL: Duration = Duration::from_millis(250);

/// The late replica joining the view the others elected without it. Its own
/// recorded view is 0, which names ITSELF primary, so this line is where it
/// gives that up.
const VIEW_ADOPTED_MARKER: &str = "adopting view from StartView";

/// Markers that each independently prove the late replica pulled committed
/// state it did not have. Which one fires depends on whether the gap sits
/// above or below the serving peers' retained journal floor: repair refills
/// from the peers' journals, state transfer is what a gap below the retained
/// floor converts into. A cluster this small usually stays above the floor and
/// repairs, but the size of the founding quorum's log is not something this
/// test fixes, so either counts.
const CAUGHT_UP_MARKERS: [&str; 2] = [
    "metadata journal repair walked",
    "metadata state transfer installed",
];

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

#[iggy_harness(cluster_nodes = 3, manual_start)]
async fn given_replica_zero_arrives_late_when_producing_to_a_fresh_topic_should_reach_the_advertised_leader(
    harness: &mut TestHarness,
) {
    // Replicas 1 and 2 only. A successful root login inside `start_nodes` is a
    // committed Register, so returning at all proves the two of them formed a
    // quorum with replica 0 still absent.
    harness
        .start_nodes(&[1, 2])
        .await
        .expect("replicas 1 and 2 must form a quorum without replica 0");
    sleep(ELECTION_SETTLE).await;

    let probe = harness
        .root_client_for_node(1)
        .await
        .expect("root client on node 1 after the election");
    let leader = read_leader_index(harness, &probe)
        .await
        .expect("the cluster must name a leader once the election settled");
    assert_ne!(
        leader, 0,
        "replica 0 was never started, so it cannot be the metadata leader; \
         with the leader at node 0 the planes agree and the split cannot show"
    );

    // Replica 0 arrives into a cluster that has already elected past it.
    harness.start_node(0).expect("start the late replica 0");
    sleep(REJOIN_SETTLE).await;

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

    // The routing contract. The partition group is brand new, so its view is
    // whatever it was seeded with: the metadata view, which is not 0 here.
    // Seeded at 0 instead it would name replica 0, the node that arrived late,
    // and no client could be told to go there.
    let mut messages = vec![message("probe")];
    let accepted = tokio::time::timeout(
        SEND_BUDGET,
        setup.send_messages(&stream_id, &topic_id, &partitioning, &mut messages),
    )
    .await;
    assert!(
        matches!(accepted, Ok(Ok(_))),
        "node {leader} is advertised as the cluster leader, so a partition write sent there must \
         be accepted, got {accepted:?}"
    );

    // The late replica missed every op committed before it arrived. Read off
    // its own log rather than through a client: the SDK redirects to the
    // leader on connect, so a client dialing node 0 reports the leader's state,
    // not node 0's.
    let deadline = Instant::now() + CONVERGE_BUDGET;
    loop {
        let late = harness.node(0);
        let adopted_view = late.stdout_contains(VIEW_ADOPTED_MARKER);
        let caught_up = CAUGHT_UP_MARKERS
            .iter()
            .any(|marker| late.stdout_contains(marker));
        if adopted_view && caught_up {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the late replica 0 never converged within {CONVERGE_BUDGET:?} \
             (adopted the live view: {adopted_view}, caught up on committed ops: {caught_up}); \
             the harness's all-nodes mesh gate is what normally keeps a node out of this position"
        );
        sleep(MARKER_POLL).await;
    }
}
