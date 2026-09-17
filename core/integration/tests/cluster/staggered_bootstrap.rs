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
use integration::harness::disk::{leader_node_index_via, read_metadata_superblock_state};
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
/// Under the SDK's own `RESPONSE_READ_TIMEOUT` (30s), so this fires first and
/// names the failure rather than being shadowed by it.
const SEND_BUDGET: Duration = Duration::from_secs(20);
/// How long the late replica gets to show it has caught up.
const CONVERGE_BUDGET: Duration = Duration::from_secs(30);
const CONVERGENCE_POLL: Duration = Duration::from_millis(250);

fn message(payload: &str) -> IggyMessage {
    IggyMessage::from_str(payload).expect("build message")
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

    // Read through node 1: node 0 is not running, so it can answer no login,
    // and the roster read is auth-gated.
    //
    // Unlike the restart-driven twin of this test, this one is an invariant
    // rather than a precondition: replica 0 has never been started, so no view
    // it could be elected in exists yet.
    let leader = leader_node_index_via(harness, 1).await;
    assert_ne!(
        leader, 0,
        "replica 0 was never started, so it cannot be the metadata leader"
    );

    // Replica 0 arrives into a cluster that has already elected past it.
    harness.start_node(0).expect("start the late replica 0");
    sleep(REJOIN_SETTLE).await;

    let setup = harness
        .root_client_for_node(leader)
        .await
        .expect("root client on the metadata leader");
    let created_stream = setup
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    let created_topic = setup
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

    // Local materialization requires applying CreateTopic and every preceding
    // metadata op. Normal commit processing can finish catch-up without a
    // repair-completion log, and an SDK read could redirect to another node.
    let late_data_path = harness.node(0).data_path();
    let partition_path = late_data_path.join(format!(
        "streams/{}/topics/{}/partitions/{PARTITION_ID}",
        created_stream.id, created_topic.id
    ));
    let deadline = Instant::now() + CONVERGE_BUDGET;
    loop {
        let adopted_view =
            read_metadata_superblock_state(&late_data_path).is_some_and(|state| state.view > 0);
        let caught_up = partition_path.is_dir();
        if adopted_view && caught_up {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the late replica 0 never converged within {CONVERGE_BUDGET:?} \
             (adopted the live view: {adopted_view}, materialized replicated topic: {caught_up}); \
             the harness's all-nodes mesh gate is what normally keeps a node out of this position"
        );
        sleep(CONVERGENCE_POLL).await;
    }
}
