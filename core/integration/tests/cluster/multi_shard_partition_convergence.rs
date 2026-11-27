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

//! Partition convergence across shards on one node.
//!
//! The node that answers `create_topic` holds the reply until its shards have
//! built the partitions it stores, for a bounded time
//! (`dispatch::submit::await_created_partitions`). A produce issued immediately
//! after finds its partition, unless the wait ended before the build.
//! Neither step is resolved on the shard the client is connected to:
//! `shards_table` is a cache of the deterministic hash assignment and may hold
//! a row before the partition exists. The owning shard resolves both. It
//! answers the wait when its partition lands, parks a frame that arrives before
//! that (`park_if_unmaterialised`), fences a mismatched incarnation
//! (`serves_committed_incarnation`), and answers anything it cannot yet serve
//! with a retriable status the SDK replays.
//!
//! Forcing more than one partition shard is the point. Existing coverage of
//! these fences is single-shard, where the shard that admits the request is
//! also the one that owns the partition, so the cross-core path never runs: the
//! hash fallback in `router::route_typed`, the frame crossing into a peer's
//! inbox, and that peer's park queue draining on its own pump. The first test
//! runs three shards under both placements. With shard 0 as an owner the
//! topics below land on all three shards, and with partitions kept off shard 0
//! they split between shards 1 and 2. Under both, some of them hash to a shard
//! other than the one homing the connection.
//!
//! That split is a murmur3 outcome and is invisible from here -- this test would
//! stay green while silently degrading to single-shard if the hash or the shard
//! count changed. It is pinned instead where the assignment is a pure function,
//! by the `partition_reconciler` test
//! `given_the_integration_topics_when_placed_on_three_shards_should_straddle_shards`,
//! which asserts over the same namespaces this test creates, once per
//! placement. Change the topic count or the stream here and that guard has to
//! move with it.
//!
//! Scope, stated plainly: this is a convergence test, not a barrier test. It
//! cannot tell a request served straight through from one that parked and was
//! re-dispatched, so it does not pin *which* mechanism carried it. What it does
//! catch is that mechanism failing outright -- a frame that never reaches the
//! owner, a park queue that never drains, or a fence that denies forever --
//! since every one of those surfaces as a failed send or a short poll.

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;

const STREAM: &str = "convergence-stream";
const PARTITION_ID: u32 = 0;
/// Enough topics that the murmur3 assignment lands on more than one partition
/// shard. Every one is asserted, so which shard each falls on does not matter.
const TOPICS: u32 = 8;

fn topic_name(index: u32) -> String {
    format!("convergence-topic-{index}")
}

async fn create_topic(client: &IggyClient, stream: &Identifier, name: &str) {
    client
        .create_topic(
            stream,
            name,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("create_topic {name}: {error}"));
}

async fn produce(client: &IggyClient, stream: &Identifier, topic: &Identifier, payload: &str) {
    let mut messages = vec![
        IggyMessage::builder()
            .payload(payload.to_owned().into())
            .build()
            .expect("message build"),
    ];
    client
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap_or_else(|error| panic!("send_messages {payload}: {error}"));
}

async fn poll_payloads(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Vec<String> {
    client
        .poll_messages(
            stream,
            topic,
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 16,
                ..PollOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("poll_messages: {error}"))
        .messages
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect()
}

/// Assert no park path degraded. All three modes log at `warn`, which the
/// harness captures by default, so counting them pins the mechanism positively:
/// the round trips stay green whether a produce parked and re-dispatched or was
/// denied and replayed, and only these markers tell the two apart.
fn assert_no_degraded_park_paths(harness: &TestHarness) {
    for marker in [
        "park buffer at capacity",
        "outlived their admission window",
        "rejecting parked partition frame",
    ] {
        assert_eq!(
            harness.server().stdout_occurrences(marker),
            0,
            "server log must not contain {marker:?}: the park buffer degraded instead of \
             converging"
        );
    }
}

/// Every topic is created before the first produce. Runs once per placement:
/// with shard 0 as a partition owner, and with partitions kept off it.
#[iggy_harness(
    cluster_nodes = 1,
    server(
        sharding.cpu_allocation = "3",
        cluster.coordinator.skip_shard_zero_for_partitions = ["false", "true"]
    )
)]
async fn given_three_shards_when_producing_right_after_create_topic_should_round_trip(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .unwrap();

    for index in 0..TOPICS {
        create_topic(&client, &stream, &topic_name(index)).await;
    }
    for index in 0..TOPICS {
        let topic = Identifier::named(&topic_name(index)).unwrap();
        produce(&client, &stream, &topic, &format!("payload-{index}")).await;
    }
    for index in 0..TOPICS {
        let topic = Identifier::named(&topic_name(index)).unwrap();
        assert_eq!(
            poll_payloads(&client, &stream, &topic).await,
            vec![format!("payload-{index}")],
            "topic {index} must return the message produced right after its creation"
        );
    }
    assert_no_degraded_park_paths(harness);
}

/// Delete + recreate reuses the freed slab keys, so the namespace is
/// byte-identical across incarnations and only `created_revision` separates
/// them. This pins the observable outcome across that transition on a
/// multi-shard node: the recreated topic serves its own data and none of the
/// dead incarnation's.
///
/// Does NOT exercise the incarnation fence. Every step is a completed round
/// trip, so the reconciler converges before the next starts and
/// `serves_committed_incarnation` has nothing to deny. That is an argument from
/// sequencing, not an observation: the deny logs at `debug` and the harness runs
/// the server at `info`, so its absence from the log proves nothing. Driving the
/// fence needs a produce concurrent with the delete from a second connection,
/// which is a different test. This one catches the steady-state failures: a
/// rebuild that wedges, or stale segments served under the recycled identity.
#[iggy_harness(cluster_nodes = 1, server(sharding.cpu_allocation = "2"))]
async fn given_two_shards_when_recreating_a_topic_should_serve_only_the_new_incarnation(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .unwrap();

    for index in 0..TOPICS {
        let name = topic_name(index);
        let topic = Identifier::named(&name).unwrap();
        create_topic(&client, &stream, &name).await;
        produce(&client, &stream, &topic, &format!("first-{index}")).await;

        client
            .delete_topic(&stream, &topic)
            .await
            .unwrap_or_else(|error| panic!("delete_topic {name}: {error}"));
        create_topic(&client, &stream, &name).await;
        produce(&client, &stream, &topic, &format!("second-{index}")).await;

        // Only the second incarnation's message may be readable: the first
        // partition's segments went with the delete, and a write admitted
        // against that incarnation would have been erased with it.
        assert_eq!(
            poll_payloads(&client, &stream, &topic).await,
            vec![format!("second-{index}")],
            "topic {index} must serve only the recreated incarnation"
        );
    }
    assert_no_degraded_park_paths(harness);
}
