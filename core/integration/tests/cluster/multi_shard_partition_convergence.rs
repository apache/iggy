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
//! `create_topic` returns on metadata commit, before the owning shard has run
//! `build_partition_fresh`, so a produce issued immediately after races
//! materialisation. Nothing about that race is resolved on the shard the client
//! is connected to: `shards_table` is a cache of the deterministic hash
//! assignment and may hold a row before the partition exists. The owning shard
//! resolves it, by parking the frame until its partition lands
//! (`park_if_unmaterialised`) and by fencing a mismatched incarnation
//! (`serves_committed_incarnation`), answering anything it cannot yet serve
//! with a retriable status the SDK replays.
//!
//! Forcing two shards is the point. Existing coverage of these fences is
//! single-shard, where the shard that admits the request is also the one that
//! owns the partition, so the cross-core path never runs: the hash fallback in
//! `router::route_typed`, the frame crossing into a peer's inbox, and that
//! peer's park queue draining on its own pump. Roughly half of the topics below
//! hash to a shard other than the one homing the connection.
//!
//! That split is a murmur3 outcome and is invisible from here -- this test would
//! stay green while silently degrading to single-shard if the hash or the shard
//! count changed. It is pinned instead where the assignment is a pure function,
//! by `partition_reconciler::tests::integration_topic_set_straddles_both_shards`,
//! which asserts over the same namespaces this test creates. Change the topic
//! count or the stream here and that guard has to move with it.
//!
//! Scope, stated plainly: this is a convergence test, not a barrier test. It
//! cannot tell a request served straight through from one that parked and was
//! re-dispatched, so it does not pin *which* mechanism carried it. What it does
//! catch is that mechanism failing outright -- a frame that never reaches the
//! owner, a park queue that never drains, or a fence that denies forever --
//! since every one of those surfaces as a failed send or a short poll.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use integration::iggy_harness;

const STREAM: &str = "convergence-stream";
const PARTITION_ID: u32 = 0;
/// Enough topics that the murmur3 assignment lands on both shards; every one is
/// asserted, so which side each falls on does not matter.
const TOPICS: u32 = 8;

fn topic_name(index: u32) -> String {
    format!("convergence-topic-{index}")
}

async fn create_topic(client: &IggyClient, stream: &Identifier, name: &str) {
    client
        .create_topic(
            stream,
            name,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
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
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            16,
            false,
        )
        .await
        .unwrap_or_else(|error| panic!("poll_messages: {error}"))
        .messages
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect()
}

/// Topics are created in a batch first, so several materialisations are in
/// flight at once when the produces start.
#[iggy_harness(cluster_nodes = 1, server(system.sharding.cpu_allocation = "2"))]
async fn given_two_shards_when_producing_right_after_create_topic_should_round_trip(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    client.create_stream(STREAM).await.unwrap();

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
}

/// Delete + recreate reuses the freed slab keys, so the namespace is
/// byte-identical across incarnations and only `created_revision` separates
/// them. This pins the observable outcome across that transition on a
/// multi-shard node: the recreated topic serves its own data and none of the
/// dead incarnation's.
///
/// It does NOT exercise the incarnation fence. Each step here is a completed
/// round trip, so the reconciler converges before the next one starts and
/// `serves_committed_incarnation` never denies (verified: zero "unverified
/// incarnation" denials in the server log across a full run). Driving the fence
/// needs a produce concurrent with the delete, from a second connection --
/// worth adding, but it is a different test. What this one catches is the
/// steady-state failure modes: a rebuild that wedges, or stale segments
/// surviving the delete and being served under the recycled identity.
#[iggy_harness(cluster_nodes = 1, server(system.sharding.cpu_allocation = "2"))]
async fn given_two_shards_when_recreating_a_topic_should_serve_only_the_new_incarnation(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = Identifier::named(STREAM).unwrap();
    client.create_stream(STREAM).await.unwrap();

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
}
