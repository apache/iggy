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

//! Deleting data that is still counted must leave the parent totals correct.
//!
//! `stream_size_validation_scenario` covers delete too, but it purges each
//! topic first, so every delete it performs removes an already-empty scope and
//! cannot observe a rollback that never happened. These scenarios delete scopes
//! that still hold messages, which is what the parent totals are wrong about.
//!
//! What this guards, and what it does not. Two mechanisms roll a deleted scope
//! out of its parents: the metadata STM evicts the registry entries at commit
//! and zeroes them into their parents, and the reconciler's partition teardown
//! settles whatever landed after that. The STM half runs inside the apply that
//! produces the ack, on counters shared across every shard and both left-right
//! buffers, so it alone satisfies every assertion here. The reconciler runs as
//! a separate task, but a delete commit wakes it (`signal_reconcile_wake`), so
//! it too finishes before a client can complete another round trip.
//!
//! Measured, not reasoned: with only the STM rollback disabled this scenario
//! PASSES (while the `metadata::stm::stream` unit tests fail), and with only
//! the reconciler settle disabled it also passes. It fails only when BOTH are
//! disabled. Either mechanism alone keeps a client's view correct, so this is a
//! contract test for what a client observes and not a regression guard for
//! either half.
//!
//! Each half is guarded by unit tests instead: the `metadata::stm::stream`
//! module for the rollback and eviction, `server::partition_reconciler` for the
//! teardown settle and the membership gate. The integration layer cannot hold
//! the reconciler off by configuration -- the tick is capped at 30s, cannot be
//! set to zero, and the commit wake bypasses it regardless.
//!
//! Verifying any of this needs `cargo build --bin iggy-server` first. The
//! harness spawns the built binary, so a source-only mutation runs against the
//! previous build and reports a green that means nothing.
//!
//! The reads after each delete are deliberately single-shot. Retrying until the
//! numbers converge, the way the pre-delete assertions do for the async send
//! folding, would also wait out a broken rollback and pass regardless.

use crate::server::scenarios::{
    PARTITIONS_COUNT, batch_size, create_client, create_messages, validate_stream,
    validate_system_stats, validate_topic,
};
use iggy::prelude::*;
use integration::harness::{TestHarness, assert_clean_system, login_root};

const STREAM_NAME: &str = "delete-stats-stream";
const KEPT_TOPIC: &str = "kept-topic";
const DELETED_TOPIC: &str = "deleted-topic";
const MSGS_COUNT: u64 = 17;
const MSGS_SIZE: u64 = batch_size(MSGS_COUNT);

pub async fn run(harness: &TestHarness) {
    let client = create_client(harness).await;
    client.ping().await.unwrap();
    login_root(&client).await.expect("login failed");

    // Partition half first: its assertions are the tighter pair.
    delete_partitions_rolls_back_topic_and_stream(&client).await;
    delete_topic_rolls_back_the_stream(&client).await;

    assert_clean_system(&client).await;
}

/// Deleting partitions that still hold messages must take their bytes out of
/// both the topic and the stream. The retained partition's messages must stay
/// counted, so a blanket zeroing fails here as loudly as no rollback at all.
async fn delete_partitions_rolls_back_topic_and_stream(client: &IggyClient) {
    client.create_stream(STREAM_NAME).await.unwrap();
    create_topic(client, STREAM_NAME, KEPT_TOPIC).await;

    // One pass per partition, so the delete below removes a known share.
    for partition_id in 0..PARTITIONS_COUNT {
        send_one_pass(client, STREAM_NAME, KEPT_TOPIC, partition_id).await;
    }
    let all_partitions = MSGS_SIZE * u64::from(PARTITIONS_COUNT);
    let all_messages = MSGS_COUNT * u64::from(PARTITIONS_COUNT);
    validate_topic(
        client,
        STREAM_NAME,
        KEPT_TOPIC,
        all_partitions,
        all_messages,
    )
    .await;
    validate_stream(client, STREAM_NAME, all_partitions, all_messages).await;

    client
        .delete_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(KEPT_TOPIC).unwrap(),
            1,
        )
        .await
        .unwrap();

    // Single-shot again: the retained partitions' messages must still be
    // counted, so this fails both on a missing rollback and on a blanket
    // zeroing of the parents.
    let retained = u64::from(PARTITIONS_COUNT - 1);
    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(KEPT_TOPIC).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");
    assert_eq!(
        topic.size,
        MSGS_SIZE * retained,
        "the deleted partitions' bytes must leave the topic total on the delete's commit"
    );
    assert_eq!(topic.messages_count, MSGS_COUNT * retained);
    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");
    assert_eq!(
        stream.size,
        MSGS_SIZE * retained,
        "the deleted partitions' bytes must leave the stream total too"
    );
    assert_eq!(stream.messages_count, MSGS_COUNT * retained);

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Deleting a topic that still holds messages must take its bytes out of the
/// stream total, not just remove the topic.
async fn delete_topic_rolls_back_the_stream(client: &IggyClient) {
    client.create_stream(STREAM_NAME).await.unwrap();
    create_topic(client, STREAM_NAME, KEPT_TOPIC).await;
    create_topic(client, STREAM_NAME, DELETED_TOPIC).await;

    send_one_pass(client, STREAM_NAME, KEPT_TOPIC, 0).await;
    send_one_pass(client, STREAM_NAME, DELETED_TOPIC, 0).await;
    validate_stream(client, STREAM_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;

    client
        .delete_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(DELETED_TOPIC).unwrap(),
        )
        .await
        .unwrap();

    // Read once, with no convergence retry: the delete acks on the metadata
    // commit, so the totals must already be right in the reply the client is
    // holding. A retry here would wait for the reconciler's on-disk wipe and
    // hide the window entirely.
    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");
    assert_eq!(
        stream.size, MSGS_SIZE,
        "the deleted topic's bytes must leave the stream total on the commit that acked the delete"
    );
    assert_eq!(stream.messages_count, MSGS_COUNT);
    validate_topic(client, STREAM_NAME, KEPT_TOPIC, MSGS_SIZE, MSGS_COUNT).await;
    validate_system_stats(client, MSGS_SIZE, MSGS_COUNT).await;

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

async fn create_topic(client: &IggyClient, stream_name: &str, topic_name: &str) {
    client
        .create_topic(
            &Identifier::named(stream_name).unwrap(),
            topic_name,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
}

async fn send_one_pass(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    partition_id: u32,
) {
    let mut messages = create_messages(MSGS_COUNT);
    client
        .send_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .unwrap();
}
