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
//! PASSES (while 7 `metadata::stm::stream` unit tests fail), and with only the
//! reconciler settle disabled it also passes. It fails only when BOTH are
//! disabled. Either mechanism alone keeps a client's view correct, so this is a
//! contract test for what a client observes and not a regression guard for
//! either half.
//!
//! Each half is guarded by unit tests instead — `metadata::stm::stream` for the
//! rollback and eviction, `server::partition_reconciler` for the teardown
//! settle and the membership gate — because the integration layer cannot hold
//! the reconciler off by configuration: the tick is capped at 30s, cannot be
//! set to zero, and the commit wake bypasses it regardless.
//!
//! Verifying any of this needs `cargo build --bin iggy-server` first. The
//! harness spawns the built binary, so a source-only mutation runs against the
//! previous build and reports a green that means nothing.
//!
//! * `given_counted_partition_when_apply_delete_partitions_should_roll_it_out_of_the_parents`
//! * `given_counted_topic_when_apply_delete_topic_should_roll_it_out_of_the_stream`
//! * `given_many_counted_partitions_when_apply_delete_topic_should_roll_all_of_them_out`
//! * `given_non_zero_base_partition_ids_when_apply_delete_partitions_should_keep_the_survivor`
//! * `given_replayed_delete_partitions_when_applied_twice_should_not_double_roll_back`
//! * `given_topic_residue_no_partition_entry_covers_when_apply_delete_topic_should_settle_it`
//!
//! The reads after each delete are deliberately single-shot. Retrying until the
//! numbers converge, the way the pre-delete assertions do for the async send
//! folding, would also wait out a broken rollback and pass regardless.

use crate::server::scenarios::PARTITIONS_COUNT;
use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestHarness, assert_clean_system, login_root};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Committed partition ops fold into the shared stats on the owning shard, so a
// read can race the apply window. Retry until the expectation holds, then make
// the terminal assertion for a real mismatch.
const STATS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const STATS_RETRY_INTERVAL: Duration = Duration::from_millis(100);

const STREAM_NAME: &str = "delete-stats-stream";
const KEPT_TOPIC: &str = "kept-topic";
const DELETED_TOPIC: &str = "deleted-topic";
const MESSAGE_PAYLOAD_SIZE_BYTES: u64 = 57;
const MSGS_COUNT: u64 = 17;

// The server accounts the on-disk batch framing: one command header per append
// pass plus a per-message header. Mirrors `stream_size_validation_scenario`.
const NG_BATCH_HEADER_SIZE: u64 = 256;
const NG_MESSAGE_HEADER_SIZE: u64 = 48;
const MSGS_SIZE: u64 =
    NG_BATCH_HEADER_SIZE + (NG_MESSAGE_HEADER_SIZE + MESSAGE_PAYLOAD_SIZE_BYTES) * MSGS_COUNT;

pub async fn run(harness: &TestHarness) {
    let client = harness
        .new_client()
        .await
        .expect("Failed to create new client");
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
    create_stream(client, STREAM_NAME).await;
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
            &Identifier::from_str(STREAM_NAME).unwrap(),
            &Identifier::from_str(KEPT_TOPIC).unwrap(),
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
            &Identifier::from_str(STREAM_NAME).unwrap(),
            &Identifier::from_str(KEPT_TOPIC).unwrap(),
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
        .get_stream(&Identifier::from_str(STREAM_NAME).unwrap())
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
        .delete_stream(&Identifier::from_str(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
/// Deleting a topic that still holds messages must take its bytes out of the
/// stream total, not just remove the topic.
async fn delete_topic_rolls_back_the_stream(client: &IggyClient) {
    create_stream(client, STREAM_NAME).await;
    create_topic(client, STREAM_NAME, KEPT_TOPIC).await;
    create_topic(client, STREAM_NAME, DELETED_TOPIC).await;

    send_one_pass(client, STREAM_NAME, KEPT_TOPIC, 0).await;
    send_one_pass(client, STREAM_NAME, DELETED_TOPIC, 0).await;
    validate_stream(client, STREAM_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;

    client
        .delete_topic(
            &Identifier::from_str(STREAM_NAME).unwrap(),
            &Identifier::from_str(DELETED_TOPIC).unwrap(),
        )
        .await
        .unwrap();

    // Read once, with no convergence retry: the delete acks on the metadata
    // commit, so the totals must already be right in the reply the client is
    // holding. A retry here would wait for the reconciler's on-disk wipe and
    // hide the window entirely.
    let stream = client
        .get_stream(&Identifier::from_str(STREAM_NAME).unwrap())
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
        .delete_stream(&Identifier::from_str(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

async fn create_stream(client: &IggyClient, stream_name: &str) {
    client.create_stream(stream_name).await.unwrap();
}

async fn create_topic(client: &IggyClient, stream_name: &str, topic_name: &str) {
    client
        .create_topic(
            &Identifier::from_str(stream_name).unwrap(),
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
    let mut messages = create_messages();
    client
        .send_messages(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .unwrap();
}

fn create_messages() -> Vec<IggyMessage> {
    (0..MSGS_COUNT)
        .map(|offset| {
            let payload = Bytes::from(vec![b'x'; MESSAGE_PAYLOAD_SIZE_BYTES as usize]);
            IggyMessage::builder()
                .id(u128::from(offset) + 1)
                .payload(payload)
                .build()
                .expect("Failed to build message")
        })
        .collect()
}

async fn validate_stream(
    client: &IggyClient,
    stream_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stream = loop {
        let stream = client
            .get_stream(&Identifier::from_str(stream_name).unwrap())
            .await
            .unwrap()
            .expect("Failed to get stream");
        if (stream.size == expected_size && stream.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break stream;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(stream.size, expected_size, "stream size mismatch");
    assert_eq!(
        stream.messages_count, expected_messages_count,
        "stream messages_count mismatch"
    );
}

async fn validate_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let topic = loop {
        let topic = client
            .get_topic(
                &Identifier::from_str(stream_name).unwrap(),
                &Identifier::from_str(topic_name).unwrap(),
            )
            .await
            .unwrap()
            .expect("Failed to get topic");
        if (topic.size == expected_size && topic.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break topic;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(topic.size, expected_size, "topic size mismatch");
    assert_eq!(
        topic.messages_count, expected_messages_count,
        "topic messages_count mismatch"
    );
}

async fn validate_system_stats(
    client: &IggyClient,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stats = loop {
        let stats = client.get_stats().await.unwrap();
        if (stats.messages_count == expected_messages_count
            && stats.messages_size_bytes.as_bytes_u64() == expected_size)
            || Instant::now() >= deadline
        {
            break stats;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(
        stats.messages_count, expected_messages_count,
        "system stats messages_count mismatch"
    );
    assert_eq!(
        stats.messages_size_bytes.as_bytes_u64(),
        expected_size,
        "system stats messages_size_bytes mismatch"
    );
}
