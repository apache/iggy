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

//! Data-plane produce+consume stress across server-ng topologies: `PRODUCERS`
//! producers and `CONSUMERS` consumers all hammer a SINGLE partition, asserting
//! no message loss and a strictly contiguous offset log.
//!
//! Targets the partition-ref-across-await UB fix: the consume poll path and the
//! produce/commit pump run as sibling tasks over the same partition, so
//! concentrating every producer and consumer on one partition maximizes the
//! `&`/`&mut` aliasing window on that partition's pump that the fix closes. All
//! producers run concurrently with all consumers for `HAMMER_DURATION`; the
//! consumers then drain. A single partition lives on a single shard, so the
//! multi-shard variants still spin up N shards but concentrate the load on the
//! one owning shard.
//!
//! Oracle: producers interleave on the partition's shared offset sequence, so
//! per-producer contiguity does not hold. Instead every consumer reads the
//! partition in full and must observe a contiguous `0..total` (no gap = no loss,
//! no dup) with a count equal to the sum of all producers' sends.
//!
//! Strictly data plane: polls by explicit offset with `auto_commit = false` and
//! performs no mid-run topic/partition mutation, so it never drives the metadata
//! consensus plane concurrently. That avoids a separate, still-open `on_ack`
//! journal-durability race that panics the primary under concurrent metadata ops
//! (see the gated `concurrent_produce_consume_scenario` in `scenarios/mod.rs`).

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "stress-pc-stream";
const TOPIC_NAME: &str = "stress-pc-topic";
// All traffic targets this one partition to concentrate produce+consume
// contention on a single partition pump.
const PARTITION_ID: u32 = 0;
const PRODUCERS: u32 = 4;
const CONSUMERS: u32 = 4;
const PRODUCER_BATCH: u32 = 16;
const CONSUMER_BATCH: u32 = 64;
const HAMMER_DURATION: Duration = Duration::from_secs(20);
// Safety net so a wedged consumer fails loudly instead of hanging the suite.
const MAX_TEST_DURATION: Duration = Duration::from_secs(120);
// Whole-test wall-clock guard. A server that dies at boot leaves the harness
// client retrying connect with no cap, and a parked poll never re-checks
// MAX_TEST_DURATION, so without this the suite hangs indefinitely instead of
// failing. Set above MAX_TEST_DURATION so a slow-but-progressing consumer
// trips its own informative deadline first.
const WALL_CLOCK_TIMEOUT: Duration = Duration::from_secs(150);
// Empty polls observed after producers stop before the partition is declared drained.
const DRAIN_EMPTY_POLLS: u32 = 20;

/// Single-node, single shard (`"1"`) and multi shard (`"2"`).
#[iggy_harness(
    cluster_nodes = 1,
    server(system.sharding.cpu_allocation = ["1", "2"])
)]
async fn given_single_node_when_produce_consume_hammered_should_not_lose_messages(
    harness: &TestHarness,
) {
    run_hammer(harness).await;
}

/// Three-node cluster, single shard (`"1"`) and multi shard (`"2"`) per node.
/// Heavy (3 servers * N shards); run on demand with `--ignored`.
#[iggy_harness(
    cluster_nodes = 3,
    server(system.sharding.cpu_allocation = ["1", "2"])
)]
#[ignore = "3-node cluster: heavy, run on demand with --ignored"]
async fn given_cluster_when_produce_consume_hammered_should_not_lose_messages(
    harness: &TestHarness,
) {
    run_hammer(harness).await;
}

async fn run_hammer(harness: &TestHarness) {
    tokio::time::timeout(WALL_CLOCK_TIMEOUT, run_hammer_inner(harness))
        .await
        .expect("stress test exceeded WALL_CLOCK_TIMEOUT; server likely crashed at boot or a poll wedged");
}

async fn run_hammer_inner(harness: &TestHarness) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();

    let setup = harness.tcp_root_client().await.unwrap();
    setup.create_stream(STREAM_NAME).await.unwrap();
    setup
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    drop(setup);

    let producer_done = Arc::new(AtomicBool::new(false));

    // Spawn consumers first so they poll concurrently with the producers from
    // the very first send. Each reads the whole partition independently by
    // explicit offset and asserts strict contiguity (no gap = no loss, no dup).
    let mut consumers = Vec::with_capacity(CONSUMERS as usize);
    for consumer_id in 0..CONSUMERS {
        let client = harness.tcp_root_client().await.unwrap();
        let done = producer_done.clone();
        consumers.push(tokio::spawn(consume_partition(client, consumer_id, done)));
    }

    // All producers hammer the single partition for HAMMER_DURATION.
    let mut producers = Vec::with_capacity(PRODUCERS as usize);
    for producer_id in 0..PRODUCERS {
        let client = harness.tcp_root_client().await.unwrap();
        producers.push(tokio::spawn(produce_partition(client, producer_id)));
    }

    // Producers stop at the hammer deadline; sum their sends, then signal
    // consumers to drain.
    let mut total_sent = 0u64;
    for handle in producers {
        total_sent += handle.await.unwrap();
    }
    producer_done.store(true, Ordering::Relaxed);

    assert!(
        total_sent > 0,
        "hammer produced no messages; workload wiring is broken"
    );

    // Every consumer independently read the full partition; each must have seen
    // exactly the committed total, contiguously (asserted inside the task).
    for (consumer_id, handle) in consumers.into_iter().enumerate() {
        let received = handle.await.unwrap();
        assert_eq!(
            received, total_sent,
            "consumer {consumer_id}: consumed {received} != produced {total_sent} (message loss)",
        );
    }

    let cleanup = harness.tcp_root_client().await.unwrap();
    cleanup.delete_stream(&stream_id).await.unwrap();
}

/// Send `PRODUCER_BATCH`-sized batches to the shared partition until the hammer
/// deadline. Returns the number of messages sent (each send awaits commit).
async fn produce_partition(client: IggyClient, producer_id: u32) -> u64 {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let deadline = Instant::now() + HAMMER_DURATION;
    let mut sent = 0u64;

    while Instant::now() < deadline {
        let mut messages: Vec<IggyMessage> = (0..PRODUCER_BATCH)
            .map(|i| {
                IggyMessage::builder()
                    .payload(Bytes::from(format!(
                        "prod{producer_id}-{}",
                        sent + u64::from(i)
                    )))
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .send_messages(&stream, &topic, &partitioning, &mut messages)
            .await
            .unwrap_or_else(|e| panic!("producer {producer_id} send failed at sent={sent}: {e}"));
        sent += u64::from(PRODUCER_BATCH);
    }
    sent
}

/// Read the shared partition in full by explicit offset with `auto_commit =
/// false`, asserting each message arrives at the next contiguous offset. Drains
/// until producers are done and `DRAIN_EMPTY_POLLS` consecutive empty polls
/// confirm the tail. Returns the number of messages received.
async fn consume_partition(
    client: IggyClient,
    consumer_id: u32,
    producer_done: Arc<AtomicBool>,
) -> u64 {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::default();
    let mut next_offset = 0u64;
    let mut received = 0u64;
    let mut consecutive_empty = 0u32;
    let deadline = Instant::now() + MAX_TEST_DURATION;

    loop {
        assert!(
            Instant::now() < deadline,
            "consumer {consumer_id} timed out: received {received}, next_offset {next_offset}"
        );

        let polled = match client
            .poll_messages(
                &stream,
                &topic,
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::offset(next_offset),
                CONSUMER_BATCH,
                false,
            )
            .await
        {
            Ok(polled) => polled,
            Err(e) => {
                // Transient under load; back off and retry.
                eprintln!("consumer {consumer_id} poll error: {e:?}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        if polled.messages.is_empty() {
            if producer_done.load(Ordering::Relaxed) {
                consecutive_empty += 1;
                if consecutive_empty >= DRAIN_EMPTY_POLLS {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }

        consecutive_empty = 0;
        for msg in &polled.messages {
            assert_eq!(
                msg.header.offset, next_offset,
                "consumer {consumer_id} offset gap/dup: expected {next_offset}, got {}",
                msg.header.offset
            );
            next_offset += 1;
            received += 1;
        }
    }

    received
}
