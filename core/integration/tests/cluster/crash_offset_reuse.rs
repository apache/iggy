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

//! Offset identity across a crash.
//!
//! `SendMessagesResponse::confirmations` hands clients concrete base offsets,
//! which makes offset reuse client-visible: a client that recorded offset N for
//! its message must never see the server confirm a DIFFERENT message at N
//! later. A solo node acks below the flush thresholds from RAM only, so nothing
//! in the segments says those offsets were ever handed out. What keeps them
//! from being re-minted is the offset RESERVATION in the partition superblock,
//! claimed by the append fence before any of them exist and read back by boot.

use std::path::Path;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "offset-reuse-stream";
const TOPIC_NAME: &str = "offset-reuse-topic";
const PARTITION_ID: u32 = 0;
/// Confirmed sends before the crash; small enough to stay far below the
/// 1024-message / 1 MiB flush thresholds, so nothing reaches the segments.
const PRE_CRASH_SENDS: u32 = 5;

/// Bounds the restarted node's boot and its consensus groups settling.
const SERVE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Send `count` single-message batches, returning each confirmed base offset.
async fn produce_acked(client: &IggyClient, payload_prefix: &str, count: u32) -> Vec<u64> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut acked = Vec::with_capacity(count as usize);
    for index in 0..count {
        let payload = format!("{payload_prefix}-{index:03}");
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload.clone().into())
                .build()
                .expect("build message"),
        ];
        let response = client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("send {payload}: {error}"));
        acked.push(
            response
                .confirmations
                .first()
                .unwrap_or_else(|| panic!("the VSR server confirms every send, none for {payload}"))
                .base_offset,
        );
    }
    acked
}

/// Poll until the restarted node serves the pre-crash stream again, returning
/// a connected root client. Panics at the deadline.
async fn wait_until_serving(harness: &TestHarness, budget: Duration) -> IggyClient {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Ok(builder) = harness.node(0).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
            && matches!(client.get_stream(&stream).await, Ok(Some(_)))
        {
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted node did not serve the stream within {budget:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Create the stream and its single-partition topic.
async fn create_topic(client: &IggyClient, messages_required_to_save: Option<u32>) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

/// Base offsets of every segment file under `root`, from the file names, which
/// are the on-disk claim about where each range begins.
///
/// Reading them is the only way to assert the shape the re-anchor produces. A
/// black-box offset assertion passes either way on the boot that WRITES the
/// wrong shape; the cost only lands on the boot that reads it back, where the
/// walk refuses and the solo arm tombstones the partition.
fn segment_base_offsets(root: &Path) -> Vec<u64> {
    let mut offsets = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "log")
                && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
                && let Ok(offset) = stem.parse::<u64>()
            {
                offsets.push(offset);
            }
        }
    }
    offsets.sort_unstable();
    offsets
}

/// Poll until the only node has the replicated consumer offset on disk. Panics
/// at the deadline.
async fn wait_for_stored_offset_on_disk(harness: &TestHarness, expected: u64, budget: Duration) {
    let data_path = harness.node(0).data_path();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if integration::harness::disk::read_replicated_consumer_offset(&data_path) == Some(expected)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node did not persist consumer offset {expected} within {budget:?} \
             (found {:?})",
            integration::harness::disk::read_replicated_consumer_offset(&data_path),
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Kill the node, bring it back, and return a client onto the restarted one.
async fn crash_and_recover(harness: &mut TestHarness) -> IggyClient {
    harness.kill_node(0).expect("SIGKILL the only node");
    harness.restart_node(0).expect("restart it");
    wait_until_serving(harness, SERVE_TIMEOUT).await
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_confirmed_sends_below_flush_threshold_when_a_solo_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, None).await;

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    drop(client);

    let client = crash_and_recover(harness).await;
    let post_crash_offset = produce_acked(&client, "post-crash", 1).await[0];

    assert!(
        post_crash_offset > highest_confirmed,
        "a crash-restarted node re-minted offsets it already confirmed: offset \
         {highest_confirmed} was handed to a client before the SIGKILL, yet the first \
         post-restart send was confirmed at offset {post_crash_offset}; without a durable \
         offset watermark the node restarts the partition log below what it acknowledged, \
         so two different messages now share an offset and consumers reading by offset get \
         silently different data"
    );

    // The shape this boot WROTE is only paid for by the boot that reads it back:
    // nothing reached a segment before the crash, so the re-anchor emptied the
    // chain and had to plant at the append point rather than leave the segment
    // named 0 with the first mint a lease block inside it.
    let bases = segment_base_offsets(harness.test_dir());
    assert!(
        bases.iter().any(|&base| base > highest_confirmed),
        "the re-anchor left no segment above the pre-crash offsets, so the post-crash \
         mint landed inside a segment named below it: segment bases {bases:?}, last \
         offset confirmed before the crash {highest_confirmed}"
    );
    drop(client);

    // And the boot that reads it: a hole inside a segment refuses the walk and
    // tombstones the partition, which shows up here as a node that never serves
    // the stream again.
    let client = crash_and_recover(harness).await;
    let third_life = produce_acked(&client, "third-life", 1).await[0];
    assert!(
        third_life > post_crash_offset,
        "the second restart re-minted: {post_crash_offset} was confirmed after the \
         first crash, yet the node came back and handed out {third_life}"
    );
}

/// The graceful stop is the runbook answer to an incident, so it must not be the
/// action that undoes the fix. Between the two boots here the node takes no
/// traffic at all: nothing reaches a segment, the committed frontier stays at
/// what the crash left, and the reservation is the only record that offsets were
/// handed out.
///
/// End to end, not a probe of one mechanism: three separate things carry the
/// append point across this sequence (the boot seed, the segment the re-anchor
/// plants, and the collapse writing the append point rather than the committed
/// frontier), so any one of them alone keeps this green. The collapse is pinned
/// on its own by `given_an_unspent_reservation_when_collapsing_should_leave_it_standing`
/// in `core/partitions`.
#[iggy_harness(cluster_nodes = 1)]
async fn given_a_crash_restarted_node_when_stopped_cleanly_should_still_not_remint_offsets(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, None).await;

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    drop(client);

    // Boot one: reads the reservation back and never spends it.
    let client = crash_and_recover(harness).await;
    drop(client);

    // The clean stop, then boot two. `restart_node` stops the running node with
    // SIGTERM and waits for it, so the shutdown flush and its collapse both run.
    harness
        .restart_node(0)
        .expect("cleanly restart the only node");
    let client = wait_until_serving(harness, SERVE_TIMEOUT).await;
    let post_stop_offset = produce_acked(&client, "post-clean-stop", 1).await[0];

    assert!(
        post_stop_offset > highest_confirmed,
        "a clean stop between the crash and the next send re-minted: offset \
         {highest_confirmed} was handed to a client before the SIGKILL, and after a \
         graceful restart the node confirmed {post_stop_offset}. The shutdown collapse \
         dropped a reservation the boot had not spent yet"
    );
}

/// The combination neither clean-stop nor flushed case covers on its own:
/// `given_a_crash_restarted_node_when_stopped_cleanly_should_still_not_remint_offsets`
/// stops cleanly but takes no traffic between boots, so its collapse only ever
/// sees an UNSPENT reservation, while every flushed case re-enters through
/// SIGKILL and never runs the collapse at all.
///
/// Here the second life spends the reservation, appends past the flush threshold,
/// and then stops gracefully, so the collapse writes an append point over a chain
/// the re-anchor already planted a gap into. The boot that follows has to accept
/// that chain and resume above it; a refusal tombstones the partition and shows
/// up as a node that never serves the stream again.
#[iggy_harness(cluster_nodes = 1)]
async fn given_a_flushed_crash_restarted_node_when_stopped_cleanly_should_still_not_remint_offsets(
    harness: &mut TestHarness,
) {
    const FLUSH_THRESHOLD: u32 = 4;
    /// Past the threshold, so the life leaves a chain and a flushed tail.
    const SENDS_PER_LIFE: u32 = 6;

    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, Some(FLUSH_THRESHOLD)).await;

    let acked = produce_acked(&client, "first-life", SENDS_PER_LIFE).await;
    let first_life_max = *acked.last().expect("confirmed sends");
    drop(client);

    let client = crash_and_recover(harness).await;
    let second_life = produce_acked(&client, "second-life", SENDS_PER_LIFE).await;
    let second_life_max = *second_life.last().expect("confirmed sends");
    assert!(
        second_life[0] > first_life_max,
        "the crash restart re-minted: confirmed {first_life_max} before the SIGKILL, \
         then {} after it",
        second_life[0]
    );
    drop(client);

    // SIGTERM and wait, so the shutdown flush and its collapse both run over the
    // re-anchored chain.
    harness
        .restart_node(0)
        .expect("cleanly restart the only node");
    let client = wait_until_serving(harness, SERVE_TIMEOUT).await;
    let post_stop = produce_acked(&client, "post-clean-stop", 1).await[0];

    assert!(
        post_stop > second_life_max,
        "a clean stop after a spent reservation re-minted: {second_life_max} was \
         confirmed to a client, and the graceful restart handed out {post_stop}. The \
         collapse recorded the committed frontier rather than the append point, so the \
         next boot resumed inside offsets the previous life had already confirmed"
    );

    let bases = segment_base_offsets(harness.test_dir());
    assert!(
        bases.iter().any(|&base| base > first_life_max),
        "the chain lost the re-anchor's plant across the clean stop: bases {bases:?}, \
         last offset confirmed before the crash {first_life_max}"
    );
}

/// The behaviour the reservation is SOLD on, which every other case here leaves
/// implicit: a consumer positioned inside the pre-crash range must keep reading
/// forward across the hole the reservation creates, and must land on the real
/// post-crash message rather than on a phantom offset inside the gap.
///
/// The other cases assert only that newly confirmed offsets rise. That is the
/// producer's half. A consumer that stored a position, survived the crash and
/// then polled `Next` is what actually walks the boundary the re-anchor planted:
/// `disk_poll_start` has to carry the walk on into the segment above the gap,
/// and the stored offset has to still mean the same place.
#[iggy_harness(cluster_nodes = 1)]
async fn given_a_stored_consumer_position_when_polling_across_a_reservation_hole_should_not_miss(
    harness: &mut TestHarness,
) {
    const CONSUMER_ID: u32 = 7;
    const POST_CRASH_PAYLOAD: &str = "across-the-hole-000";

    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, None).await;

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    // Positioned one BELOW the last confirmed offset, so the pre-crash tail is
    // still ahead of the consumer when the node dies. A position at the tail
    // would make the first post-crash poll indistinguishable from a fresh read.
    let stored = highest_confirmed - 1;
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    client
        .store_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID), stored)
        .await
        .expect("store the pre-crash consumer position");
    // Gated on the position reaching DISK before the SIGKILL. The ack is granted
    // at commit and the consumer-offset write is threshold-gated like any other,
    // so without this the crash can legitimately take the position with it and
    // the assertion below races.
    wait_for_stored_offset_on_disk(harness, stored, SERVE_TIMEOUT).await;
    drop(client);

    let client = crash_and_recover(harness).await;
    assert_eq!(
        client
            .get_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID))
            .await
            .expect("read the consumer offset back")
            .expect("the stored position survived the crash")
            .stored_offset,
        stored,
        "the position a consumer committed before the crash must mean the same \
         offset after it, or every consumer silently re-reads or skips"
    );

    // One message above the hole, with a payload no earlier send used, so the
    // poll cannot pass by matching something the pre-crash range already held.
    let post_crash = produce_acked(&client, "across-the-hole", 1).await[0];
    assert!(
        post_crash > highest_confirmed,
        "the premise: the restart must mint above the confirmed range"
    );

    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .expect("poll forward from the stored position");

    let message = polled
        .messages
        .first()
        .unwrap_or_else(|| panic!("`Next` from offset {stored} served nothing at all"));
    assert_eq!(
        String::from_utf8_lossy(&message.payload),
        POST_CRASH_PAYLOAD,
        "the consumer must land on the post-crash message, not on stale bytes or a \
         phantom offset inside the reservation's hole"
    );
    assert_eq!(
        message.header.offset, post_crash,
        "and it must be served at the offset the producer was confirmed at: the hole \
         between {highest_confirmed} and {post_crash} is unwritten offset space, not \
         messages a consumer may be handed"
    );
}

/// The replicated fence path, which no other case here reaches. A three-node
/// group acks a send once a quorum has journaled it, so it claims no reservation
/// and re-anchors nothing -- and one node crashing must still not disturb the
/// offsets the group hands out.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_replicated_group_when_a_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, None).await;

    let acked = produce_acked(&client, "pre-crash", PRE_CRASH_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    assert_eq!(
        acked,
        (0..u64::from(PRE_CRASH_SENDS)).collect::<Vec<_>>(),
        "the pre-crash run mints a contiguous range from zero"
    );
    drop(client);

    let client = crash_and_recover(harness).await;
    let post_crash_offset = produce_acked(&client, "post-crash", 1).await[0];

    assert!(
        post_crash_offset > highest_confirmed,
        "a replicated group re-minted after one node restarted: {highest_confirmed} was \
         confirmed before the SIGKILL, then {post_crash_offset} after it"
    );

    // No reservation was claimed, so no gap was planted: a replicated group's
    // segment boundaries have to stay a function of its batches alone, or the
    // reconciler's offset-keyed segment GC never converges.
    let bases = segment_base_offsets(harness.test_dir());
    assert!(
        bases.iter().all(|&base| base <= post_crash_offset),
        "a replicated group planted a segment above every offset it minted, so it \
         re-anchored around a reservation it should never have claimed: bases {bases:?}"
    );
}

/// The fix has to survive its own side effect: the hole the reservation leaves
/// between the recovered segments and the new append point makes the next boot
/// REFUSE the chain if it lands INSIDE a segment, which tombstones the partition
/// on a solo node.
///
/// So the SECOND crash is the one that matters, and only if the run between the
/// two reaches disk, which is what the flush threshold is for.
#[iggy_harness(cluster_nodes = 1)]
async fn given_a_crash_restarted_node_when_it_flushes_and_crashes_again_should_still_not_remint_offsets(
    harness: &mut TestHarness,
) {
    const FLUSH_THRESHOLD: u32 = 4;
    /// Past the threshold, so every life leaves a chain for the next boot.
    const SENDS_PER_LIFE: u32 = 6;

    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, Some(FLUSH_THRESHOLD)).await;

    let acked = produce_acked(&client, "first-life", SENDS_PER_LIFE).await;
    let first_life_max = *acked.last().expect("confirmed sends");
    drop(client);

    // The boot that consumes a reservation and re-anchors, then flushes the
    // hole's far side to disk.
    let client = crash_and_recover(harness).await;
    let second_life = produce_acked(&client, "second-life", SENDS_PER_LIFE).await;
    let second_life_min = second_life[0];
    let second_life_max = *second_life.last().expect("confirmed sends");
    assert!(
        second_life_min > first_life_max,
        "the first restart re-minted: confirmed {first_life_max} before the crash, \
         then {second_life_min} after it"
    );
    drop(client);

    // ON a segment boundary, not inside one: a segment still named 0 while
    // holding the second life's offsets claims a range it does not have.
    let bases = segment_base_offsets(harness.test_dir());
    assert!(
        bases.iter().any(|&base| base > first_life_max),
        "no segment is anchored above the pre-crash offsets, so the second life \
         appended into a segment named for the first: segment bases {bases:?}, last \
         offset confirmed before the crash {first_life_max}"
    );

    // The first boot that has to read a chain the re-anchor wrote.
    let client = crash_and_recover(harness).await;
    let third_life = produce_acked(&client, "third-life", 1).await[0];
    assert!(
        third_life > second_life_max,
        "the SECOND restart re-minted: {second_life_max} was confirmed between the \
         two crashes, yet the node came back and handed out {third_life}. A hole \
         left INSIDE a segment costs the tail that proved the frontier"
    );
}

/// The partially-flushed shape a real workload crashes in: the run below the
/// threshold is confirmed out of the journal while everything before it is on
/// disk. The recovered chain then ends BELOW the reservation with bytes in it,
/// so the re-anchor has to seal it rather than append into the gap.
#[iggy_harness(cluster_nodes = 1)]
async fn given_sends_straddling_the_flush_threshold_when_the_node_is_killed_should_not_remint_offsets(
    harness: &mut TestHarness,
) {
    const FLUSH_THRESHOLD: u32 = 4;
    const STRADDLING_SENDS: u32 = 6;

    let client = harness.tcp_root_client().await.unwrap();
    create_topic(&client, Some(FLUSH_THRESHOLD)).await;

    let acked = produce_acked(&client, "straddle", STRADDLING_SENDS).await;
    let highest_confirmed = *acked.last().expect("confirmed sends");
    assert_eq!(
        acked,
        (0..u64::from(STRADDLING_SENDS)).collect::<Vec<_>>(),
        "the pre-crash run mints a contiguous range from zero"
    );
    drop(client);

    let client = crash_and_recover(harness).await;
    let post_crash = produce_acked(&client, "post-straddle", 1).await[0];
    assert!(
        post_crash > highest_confirmed,
        "offsets confirmed out of the journal above the last flushed one were \
         re-minted: {highest_confirmed} went to a client before the SIGKILL, and the \
         first send after it was confirmed at {post_crash}"
    );
}
