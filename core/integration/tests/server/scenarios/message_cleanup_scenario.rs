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

//! Tests for message retention policies (time-based and size-based).
//!
//! Configuration: 1 MiB segments, 100ms cleaner interval, instant flush. The
//! segment size is a topic creation option now and 1 MiB is the smallest a
//! topic may declare, so the payload carries what a 10 KiB segment used to:
//! one 10-message batch is a hair over 1 MiB on disk, which makes roughly one
//! segment per batch. Every retention assertion below is a lower bound on the
//! segment count, so the exact rotation point does not matter -- only that the
//! volume sent clears the cap several times over.

use bytes::Bytes;
use iggy::prelude::*;
use iggy_common::IggyByteSize;
use std::fs::{DirEntry, read_dir};
use std::path::Path;
use std::time::Duration;

const STREAM_NAME: &str = "test_expiry_stream";
const TOPIC_NAME: &str = "test_expiry_topic";
const PARTITION_ID: u32 = 0;
const LOG_EXTENSION: &str = "log";

/// Smallest segment a topic may declare (`iggy_common::MIN_TOPIC_SEGMENT_SIZE`).
const SEGMENT_SIZE: u64 = 1024 * 1024;

/// Payload size chosen so a 10-message batch lands just past [`SEGMENT_SIZE`]
/// on disk: a 256-byte command header per send plus 48 bytes per message, so
/// 256 + 10 * (48 + 105000) = 1050736 >= 1 MiB.
const PAYLOAD_SIZE: usize = 105_000;

/// Buffer time for cleaner to run after expiry conditions are met.
const CLEANER_BUFFER: Duration = Duration::from_millis(300);

fn make_payload(fill: char) -> Bytes {
    Bytes::from(fill.to_string().repeat(PAYLOAD_SIZE))
}

/// Knobs every topic here shares. The 1 MiB segment gives the retention
/// policies sealed segments to reclaim (an active segment is never deleted),
/// and the flush per message puts a send on disk before the segment count is
/// read -- both were server config before they became topic options.
fn cleanup_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(1),
        segment_size: Some(IggyByteSize::from(SEGMENT_SIZE)),
        enforce_fsync: Some(true),
        messages_required_to_save: Some(1),
        ..TopicCreateOptions::default()
    }
}

/// Tests time-based retention: segments are cleaned up after expiry.
pub async fn run_expiry_after_rotation(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    // The whole send burst has to land inside this window: expiry runs off each
    // message's own timestamp, so a produce run that outlives it gets its
    // oldest segments reclaimed before the pre-expiry poll ever runs. A 3-node
    // vsr cluster in a debug build pays a consensus round-trip plus an fsync
    // per request, which is what pushed the old one-message-per-request loop
    // past the old 2s window.
    let expiry = Duration::from_secs(4);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let partition_path = data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string();

    // Send 40 messages in batches, spanning several 1 MiB segments.
    // Batched rather than one request per message: the burst must fit inside
    // `expiry` with room to spare, and each request costs a round-trip.
    let payload = make_payload('A');
    let total_messages: usize = 40;
    let batch_size = 10;

    for chunk_start in (0..total_messages).step_by(batch_size) {
        let mut messages: Vec<IggyMessage> = (chunk_start
            ..total_messages.min(chunk_start + batch_size))
            .map(|i| {
                IggyMessage::builder()
                    .id(i as u128)
                    .payload(payload.clone())
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    let initial_count = initial_segments.len();
    assert!(
        initial_count >= 2,
        "Expected at least 2 segments but got {}",
        initial_count
    );

    // Verify we can poll all messages before expiry
    let polled_before = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            total_messages as u32,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled_before.messages.len(),
        total_messages,
        "Should poll all messages before expiry"
    );

    // Wait for expiry + cleaner
    tokio::time::sleep(expiry + CLEANER_BUFFER).await;

    let remaining_segments = get_segment_paths_for_partition(&partition_path);
    assert!(
        remaining_segments.len() < initial_count,
        "Expected segments to be deleted after expiry"
    );
    assert!(
        !remaining_segments.is_empty(),
        "Active segment should not be deleted"
    );

    // Verify fewer messages available after cleanup
    let polled_after = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            total_messages as u32,
            false,
        )
        .await
        .unwrap();

    assert!(
        polled_after.messages.len() < polled_before.messages.len(),
        "Expected fewer messages after cleanup"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Tests that the active segment is never deleted, even if expired.
pub async fn run_active_segment_protection(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let expiry = Duration::from_secs(1);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let partition_path = data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string();

    // Send one small message (stays in active segment, no rotation)
    let message = IggyMessage::builder()
        .id(1u128)
        .payload(Bytes::from("small"))
        .build()
        .unwrap();

    let mut messages = vec![message];
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    assert_eq!(initial_segments.len(), 1, "Should have exactly 1 segment");

    // Wait for expiry + cleaner
    tokio::time::sleep(expiry + CLEANER_BUFFER).await;

    let remaining_segments = get_segment_paths_for_partition(&partition_path);
    assert_eq!(
        remaining_segments.len(),
        1,
        "Active segment should NOT be deleted even after expiry"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Tests size-based retention: oldest segments deleted when topic exceeds max_size.
pub async fn run_size_based_retention(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    // 4 MiB max, cleanup at 90% = 3.6 MiB. The cap has to sit above the 1 MiB
    // segment size, or the topic would hit it before a single segment sealed and
    // the cleaner would have nothing it is allowed to reclaim.
    let max_size_bytes = 4 * 1024 * 1024;
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                message_expiry: Some(IggyExpiry::NeverExpire),
                max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(max_size_bytes))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let partition_path = data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string();

    // Send 80 messages (~8 MiB) to clear the 3.6 MiB threshold several times over
    let payload = make_payload('B');
    let total_messages = 80;

    for i in 0..total_messages {
        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(payload.clone())
            .build()
            .unwrap();

        let mut messages = vec![message];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // Wait for cleaner
    tokio::time::sleep(CLEANER_BUFFER).await;

    let remaining_segments = get_segment_paths_for_partition(&partition_path);

    // Verify oldest messages deleted (first offset > 0)
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            total_messages as u32,
            false,
        )
        .await
        .unwrap();

    let first_offset = polled
        .messages
        .first()
        .map(|m| m.header.offset)
        .unwrap_or(0);

    assert!(
        first_offset > 0,
        "Oldest messages should be deleted, first_offset should be > 0"
    );
    assert!(
        polled.messages.len() < total_messages as usize,
        "Some messages should be deleted"
    );
    assert!(
        !remaining_segments.is_empty(),
        "Active segment should not be deleted"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Tests both retention policies together: time-based AND size-based.
pub async fn run_combined_retention(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let expiry = Duration::from_secs(2);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
                // 500 MiB (won't trigger)
                max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(500 * 1024 * 1024))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let partition_path = data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string();

    // Send 40 messages to create several segments (under size threshold, but
    // will expire). Batched so the burst finishes well inside `expiry`: a
    // per-message request pays a consensus round-trip plus an fsync, and a loop
    // that outlives the window has its head reclaimed before the count below.
    let payload = make_payload('C');
    let total_messages: usize = 40;
    let batch_size = 10;
    for chunk_start in (0..total_messages).step_by(batch_size) {
        let mut messages: Vec<IggyMessage> = (chunk_start
            ..total_messages.min(chunk_start + batch_size))
            .map(|i| {
                IggyMessage::builder()
                    .id(i as u128)
                    .payload(payload.clone())
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    let initial_count = initial_segments.len();
    assert!(initial_count >= 2, "Expected at least 2 segments");

    // Wait for time-based expiry
    tokio::time::sleep(expiry + CLEANER_BUFFER).await;

    let remaining_segments = get_segment_paths_for_partition(&partition_path);
    assert!(
        remaining_segments.len() < initial_count,
        "Segments should be deleted after expiry"
    );
    assert!(
        !remaining_segments.is_empty(),
        "Active segment should not be deleted"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Tests time-based retention with multiple partitions.
pub async fn run_expiry_with_multiple_partitions(client: &IggyClient, data_path: &Path) {
    const PARTITIONS_COUNT: u32 = 3;

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let expiry = Duration::from_secs(5);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = make_payload('D');
    let messages_per_partition: usize = 40;
    let batch_size = 10;

    // Send messages to all partitions. Batched: a per-message request costs a
    // consensus round-trip plus an fsync, and `PARTITIONS_COUNT` × 110 of those
    // outlive `expiry`, so the cleaner would reclaim the first partition's
    // sealed segments before the last one had even been written.
    for partition_id in 0..PARTITIONS_COUNT {
        for chunk_start in (0..messages_per_partition).step_by(batch_size) {
            let mut messages: Vec<IggyMessage> = (chunk_start
                ..messages_per_partition.min(chunk_start + batch_size))
                .map(|i| {
                    IggyMessage::builder()
                        .id(partition_id as u128 * 1000 + i as u128)
                        .payload(payload.clone())
                        .build()
                        .unwrap()
                })
                .collect();
            client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    // Collect initial segment counts
    let mut initial_counts: Vec<usize> = Vec::new();

    // Wait until all partitions have >= 2 segments (up to 5s)
    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_path
            .join(format!(
                "streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}"
            ))
            .display()
            .to_string();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let count = loop {
            let segments = get_segment_paths_for_partition(&partition_path);
            if segments.len() >= 2 {
                break segments.len();
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Partition {} should have at least 2 segments after 5s, got {}",
                    partition_id,
                    segments.len()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        initial_counts.push(count);
    }

    // Wait for expiry + cleaner
    tokio::time::sleep(expiry + CLEANER_BUFFER).await;

    // Verify cleanup in all partitions
    let mut total_deleted = 0usize;
    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_path
            .join(format!(
                "streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}"
            ))
            .display()
            .to_string();
        let remaining = get_segment_paths_for_partition(&partition_path);
        let deleted = initial_counts[partition_id as usize].saturating_sub(remaining.len());
        total_deleted += deleted;

        assert!(
            !remaining.is_empty(),
            "Partition {} should retain active segment",
            partition_id
        );
    }

    assert!(
        total_deleted > 0,
        "At least some segments should be deleted"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Tests fair size-based cleanup across multiple partitions: the topic cap is
/// enforced as a per-partition SHARE, not as a topic-wide total.
pub async fn run_fair_size_based_cleanup_multipartition(client: &IggyClient, data_path: &Path) {
    const PARTITIONS_COUNT: u32 = 3;

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    // 12 MiB over 3 partitions = a 4 MiB share each. The SHARE is what has to
    // clear the retention floor of one sealed segment (`segment_size` plus the
    // bus message cap), not the topic-wide figure: a cap that looks large
    // enough before the divisor still leaves every partition floored and
    // reclaiming nothing.
    let max_size_bytes = 12 * 1024 * 1024;
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::NeverExpire),
                max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(max_size_bytes))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = make_payload('E');

    // A 10-message batch is already past the 1 MiB segment size on its own and
    // the crossing batch is written whole, so each batch seals exactly one
    // segment: 7 batches leave ~7 MiB of SEALED bytes per partition plus an
    // empty active one, well past the 4 MiB share. Batched rather than one
    // request per message because each request costs a consensus round-trip
    // plus an fsync.
    let batches_per_partition = 7u32;
    let batch_size = 10u32;
    let messages_per_partition = batches_per_partition * batch_size;
    for partition_id in 0..PARTITIONS_COUNT {
        for batch in 0..batches_per_partition {
            let mut messages: Vec<IggyMessage> = (0..batch_size)
                .map(|i| {
                    IggyMessage::builder()
                        .id(u128::from(partition_id * 1000 + batch * batch_size + i))
                        .payload(payload.clone())
                        .build()
                        .unwrap()
                })
                .collect();
            client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    // Wait for cleaner
    tokio::time::sleep(CLEANER_BUFFER).await;

    // Verify every partition kept an active segment and lost its oldest ones.
    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_path
            .join(format!(
                "streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}"
            ))
            .display()
            .to_string();
        let segments = get_segment_paths_for_partition(&partition_path);
        assert!(
            !segments.is_empty(),
            "Partition {} should have at least 1 segment",
            partition_id
        );

        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(partition_id),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                messages_per_partition,
                false,
            )
            .await
            .unwrap();
        let first_offset = polled
            .messages
            .first()
            .map(|m| m.header.offset)
            .unwrap_or(0);

        // The divisor is what this asserts: ~7 MiB per partition never reaches
        // the 12 MiB topic-wide figure, so a cap enforced without dividing by
        // the partition count would delete nothing here.
        assert!(
            first_offset > 0,
            "Partition {} should have lost its oldest messages to the per-partition share, \
             got first_offset {}",
            partition_id,
            first_offset
        );
    }

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

/// Reproduces Bug 2 from #2924: the time-based cleaner deletes expired segments
/// without checking whether consumers have read them. The size-based
/// `delete_oldest_segments` checks `min_committed_offset` but the time-based
/// `delete_expired_segments_for_partition` does not.
///
/// Scenario:
/// 1. Send 100 messages (several 1 MiB segments)
/// 2. Consumer reads only 50 messages (stored offset ~49, within segment 0)
/// 3. Wait for all segments to expire (4s expiry)
/// 4. Verify consumer can still poll Next() and get contiguous offsets
///
/// On unfixed code: the cleaner deletes segments 0+1 (expired, consumer offset
/// not checked), consumer's Next() jumps to segment 2, skipping ~250 messages.
pub async fn run_expiry_respects_consumer_offset(client: &IggyClient, data_path: &Path) {
    const TEST_STREAM: &str = "test_cleaner_barrier_stream";
    const TEST_TOPIC: &str = "test_cleaner_barrier_topic";

    let stream = client.create_stream(TEST_STREAM).await.unwrap();
    let stream_id = stream.id;

    // Expiry must outlast the send + first-poll phase. If segments expire
    // before the consumer commits its first offset, there is no barrier yet
    // and the cleaner legally deletes them, breaking the premise: the poll
    // below then starts at the earliest surviving offset instead of 0.
    // The sends are batched for the same reason (see below).
    let expiry = Duration::from_secs(4);
    let topic = client
        .create_topic(
            &Identifier::named(TEST_STREAM).unwrap(),
            TEST_TOPIC,
            &TopicCreateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(expiry))),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let partition_path = data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string();

    // Send 100 messages -> several sealed segments + active. Batched: one
    // request per message costs a consensus round-trip plus an fsync each,
    // which on a 3-node debug cluster runs the burst well past `expiry`.
    let payload = make_payload('B');
    let total_messages = 100u32;
    let batch_size = 10u32;
    for chunk_start in (0..total_messages).step_by(batch_size as usize) {
        let mut messages: Vec<IggyMessage> = (chunk_start
            ..total_messages.min(chunk_start + batch_size))
            .map(|i| {
                IggyMessage::builder()
                    .id(i as u128)
                    .payload(payload.clone())
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .send_messages(
                &Identifier::named(TEST_STREAM).unwrap(),
                &Identifier::named(TEST_TOPIC).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    assert!(
        initial_segments.len() >= 3,
        "Need at least 3 segments, got {}",
        initial_segments.len()
    );

    // Consumer reads only 50 messages with auto_commit, storing offset ~49.
    // This means the consumer has NOT read segments 1, 2, etc.
    let consumer = Consumer::new(Identifier::numeric(42).unwrap());
    let mut consumed_offsets = Vec::new();
    let mut remaining = 50u32;
    while remaining > 0 {
        let batch_size = remaining.min(10);
        let polled = client
            .poll_messages(
                &Identifier::named(TEST_STREAM).unwrap(),
                &Identifier::named(TEST_TOPIC).unwrap(),
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::next(),
                batch_size,
                true, // auto_commit
            )
            .await
            .unwrap();
        for msg in &polled.messages {
            consumed_offsets.push(msg.header.offset);
        }
        remaining -= polled.messages.len() as u32;
    }
    let last_committed = *consumed_offsets.last().unwrap();
    assert_eq!(
        last_committed, 49,
        "Consumer should have read through offset 49"
    );

    // Wait for expiry + cleaner buffer
    tokio::time::sleep(expiry + CLEANER_BUFFER + CLEANER_BUFFER).await;

    // Now poll Next() - consumer should continue from offset 50 without gaps.
    // BUG: on unfixed code, the cleaner deleted the segment holding offset 50
    // (expired, no consumer barrier check), so Next() jumps past it.
    let polled = client
        .poll_messages(
            &Identifier::named(TEST_STREAM).unwrap(),
            &Identifier::named(TEST_TOPIC).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::next(),
            10,
            false,
        )
        .await
        .unwrap();

    assert!(
        !polled.messages.is_empty(),
        "Consumer should still be able to poll messages after expiry"
    );

    let first_offset = polled.messages[0].header.offset;
    assert_eq!(
        first_offset,
        last_committed + 1,
        "BUG #2924: cleaner deleted unconsumed segments! \
         Expected next offset {}, got {} (skipped {} messages)",
        last_committed + 1,
        first_offset,
        first_offset - last_committed - 1,
    );

    client
        .delete_stream(&Identifier::named(TEST_STREAM).unwrap())
        .await
        .unwrap();
}

fn get_segment_paths_for_partition(partition_path: &str) -> Vec<DirEntry> {
    read_dir(partition_path)
        .map(|read_dir| {
            read_dir
                .filter_map(|dir_entry| {
                    dir_entry
                        .map(|dir_entry| {
                            match dir_entry
                                .path()
                                .extension()
                                .is_some_and(|ext| ext == LOG_EXTENSION)
                            {
                                true => Some(dir_entry),
                                false => None,
                            }
                        })
                        .ok()
                        .flatten()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}
