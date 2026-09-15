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

const MESSAGE_EXPIRY: Duration = Duration::from_millis(100);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
        message_expiry: Some(IggyExpiry::NeverExpire),
        segment_size: Some(IggyByteSize::from(SEGMENT_SIZE)),
        durability: iggy_common::Durability::Persisted,
        messages_required_to_save: Some(1),
        ..TopicCreateOptions::default()
    }
}

/// Tests time-based retention: segments are cleaned up after expiry.
pub async fn run_expiry_after_rotation(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &cleanup_topic_options(),
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

    expire_messages(client, STREAM_NAME, TOPIC_NAME).await;
    wait_for_segment_cleanup(&partition_path, initial_count).await;

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

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
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

    expire_messages(client, STREAM_NAME, TOPIC_NAME).await;
    wait_for_segment_cleanup(&partition_path, initial_count).await;

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

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                ..cleanup_topic_options()
            },
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = make_payload('D');
    let messages_per_partition: usize = 40;
    let batch_size = 10;

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

    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_path
            .join(format!(
                "streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}"
            ))
            .display()
            .to_string();

        let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        let count = loop {
            let segments = get_segment_paths_for_partition(&partition_path);
            if segments.len() >= 2 {
                break segments.len();
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Partition {partition_id} should have at least 2 segments after {CLEANUP_TIMEOUT:?}, got {}",
                    segments.len()
                );
            }
            tokio::time::sleep(CLEANUP_POLL_INTERVAL).await;
        };
        initial_counts.push(count);
    }

    expire_messages(client, STREAM_NAME, TOPIC_NAME).await;

    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_path
            .join(format!(
                "streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}"
            ))
            .display()
            .to_string();
        wait_for_segment_cleanup(&partition_path, initial_counts[partition_id as usize]).await;
    }

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
/// 2. Consumer reads only 50 messages and stores offset 49
/// 3. Enable expiry and wait for consumed segments to be removed
/// 4. Verify consumer can still poll Next() and get contiguous offsets
///
/// Without the consumer barrier, cleanup also removes the unconsumed segments.
pub async fn run_expiry_respects_consumer_offset(client: &IggyClient, data_path: &Path) {
    const TEST_STREAM: &str = "test_cleaner_barrier_stream";
    const TEST_TOPIC: &str = "test_cleaner_barrier_topic";

    let stream = client.create_stream(TEST_STREAM).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(TEST_STREAM).unwrap(),
            TEST_TOPIC,
            &cleanup_topic_options(),
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
        assert!(
            !polled.messages.is_empty(),
            "messages must remain available before expiry is enabled"
        );
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

    expire_messages(client, TEST_STREAM, TEST_TOPIC).await;
    wait_for_segment_cleanup(&partition_path, initial_segments.len()).await;

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

async fn expire_messages(client: &IggyClient, stream: &str, topic: &str) {
    // Slow setup must not consume the expiry window before the assertions are ready.
    client
        .update_topic(
            &Identifier::named(stream).unwrap(),
            &Identifier::named(topic).unwrap(),
            topic,
            &TopicUpdateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                    MESSAGE_EXPIRY,
                ))),
                ..TopicUpdateOptions::default()
            },
        )
        .await
        .unwrap();
    // Every previously sent batch must be eligible, including unconsumed batches.
    tokio::time::sleep(MESSAGE_EXPIRY).await;
}

async fn wait_for_segment_cleanup(partition_path: &str, initial_count: usize) {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let remaining = get_segment_paths_for_partition(partition_path).len();
        if remaining > 0 && remaining < initial_count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cleanup must remove sealed segments and retain the active segment in {partition_path}: initial={initial_count}, remaining={remaining} after {CLEANUP_TIMEOUT:?}"
        );
        tokio::time::sleep(CLEANUP_POLL_INTERVAL).await;
    }
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
