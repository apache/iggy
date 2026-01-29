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
//! Configuration: 100KB segment size, 100ms cleaner interval, instant flush.
//! Message size: 64B header + 936B payload = 1KB per message.
//! Therefore: 100 messages = 1 segment, 101+ messages = 2+ segments.

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

/// Payload size chosen so that header (64B) + payload = 1KB per message.
const PAYLOAD_SIZE: usize = 936;

/// Buffer time for cleaner to run after expiry conditions are met.
const CLEANER_BUFFER: Duration = Duration::from_millis(300);

fn make_payload(fill: char) -> Bytes {
    Bytes::from(fill.to_string().repeat(PAYLOAD_SIZE))
}

/// Tests time-based retention: segments are cleaned up after expiry.
pub async fn run_expiry_after_rotation(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let expiry = Duration::from_secs(2);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from(expiry)),
            MaxTopicSize::ServerDefault,
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

    // Send 110 messages (1KB each) to create 2 segments (100KB segment size)
    let payload = make_payload('A');
    let total_messages = 110;

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
        total_messages as usize,
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
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from(expiry)),
            MaxTopicSize::ServerDefault,
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

    // 150KB max, cleanup at 90% = 135KB. With 100KB segments, exceeding 135KB triggers cleanup.
    let max_size_bytes = 150 * 1024;
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Custom(IggyByteSize::from(max_size_bytes)),
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

    // Send 160 messages (160KB) to exceed 90% threshold (135KB)
    let payload = make_payload('B');
    let total_messages = 160;

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
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from(expiry)),
            MaxTopicSize::Custom(IggyByteSize::from(500 * 1024)), // 500KB (won't trigger)
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

    // Send 110 messages to create 2 segments (under size threshold, but will expire)
    let payload = make_payload('C');
    for i in 0..110 {
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

    let expiry = Duration::from_secs(3);
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from(expiry)),
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = make_payload('D');
    let messages_per_partition = 110;

    // Send messages to all partitions
    for partition_id in 0..PARTITIONS_COUNT {
        for i in 0..messages_per_partition {
            let msg_id = partition_id as u128 * 1000 + i as u128;
            let message = IggyMessage::builder()
                .id(msg_id)
                .payload(payload.clone())
                .build()
                .unwrap();

            let mut messages = vec![message];
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
        let segments = get_segment_paths_for_partition(&partition_path);
        initial_counts.push(segments.len());
        assert!(
            segments.len() >= 2,
            "Partition {} should have at least 2 segments, got {}",
            partition_id,
            segments.len()
        );
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

/// Tests fair size-based cleanup across multiple partitions.
pub async fn run_fair_size_based_cleanup_multipartition(client: &IggyClient, data_path: &Path) {
    const PARTITIONS_COUNT: u32 = 3;

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    // 200KB max, cleanup at 90% = 180KB
    let max_size_bytes = 200 * 1024;
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Custom(IggyByteSize::from(max_size_bytes)),
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = make_payload('E');

    // Send 70 messages per partition = 210KB total, exceeds 180KB threshold
    for partition_id in 0..PARTITIONS_COUNT {
        for i in 0..70 {
            let msg_id = partition_id as u128 * 1000 + i as u128;
            let message = IggyMessage::builder()
                .id(msg_id)
                .payload(payload.clone())
                .build()
                .unwrap();

            let mut messages = vec![message];
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

    // Verify segments exist for all partitions
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
    }

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
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
