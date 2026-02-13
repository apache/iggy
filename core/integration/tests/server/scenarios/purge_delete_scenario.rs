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

use bytes::Bytes;
use iggy::prelude::*;
use iggy_common::Credentials;
use integration::harness::TestHarness;
use std::fs::{metadata, read_dir};
use std::path::Path;
use std::str::FromStr;

const STREAM_NAME: &str = "test_stream";
const TOPIC_NAME: &str = "test_topic";
const PARTITION_ID: u32 = 0;
const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

/// Payload chosen so IGGY_MESSAGE_HEADER_SIZE + payload = 1000B per message on disk.
///
/// Rotation mechanics (with segment.size = 5KiB = 5120B, messages_required_to_save = 1):
///   `is_full()` checks `size >= 5120` BEFORE persisting the current message.
///   After 6 persisted messages (6000B >= 5120) the next arrival sees is_full=true,
///   gets persisted into the same segment, then rotation fires.
///   Result: 7 messages per sealed segment (7000B on disk).
const PAYLOAD_SIZE: usize = 936;
const MESSAGE_ON_DISK_SIZE: u64 = IGGY_MESSAGE_HEADER_SIZE as u64 + PAYLOAD_SIZE as u64;
const INDEX_SIZE_PER_MSG: u64 = INDEX_SIZE as u64;
const TOTAL_MESSAGES: u32 = 25;

/// 3 sealed segments (7 msgs each) + 1 active (4 msgs at offsets 21-24).
const EXPECTED_SEGMENT_OFFSETS: [u64; 4] = [0, 7, 14, 21];
const MSGS_PER_SEALED_SEGMENT: u64 = 7;

/// Single consumer barrier: oldest-first deletion, barrier advancement, and edge cases.
///
/// Covers: barrier blocks deletion, advancing barrier releases segments, delete(0) no-op,
/// delete(u32::MAX) bulk, consumer not stuck after deletion, error cases for invalid IDs.
pub async fn run(harness: &mut TestHarness, restart_server: bool) {
    let client = build_root_client(harness);
    client.connect().await.unwrap();
    let data_path = harness.server().data_path().to_path_buf();

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let stream_ident = Identifier::named(STREAM_NAME).unwrap();
    let topic_ident = Identifier::named(TOPIC_NAME).unwrap();

    send_messages(&client, &stream_ident, &topic_ident, TOTAL_MESSAGES).await;

    let partition_path = partition_path(&data_path, stream_id, topic_id);

    // --- Verify exact segment layout ---
    let segment_offsets = get_sorted_segment_offsets(&partition_path);
    assert_eq!(
        segment_offsets, EXPECTED_SEGMENT_OFFSETS,
        "Segment layout must match calculated offsets"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS);

    let all_offsets = poll_all_offsets(&client, &stream_ident, &topic_ident).await;
    let expected_offsets: Vec<u64> = (0..TOTAL_MESSAGES as u64).collect();
    assert_eq!(all_offsets, expected_offsets);

    // --- Consumer offset barrier ---
    //
    // stored_offset = 7 (start of segment 1). Segment 0 end_offset = 6 <= 7 → deletable.
    // Segment 1 end_offset = 13 > 7 → protected by barrier.
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };
    let stored_offset = EXPECTED_SEGMENT_OFFSETS[1]; // 7
    let seg1_end_offset = EXPECTED_SEGMENT_OFFSETS[2] - 1; // 13
    client
        .store_consumer_offset(
            &consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            stored_offset,
        )
        .await
        .unwrap();

    // --- Delete 1 oldest segment ---
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 1)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS[1..],
        "Segment 0 deleted → [7, 14, 21]"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[1..]);
    assert_eq!(
        poll_all_offsets(&client, &stream_ident, &topic_ident).await,
        (MSGS_PER_SEALED_SEGMENT..TOTAL_MESSAGES as u64).collect::<Vec<_>>(),
        "Messages 7..25 survive"
    );

    // --- Barrier prevents deletion ---
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 1)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS[1..],
        "Segment 1 (end_offset={seg1_end_offset}) blocked: consumer at {stored_offset}"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[1..]);

    // --- Advance consumer past segment 1, delete it ---
    client
        .store_consumer_offset(
            &consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            seg1_end_offset,
        )
        .await
        .unwrap();

    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 1)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS[2..],
        "Segment 1 deleted → [14, 21]"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[2..]);
    assert_eq!(
        poll_all_offsets(&client, &stream_ident, &topic_ident).await,
        (2 * MSGS_PER_SEALED_SEGMENT..TOTAL_MESSAGES as u64).collect::<Vec<_>>(),
        "Messages 14..25 survive"
    );

    // --- Consumer not stuck ---
    let polled_next = client
        .poll_messages(
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::next(),
            100,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        polled_next.messages[0].header.offset, EXPECTED_SEGMENT_OFFSETS[2],
        "Next poll resumes at offset 14 (first message after stored_offset 13)"
    );

    // --- delete(0) is a no-op ---
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 0)
        .await
        .unwrap();
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS[2..]
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[2..]);

    // --- delete(u32::MAX) with consumer past all sealed segments ---
    client
        .store_consumer_offset(
            &consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            (TOTAL_MESSAGES - 1) as u64,
        )
        .await
        .unwrap();

    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [EXPECTED_SEGMENT_OFFSETS[3]],
        "Only active segment at offset 21 survives"
    );
    assert_no_orphaned_segment_files(&partition_path, 1);
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[3..]);
    assert_eq!(
        poll_all_offsets(&client, &stream_ident, &topic_ident).await,
        (EXPECTED_SEGMENT_OFFSETS[3]..TOTAL_MESSAGES as u64).collect::<Vec<_>>(),
        "Messages 21..25 survive in active segment"
    );

    // --- Error cases ---
    assert!(
        client
            .delete_segments(
                &Identifier::numeric(999).unwrap(),
                &topic_ident,
                PARTITION_ID,
                1,
            )
            .await
            .is_err(),
        "Non-existent stream"
    );
    assert!(
        client
            .delete_segments(
                &stream_ident,
                &Identifier::numeric(999).unwrap(),
                PARTITION_ID,
                1,
            )
            .await
            .is_err(),
        "Non-existent topic"
    );
    assert!(
        client
            .delete_segments(&stream_ident, &topic_ident, 999, 1)
            .await
            .is_err(),
        "Non-existent partition"
    );

    // Cleanup
    client
        .delete_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    client.delete_stream(&stream_ident).await.unwrap();
}

/// No consumers — no barrier: sealed segments are unconditionally deletable.
///
/// Deletes all 3 sealed segments one by one, verifying .log/.index file sizes and
/// surviving message offsets after each. Active segment is never deleted.
pub async fn run_no_consumers(harness: &mut TestHarness, restart_server: bool) {
    let client = build_root_client(harness);
    client.connect().await.unwrap();
    let data_path = harness.server().data_path().to_path_buf();

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let stream_ident = Identifier::named(STREAM_NAME).unwrap();
    let topic_ident = Identifier::named(TOPIC_NAME).unwrap();

    send_messages(&client, &stream_ident, &topic_ident, TOTAL_MESSAGES).await;

    let partition_path = partition_path(&data_path, stream_id, topic_id);

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS,
        "Segment layout must match calculated offsets"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS);
    assert_eq!(
        poll_all_offsets(&client, &stream_ident, &topic_ident).await,
        (0..TOTAL_MESSAGES as u64).collect::<Vec<_>>()
    );

    // Delete 3 sealed segments one by one
    let sealed_count = EXPECTED_SEGMENT_OFFSETS.len() - 1;
    for i in 0..sealed_count {
        client
            .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 1)
            .await
            .unwrap();
        maybe_restart(harness, restart_server).await;

        let first_surviving = EXPECTED_SEGMENT_OFFSETS[i + 1];
        assert_eq!(
            get_sorted_segment_offsets(&partition_path),
            EXPECTED_SEGMENT_OFFSETS[i + 1..],
            "After deleting {n} sealed segment(s)",
            n = i + 1
        );
        assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[i + 1..]);
        assert_eq!(
            poll_all_offsets(&client, &stream_ident, &topic_ident).await,
            (first_surviving..TOTAL_MESSAGES as u64).collect::<Vec<_>>(),
            "Messages from offset {first_surviving} onward survive"
        );
    }

    // Only active segment remains — delete is a no-op
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [EXPECTED_SEGMENT_OFFSETS[3]],
        "Only active segment at offset 21"
    );
    assert_no_orphaned_segment_files(&partition_path, 1);

    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, 1)
        .await
        .unwrap();

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [EXPECTED_SEGMENT_OFFSETS[3]],
        "No-op: active segment protected"
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS[3..]);
    assert_eq!(
        poll_all_offsets(&client, &stream_ident, &topic_ident).await,
        (EXPECTED_SEGMENT_OFFSETS[3]..TOTAL_MESSAGES as u64).collect::<Vec<_>>(),
        "Active segment messages still pollable"
    );

    // Cleanup
    client
        .delete_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    client.delete_stream(&stream_ident).await.unwrap();
}

/// Single consumer group barrier with message-by-message progression.
///
/// Polls one message at a time (next + auto_commit), attempts delete_segments(u32::MAX) after
/// each poll. Verifies that each sealed segment is released exactly when the committed offset
/// reaches its end_offset — not one message earlier, not one later.
///
/// No restart_server variant — 25 delete_segments calls would mean 25 restarts.
pub async fn run_consumer_group_barrier(client: &IggyClient, data_path: &Path) {
    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let stream_ident = Identifier::named(STREAM_NAME).unwrap();
    let topic_ident = Identifier::named(TOPIC_NAME).unwrap();

    send_messages(client, &stream_ident, &topic_ident, TOTAL_MESSAGES).await;

    let partition_path = partition_path(data_path, stream_id, topic_id);

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS
    );
    assert_segment_file_sizes(&partition_path, &EXPECTED_SEGMENT_OFFSETS);

    // Use high-level consumer group API: auto-creates group, auto-joins, auto-commits
    let mut consumer = client
        .consumer_group("test_group", STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .batch_length(1)
        .build();
    consumer.init().await.unwrap();

    let mut expected_segments = EXPECTED_SEGMENT_OFFSETS.to_vec();

    for offset in 0..TOTAL_MESSAGES as u64 {
        use futures::StreamExt;
        let message = consumer
            .next()
            .await
            .expect("stream ended prematurely")
            .unwrap();

        assert_eq!(
            message.message.header.offset, offset,
            "Expected message at offset {offset}"
        );

        while expected_segments.len() >= 2 {
            let seg_end = expected_segments[1] - 1;
            if segment_deletable(seg_end, offset) {
                expected_segments.remove(0);
            } else {
                break;
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        client
            .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
            .await
            .unwrap();

        assert_eq!(
            get_sorted_segment_offsets(&partition_path),
            expected_segments,
            "After consuming offset {offset}"
        );
        assert_segment_file_sizes(&partition_path, &expected_segments);
    }

    assert_eq!(
        expected_segments,
        [EXPECTED_SEGMENT_OFFSETS[3]],
        "Only active segment remains after consuming all messages"
    );
    assert_no_orphaned_segment_files(&partition_path, 1);

    // Cleanup: consumer group auto-managed, just delete stream resources
    drop(consumer);
    client
        .delete_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    client.delete_stream(&stream_ident).await.unwrap();
}

/// Multiple consumers: the slowest consumer gates deletion for all.
///
/// A consumer group ("fast") has consumed everything. A standalone consumer ("slow") lags behind.
/// The barrier is `min(fast, slow)`, so deletion is entirely gated by the slow consumer.
/// Advances the slow consumer through each segment boundary, verifying that segments are
/// released only when `segment_deletable(seg_end, barrier)` becomes true.
pub async fn run_multi_consumer_barrier(harness: &mut TestHarness, restart_server: bool) {
    let client = build_root_client(harness);
    client.connect().await.unwrap();
    let data_path = harness.server().data_path().to_path_buf();

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let stream_ident = Identifier::named(STREAM_NAME).unwrap();
    let topic_ident = Identifier::named(TOPIC_NAME).unwrap();

    send_messages(&client, &stream_ident, &topic_ident, TOTAL_MESSAGES).await;

    let partition_path = partition_path(&data_path, stream_id, topic_id);

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS
    );

    // --- Fast consumer group: poll all messages with auto_commit via high-level API ---
    let mut fast_consumer = client
        .consumer_group("fast_group", STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(TOTAL_MESSAGES)
        .build();
    fast_consumer.init().await.unwrap();

    {
        use futures::StreamExt;
        let mut consumed = 0u32;
        while let Some(msg) = fast_consumer.next().await {
            msg.unwrap();
            consumed += 1;
            if consumed >= TOTAL_MESSAGES {
                break;
            }
        }
        assert_eq!(consumed, TOTAL_MESSAGES);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --- Set up slow standalone consumer: store offset at 0 ---
    let slow_consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };
    client
        .store_consumer_offset(
            &slow_consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            0,
        )
        .await
        .unwrap();

    // Phase 1: barrier=min(24,0)=0 → segment_deletable(6, 0)=false → nothing deleted
    assert!(!segment_deletable(6, 0));
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS,
        "Phase 1: segment_deletable(6, 0)=false, all segments protected"
    );

    // Phase 2: slow→6, barrier=6 → segment_deletable(6, 6)=true → seg0 released
    assert!(segment_deletable(6, 6));
    client
        .store_consumer_offset(
            &slow_consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            6,
        )
        .await
        .unwrap();
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [7, 14, 21],
        "Phase 2: segment_deletable(6, 6)=true, seg0 released"
    );

    // Phase 3: slow→10, barrier=10 → segment_deletable(13, 10)=false → seg1 protected
    assert!(!segment_deletable(13, 10));
    client
        .store_consumer_offset(
            &slow_consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            10,
        )
        .await
        .unwrap();
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [7, 14, 21],
        "Phase 3: segment_deletable(13, 10)=false, seg1 protected"
    );

    // Phase 4: slow→13, barrier=13 → segment_deletable(13, 13)=true → seg1 released
    assert!(segment_deletable(13, 13));
    client
        .store_consumer_offset(
            &slow_consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            13,
        )
        .await
        .unwrap();
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [14, 21],
        "Phase 4: segment_deletable(13, 13)=true, seg1 released"
    );

    // Phase 5: slow→20, barrier=20 → segment_deletable(20, 20)=true → seg2 released
    assert!(segment_deletable(20, 20));
    client
        .store_consumer_offset(
            &slow_consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            20,
        )
        .await
        .unwrap();
    client
        .delete_segments(&stream_ident, &topic_ident, PARTITION_ID, u32::MAX)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;
    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        [21],
        "Phase 5: segment_deletable(20, 20)=true, only active segment remains"
    );
    assert_no_orphaned_segment_files(&partition_path, 1);

    // Cleanup: drop high-level consumer, delete stream resources
    drop(fast_consumer);
    client
        .delete_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    client.delete_stream(&stream_ident).await.unwrap();
}

/// purge_topic is a full reset: consumer offsets wiped (memory + disk), segments deleted,
/// partition restarted at offset 0.
///
/// Sets up both a standalone consumer offset and a consumer group offset, purges, then
/// verifies: in-memory offsets return None, offset files deleted from disk, single empty
/// segment at offset 0, new messages start at offset 0.
pub async fn run_purge_topic(harness: &mut TestHarness, restart_server: bool) {
    let client = build_root_client(harness);
    client.connect().await.unwrap();
    let data_path = harness.server().data_path().to_path_buf();

    let stream = client.create_stream(STREAM_NAME).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    let stream_ident = Identifier::named(STREAM_NAME).unwrap();
    let topic_ident = Identifier::named(TOPIC_NAME).unwrap();

    send_messages(&client, &stream_ident, &topic_ident, TOTAL_MESSAGES).await;

    let partition_path = partition_path(&data_path, stream_id, topic_id);

    assert_eq!(
        get_sorted_segment_offsets(&partition_path),
        EXPECTED_SEGMENT_OFFSETS
    );

    // --- Store individual consumer offset at 13 ---
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };
    client
        .store_consumer_offset(
            &consumer,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            13,
        )
        .await
        .unwrap();

    // --- Consumer group: poll 10 messages → group offset at 9 via high-level API ---
    let mut group_consumer = client
        .consumer_group("purge_group", STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(10)
        .build();
    group_consumer.init().await.unwrap();

    {
        use futures::StreamExt;
        let mut consumed = 0u32;
        while let Some(msg) = group_consumer.next().await {
            msg.unwrap();
            consumed += 1;
            if consumed >= 10 {
                break;
            }
        }
        assert_eq!(consumed, 10);
    }

    // Need the group ID for get_consumer_offset verification
    let group_details = client
        .get_consumer_group(
            &stream_ident,
            &topic_ident,
            &Identifier::named("purge_group").unwrap(),
        )
        .await
        .unwrap()
        .expect("purge_group must exist");
    let group_ident = Identifier::numeric(group_details.id).unwrap();
    let group_consumer_ref = Consumer {
        kind: ConsumerKind::ConsumerGroup,
        id: group_ident.clone(),
    };

    // Verify both offsets are stored
    let consumer_offset = client
        .get_consumer_offset(&consumer, &stream_ident, &topic_ident, Some(PARTITION_ID))
        .await
        .unwrap();
    assert!(consumer_offset.is_some(), "Consumer offset must be stored");
    assert_eq!(consumer_offset.unwrap().stored_offset, 13);

    let group_offset = client
        .get_consumer_offset(
            &group_consumer_ref,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
        )
        .await
        .unwrap();
    assert!(
        group_offset.is_some(),
        "Consumer group offset must be stored"
    );
    assert_eq!(group_offset.unwrap().stored_offset, 9);

    // Verify offset files exist on disk
    let consumers_dir = format!("{partition_path}/offsets/consumers");
    let groups_dir = format!("{partition_path}/offsets/groups");
    assert!(
        !is_dir_empty(&consumers_dir),
        "Consumer offset file must exist before purge"
    );
    assert!(
        !is_dir_empty(&groups_dir),
        "Consumer group offset file must exist before purge"
    );

    // --- Purge topic ---
    // Drop high-level consumer before purge to release group membership
    drop(group_consumer);
    client
        .purge_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    maybe_restart(harness, restart_server).await;

    // --- Verify consumer offsets cleared ---
    let consumer_offset = client
        .get_consumer_offset(&consumer, &stream_ident, &topic_ident, Some(PARTITION_ID))
        .await
        .unwrap();
    assert!(
        consumer_offset.is_none(),
        "Consumer offset must be cleared after purge"
    );

    let group_offset = client
        .get_consumer_offset(
            &group_consumer_ref,
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
        )
        .await
        .unwrap();
    assert!(
        group_offset.is_none(),
        "Consumer group offset must be cleared after purge"
    );

    // --- Verify offset files deleted from disk ---
    let consumer_files: Vec<_> = read_dir(&consumers_dir)
        .map(|e| e.filter_map(|e| e.ok().map(|e| e.file_name())).collect())
        .unwrap_or_default();
    let group_files: Vec<_> = read_dir(&groups_dir)
        .map(|e| e.filter_map(|e| e.ok().map(|e| e.file_name())).collect())
        .unwrap_or_default();
    assert!(
        consumer_files.is_empty(),
        "Consumer offset files must be deleted after purge, found: {consumer_files:?}"
    );
    assert!(
        group_files.is_empty(),
        "Consumer group offset files must be deleted after purge, found: {group_files:?}"
    );

    // --- Verify partition reset: single empty segment at offset 0 ---
    assert_fresh_empty_partition(&partition_path);

    // --- Verify new messages start at offset 0 ---
    let new_msg_count = 3u32;
    send_messages(&client, &stream_ident, &topic_ident, new_msg_count).await;

    let probe_consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(99).unwrap(),
    };
    let polled = client
        .poll_messages(
            &stream_ident,
            &topic_ident,
            Some(PARTITION_ID),
            &probe_consumer,
            &PollingStrategy::offset(0),
            100,
            false,
        )
        .await
        .unwrap();
    let offsets: Vec<u64> = polled.messages.iter().map(|m| m.header.offset).collect();
    assert_eq!(
        offsets,
        (0..new_msg_count as u64).collect::<Vec<_>>(),
        "New messages must start at offset 0 after purge"
    );

    // Cleanup: consumer group was dropped before purge, just delete stream resources
    client
        .delete_consumer_group(&stream_ident, &topic_ident, &group_ident)
        .await
        .unwrap();
    client
        .delete_topic(&stream_ident, &topic_ident)
        .await
        .unwrap();
    client.delete_stream(&stream_ident).await.unwrap();
}

async fn maybe_restart(harness: &mut TestHarness, restart_server: bool) {
    if restart_server {
        harness.restart_server().await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

/// Build a root client with SDK-level auto-reconnect and auto-sign-in.
///
/// Unlike `harness.tcp_root_client()` which does a one-shot `login_user()`, this embeds
/// credentials in the transport config so the SDK re-authenticates on reconnect.
fn build_root_client(harness: &TestHarness) -> IggyClient {
    let addr = harness.server().tcp_addr().unwrap();
    let interval = IggyDuration::from_str("200ms").unwrap();
    IggyClient::builder()
        .with_tcp()
        .with_server_address(addr.to_string())
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            DEFAULT_ROOT_USERNAME.to_string(),
            DEFAULT_ROOT_PASSWORD.to_string(),
        )))
        .with_reconnection_max_retries(Some(10))
        .with_reconnection_interval(interval)
        .with_reestablish_after(interval)
        .build()
        .unwrap()
}

fn partition_path(data_path: &Path, stream_id: u32, topic_id: u32) -> String {
    data_path
        .join(format!(
            "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
        ))
        .display()
        .to_string()
}

async fn send_messages(
    client: &IggyClient,
    stream_ident: &Identifier,
    topic_ident: &Identifier,
    count: u32,
) {
    for i in 0..count {
        let payload = Bytes::from(format!("{i:04}").repeat(PAYLOAD_SIZE / 4));
        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(payload)
            .build()
            .expect("Failed to create message");

        let mut messages = vec![message];
        client
            .send_messages(
                stream_ident,
                topic_ident,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }
}

/// Polls all messages from offset 0 and returns their offsets in order.
async fn poll_all_offsets(
    client: &IggyClient,
    stream_ident: &Identifier,
    topic_ident: &Identifier,
) -> Vec<u64> {
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(99).unwrap(),
    };
    let polled = client
        .poll_messages(
            stream_ident,
            topic_ident,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            TOTAL_MESSAGES * 2,
            false,
        )
        .await
        .unwrap();
    polled.messages.iter().map(|m| m.header.offset).collect()
}

/// Asserts that each segment's `.log` and `.index` files have the exact expected size.
/// Derives message count per segment from adjacent offsets and TOTAL_MESSAGES.
fn assert_segment_file_sizes(partition_path: &str, offsets: &[u64]) {
    for (i, &offset) in offsets.iter().enumerate() {
        let msg_count = if i + 1 < offsets.len() {
            offsets[i + 1] - offset
        } else {
            TOTAL_MESSAGES as u64 - offset
        };

        let log_path = format!("{partition_path}/{offset:0>20}.{LOG_EXTENSION}");
        let index_path = format!("{partition_path}/{offset:0>20}.{INDEX_EXTENSION}");

        let log_size = metadata(&log_path)
            .unwrap_or_else(|e| panic!("{log_path}: {e}"))
            .len();
        let index_size = metadata(&index_path)
            .unwrap_or_else(|e| panic!("{index_path}: {e}"))
            .len();

        let expected_log = msg_count * MESSAGE_ON_DISK_SIZE;
        let expected_index = msg_count * INDEX_SIZE_PER_MSG;
        assert_eq!(
            log_size, expected_log,
            "Segment {offset}: log {log_size}B != expected {expected_log}B ({msg_count} msgs)"
        );
        assert_eq!(
            index_size, expected_index,
            "Segment {offset}: index {index_size}B != expected {expected_index}B ({msg_count} msgs)"
        );
    }
}

fn get_sorted_segment_offsets(partition_path: &str) -> Vec<u64> {
    let mut offsets: Vec<u64> = read_dir(partition_path)
        .map(|entries| {
            entries
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == LOG_EXTENSION) {
                        path.file_stem()
                            .and_then(|s| s.to_str())
                            .and_then(|s| s.parse::<u64>().ok())
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    offsets.sort();
    offsets
}

fn is_dir_empty(dir: &str) -> bool {
    read_dir(dir)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(true)
}

/// Asserts the partition directory contains exactly one .log and one .index file at offset 0,
/// both with size 0 — the expected state after a full purge or segment reset.
fn assert_fresh_empty_partition(partition_path: &str) {
    assert_eq!(
        get_sorted_segment_offsets(partition_path),
        [0],
        "Partition must contain a single segment at offset 0"
    );
    assert_eq!(
        count_files_with_ext(partition_path, INDEX_EXTENSION),
        1,
        "Exactly one .index file must remain"
    );

    let log_path = format!("{partition_path}/{:0>20}.{LOG_EXTENSION}", 0);
    let index_path = format!("{partition_path}/{:0>20}.{INDEX_EXTENSION}", 0);
    assert_eq!(
        metadata(&log_path).unwrap().len(),
        0,
        "Fresh .log must be empty"
    );
    assert_eq!(
        metadata(&index_path).unwrap().len(),
        0,
        "Fresh .index must be empty"
    );
}

/// Asserts no orphaned segment files remain after deletion.
/// `get_sorted_segment_offsets` only checks .log files — this additionally verifies
/// that the .index file count matches, catching stale .index files left behind.
fn assert_no_orphaned_segment_files(partition_path: &str, expected_count: usize) {
    let log_count = count_files_with_ext(partition_path, LOG_EXTENSION);
    let index_count = count_files_with_ext(partition_path, INDEX_EXTENSION);
    assert_eq!(
        log_count, expected_count,
        "Expected {expected_count} .log files, found {log_count}"
    );
    assert_eq!(
        index_count, expected_count,
        "Expected {expected_count} .index files, found {index_count}"
    );
}

fn count_files_with_ext(dir: &str, ext: &str) -> usize {
    read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|e| e == ext))
                .count()
        })
        .unwrap_or(0)
}

/// Mirrors the server's deletion rule: a sealed segment is deletable when
/// `seg.end_offset <= min_committed_offset` (see `delete_oldest_segments` in segments.rs).
///
/// `seg_end_offset` is the last offset stored in the segment (inclusive).
/// `committed` is the minimum committed offset across all consumers/groups.
fn segment_deletable(seg_end_offset: u64, committed: u64) -> bool {
    seg_end_offset <= committed
}
