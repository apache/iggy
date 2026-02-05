/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use iggy::prelude::*;
use integration::bench_utils::run_bench_and_wait_for_finish;
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::{collections::HashMap, str::FromStr};
use test_case::test_matrix;

fn cache_open_segment() -> &'static str {
    "open_segment"
}

fn cache_all() -> &'static str {
    "all"
}

fn cache_none() -> &'static str {
    "none"
}

fn build_server_config(cache_setting: &str) -> TestServerConfig {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        cache_setting.to_string(),
    );
    TestServerConfig::builder().extra_envs(extra_envs).build()
}

// TODO(numminex) - Move the message generation method from benchmark run to a special method.
#[test_matrix(
    [cache_all(), cache_open_segment(), cache_none()]
)]
#[tokio::test]
#[parallel]
async fn should_fill_data_and_verify_after_restart(cache_setting: &'static str) {
    let mut harness = TestHarness::builder()
        .server(build_server_config(cache_setting))
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let server_addr = harness.server().raw_tcp_addr().unwrap();

    // Run send bench to fill 5 MB of data
    let amount_of_data_to_process = IggyByteSize::from_str("5 MB").unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-producer",
        amount_of_data_to_process,
    );

    // Run poll bench to check if everything's OK
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        amount_of_data_to_process,
    );

    // Connect and login to server
    let client = harness.tcp_root_client().await.unwrap();

    let topic_id = Identifier::numeric(0).unwrap();
    for i in 0..7 {
        let stream_id = Identifier::numeric(i).unwrap();
        client
            .flush_unsaved_buffer(&stream_id, &topic_id, 0, true)
            .await
            .unwrap();
    }

    // Create consumer groups to test persistence
    let consumer_group_names = ["test-cg-1", "test-cg-2", "test-cg-3"];
    for (idx, cg_name) in consumer_group_names.iter().enumerate() {
        let stream_id = Identifier::numeric(idx as u32).unwrap();
        client
            .create_consumer_group(&stream_id, &topic_id, cg_name)
            .await
            .unwrap();
    }

    // Save stats from the first server
    let stats = client.get_stats().await.unwrap();
    let expected_messages_size_bytes = stats.messages_size_bytes;
    let expected_streams_count = stats.streams_count;
    let expected_topics_count = stats.topics_count;
    let expected_partitions_count = stats.partitions_count;
    let expected_segments_count = stats.segments_count;
    let expected_messages_count = stats.messages_count;
    let expected_clients_count = stats.clients_count;
    let expected_consumer_groups_count = stats.consumer_groups_count;

    // Restart server
    harness.restart_server().await.unwrap();
    let server_addr = harness.server().raw_tcp_addr().unwrap();

    // Verify stats are preserved after restart (before adding more data)
    let client_after_restart = harness.tcp_root_client().await.unwrap();

    let stats_after_restart = client_after_restart.get_stats().await.unwrap();
    assert_eq!(
        expected_messages_count, stats_after_restart.messages_count,
        "Messages count should be preserved after restart (before: {}, after: {})",
        expected_messages_count, stats_after_restart.messages_count
    );
    assert_eq!(
        expected_messages_size_bytes.as_bytes_usize(),
        stats_after_restart.messages_size_bytes.as_bytes_usize(),
        "Messages size bytes should be preserved after restart (before: {}, after: {})",
        expected_messages_size_bytes.as_bytes_usize(),
        stats_after_restart.messages_size_bytes.as_bytes_usize()
    );
    assert_eq!(
        expected_streams_count, stats_after_restart.streams_count,
        "Streams count should be preserved after restart"
    );
    assert_eq!(
        expected_topics_count, stats_after_restart.topics_count,
        "Topics count should be preserved after restart"
    );
    assert_eq!(
        expected_partitions_count, stats_after_restart.partitions_count,
        "Partitions count should be preserved after restart"
    );
    assert_eq!(
        expected_segments_count, stats_after_restart.segments_count,
        "Segments count should be preserved after restart"
    );
    assert_eq!(
        expected_consumer_groups_count, stats_after_restart.consumer_groups_count,
        "Consumer groups count should be preserved after restart"
    );

    // Verify consumer groups exist after restart
    for (idx, cg_name) in consumer_group_names.iter().enumerate() {
        let stream_id = Identifier::numeric(idx as u32).unwrap();
        let consumer_group = client_after_restart
            .get_consumer_group(
                &stream_id,
                &topic_id,
                &Identifier::from_str(cg_name).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            consumer_group.is_some(),
            "Consumer group {} should exist after restart",
            cg_name
        );
        assert_eq!(
            consumer_group.unwrap().name,
            *cg_name,
            "Consumer group name should match"
        );
    }

    // Run send bench again to add more data
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-producer",
        amount_of_data_to_process,
    );

    // Run poll bench again to check if all data is still there
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        IggyByteSize::from(amount_of_data_to_process.as_bytes_u64() * 2),
    );
    drop(client_after_restart);

    // Connect and login to server
    let client = harness.tcp_root_client().await.unwrap();

    // Flush unsaved buffer
    let topic_id = Identifier::numeric(0).unwrap();
    for i in 0..7 {
        let stream_id = Identifier::numeric(i).unwrap();
        client
            .flush_unsaved_buffer(&stream_id, &topic_id, 0, true)
            .await
            .unwrap();
    }

    // Save stats from the second server (should have double the data)
    let stats = client.get_stats().await.unwrap();
    let actual_messages_size_bytes = stats.messages_size_bytes;
    let actual_streams_count = stats.streams_count;
    let actual_topics_count = stats.topics_count;
    let actual_partitions_count = stats.partitions_count;
    let actual_segments_count = stats.segments_count;
    let actual_messages_count = stats.messages_count;
    let actual_clients_count = stats.clients_count;
    let actual_consumer_groups_count = stats.consumer_groups_count;

    // Compare stats (expecting double the messages/size after second bench run)
    assert_eq!(
        expected_messages_size_bytes.as_bytes_usize() * 2,
        actual_messages_size_bytes.as_bytes_usize(),
        "Messages size bytes should be doubled"
    );
    assert_eq!(
        expected_streams_count, actual_streams_count,
        "Streams count"
    );
    assert_eq!(expected_topics_count, actual_topics_count, "Topics count");
    assert_eq!(
        expected_partitions_count, actual_partitions_count,
        "Partitions count"
    );
    assert!(
        actual_segments_count >= expected_segments_count,
        "Segments count should be at least the same or more"
    );
    assert_eq!(
        expected_messages_count * 2,
        actual_messages_count,
        "Messages count should be doubled"
    );
    assert_eq!(
        expected_clients_count, actual_clients_count,
        "Clients count"
    );
    assert_eq!(
        expected_consumer_groups_count, actual_consumer_groups_count,
        "Consumer groups count"
    );

    // Verify consumer groups still exist after second benchmark run
    for (idx, cg_name) in consumer_group_names.iter().enumerate() {
        let stream_id = Identifier::numeric(idx as u32).unwrap();
        let consumer_group = client
            .get_consumer_group(
                &stream_id,
                &topic_id,
                &Identifier::from_str(cg_name).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            consumer_group.is_some(),
            "Consumer group {} should still exist after second benchmark",
            cg_name
        );
        assert_eq!(
            consumer_group.unwrap().name,
            *cg_name,
            "Consumer group name should match"
        );
    }

    // Run poll bench to check if all data (10MB total) is still there
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        IggyByteSize::from(amount_of_data_to_process.as_bytes_u64() * 2),
    );
}

/// Test that verifies server correctly handles ID gaps after deletions at all levels:
/// streams, topics, partitions, and consumer groups.
///
/// Creates a hierarchy with gaps from deletions:
/// - Streams: 0, 2 (1 deleted)
/// - Topics per stream: 0, 2 (1 deleted)
/// - Partitions per topic: 0, 2 (1 deleted)
/// - Consumer groups per topic: 0, 2 (1 deleted)
///
/// Also tests ID reuse: after deletion, creating new resources should reuse the freed IDs.
/// This catches the bug where `streams_count()`/`topics_count()` (slab length) was used
/// instead of `vacant_key()` to predict assigned IDs, causing mismatch when IDs are reused.
#[tokio::test]
#[parallel]
async fn should_handle_resource_deletion_and_restart() {
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::default())
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();

    // Create 3 streams
    let stream_0 = client.create_stream("stream-0").await.unwrap();
    let stream_1 = client.create_stream("stream-1").await.unwrap();
    let stream_2 = client.create_stream("stream-2").await.unwrap();

    assert_eq!(stream_0.id, 0);
    assert_eq!(stream_1.id, 1);
    assert_eq!(stream_2.id, 2);

    // For streams 0 and 2, create topics, partitions, and consumer groups with gaps
    for stream_id in [0u32, 2u32] {
        let stream_ident = Identifier::numeric(stream_id).unwrap();

        // Create 3 topics per stream (each with 3 partitions initially)
        for topic_idx in 0..3 {
            let topic = client
                .create_topic(
                    &stream_ident,
                    &format!("topic-{}", topic_idx),
                    3,
                    CompressionAlgorithm::None,
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::Unlimited,
                )
                .await
                .unwrap();
            assert_eq!(topic.id, topic_idx);

            let topic_ident = Identifier::numeric(topic_idx).unwrap();

            // Create 3 consumer groups per topic
            for cg_idx in 0..3 {
                client
                    .create_consumer_group(&stream_ident, &topic_ident, &format!("cg-{}", cg_idx))
                    .await
                    .unwrap();
            }
        }

        // Delete middle topic (topic 1)
        client
            .delete_topic(&stream_ident, &Identifier::numeric(1).unwrap())
            .await
            .unwrap();

        // For remaining topics (0 and 2), delete middle partition then middle consumer group
        // This order tests that the server handles partition deletion before consumer group deletion
        for topic_id in [0u32, 2u32] {
            let topic_ident = Identifier::numeric(topic_id).unwrap();

            client
                .delete_partitions(&stream_ident, &topic_ident, 1)
                .await
                .unwrap();

            client
                .delete_consumer_group(
                    &stream_ident,
                    &topic_ident,
                    &Identifier::numeric(1).unwrap(),
                )
                .await
                .unwrap();
        }
    }

    // Delete middle stream (stream 1)
    client
        .delete_stream(&Identifier::numeric(1).unwrap())
        .await
        .unwrap();

    // TEST ID REUSE: Create a new stream after deletion - it should reuse ID 1
    // This is the critical test that catches the bug where streams_count() (slab.len())
    // was used instead of vacant_key() to predict the assigned ID.
    // After deletion: streams are [0, 2], len=2, but vacant_key=1
    // Bug: code used len (2) to create directory, but metadata assigned ID 1
    let stream_reused = client.create_stream("stream-reused").await.unwrap();
    assert_eq!(
        stream_reused.id, 1,
        "New stream should reuse deleted ID 1, got {}",
        stream_reused.id
    );

    // Also test topic ID reuse within a stream
    // Delete topic 0 in stream 0, then create new topic - should reuse ID 0... wait, topic 1 was already deleted
    // Actually let's delete topic 0 and create new one, it should get ID 0 or 1 depending on vacant_key
    let stream_0_ident = Identifier::numeric(0).unwrap();
    client
        .delete_topic(&stream_0_ident, &Identifier::numeric(0).unwrap())
        .await
        .unwrap();

    // Now stream 0 has only topic 2. Creating new topic should reuse ID 0 (first vacant slot)
    let topic_reused = client
        .create_topic(
            &stream_0_ident,
            "topic-reused",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
        .await
        .unwrap();
    assert_eq!(
        topic_reused.id, 0,
        "New topic should reuse deleted ID 0, got {}",
        topic_reused.id
    );

    // Verify state before restart
    let streams = client.get_streams().await.unwrap();
    assert_eq!(streams.len(), 3); // 0, 1 (reused), 2
    let stream_ids: Vec<u32> = streams.iter().map(|s| s.id).collect();
    assert!(
        stream_ids.contains(&0) && stream_ids.contains(&1) && stream_ids.contains(&2),
        "Expected streams 0, 1, 2 after ID reuse, got: {:?}",
        stream_ids
    );

    drop(client);

    // Restart server
    harness.restart_server().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();

    // Verify streams after restart - should have 3 streams: 0, 1 (reused), 2
    let streams = client.get_streams().await.unwrap();
    assert_eq!(streams.len(), 3, "Expected 3 streams after restart");
    let stream_ids: Vec<u32> = streams.iter().map(|s| s.id).collect();
    assert!(
        stream_ids.contains(&0) && stream_ids.contains(&1) && stream_ids.contains(&2),
        "Expected streams 0, 1, 2 after restart, got: {:?}",
        stream_ids
    );

    // Verify the reused stream 1 has the correct name
    let stream_1 = client
        .get_stream(&Identifier::numeric(1).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stream_1.name, "stream-reused",
        "Stream 1 should be the reused stream"
    );

    // Verify stream 0 topics after restart
    // Stream 0 should have: topic 0 (reused, "topic-reused") and topic 2
    let stream_0_ident = Identifier::numeric(0).unwrap();
    let topics_stream_0 = client.get_topics(&stream_0_ident).await.unwrap();
    let topic_ids_0: Vec<u32> = topics_stream_0.iter().map(|t| t.id).collect();
    assert!(
        topic_ids_0.contains(&0) && topic_ids_0.contains(&2),
        "Stream 0 should have topics 0 and 2, got: {:?}",
        topic_ids_0
    );

    // Verify the reused topic has correct name
    let topic_0 = client
        .get_topic(&stream_0_ident, &Identifier::numeric(0).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        topic_0.name, "topic-reused",
        "Topic 0 in stream 0 should be the reused topic"
    );

    // Verify topic 2 in stream 0 still has correct structure
    let topic_2_ident = Identifier::numeric(2).unwrap();
    let topic_2 = client
        .get_topic(&stream_0_ident, &topic_2_ident)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        topic_2.partitions_count, 2,
        "Topic 2 in stream 0 should have 2 partitions"
    );
    let cgs_topic_2 = client
        .get_consumer_groups(&stream_0_ident, &topic_2_ident)
        .await
        .unwrap();
    assert_eq!(
        cgs_topic_2.len(),
        2,
        "Topic 2 in stream 0 should have 2 consumer groups"
    );
    let cg_ids: Vec<u32> = cgs_topic_2.iter().map(|c| c.id).collect();
    assert!(
        cg_ids.contains(&0) && cg_ids.contains(&2),
        "Topic 2 in stream 0 should have consumer groups 0 and 2, got: {:?}",
        cg_ids
    );

    // Verify stream 2 structure (unchanged from original test)
    let stream_2_ident = Identifier::numeric(2).unwrap();
    let topics_stream_2 = client.get_topics(&stream_2_ident).await.unwrap();
    let topic_ids_2: Vec<u32> = topics_stream_2.iter().map(|t| t.id).collect();
    assert!(
        topic_ids_2.contains(&0) && topic_ids_2.contains(&2),
        "Stream 2 should have topics 0 and 2, got: {:?}",
        topic_ids_2
    );

    for topic_id in [0u32, 2u32] {
        let topic_ident = Identifier::numeric(topic_id).unwrap();
        let topic = client
            .get_topic(&stream_2_ident, &topic_ident)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            topic.partitions_count, 2,
            "Topic {} in stream 2 should have 2 partitions",
            topic_id
        );

        let cgs = client
            .get_consumer_groups(&stream_2_ident, &topic_ident)
            .await
            .unwrap();
        assert_eq!(
            cgs.len(),
            2,
            "Topic {} in stream 2 should have 2 consumer groups",
            topic_id
        );
        let cg_ids: Vec<u32> = cgs.iter().map(|c| c.id).collect();
        assert!(
            cg_ids.contains(&0) && cg_ids.contains(&2),
            "Topic {} in stream 2 should have consumer groups 0 and 2, got: {:?}",
            topic_id,
            cg_ids
        );
    }
}
