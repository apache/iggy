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
use integration::test_server::{ClientFactory, TestServer};
use std::fs::{DirEntry, read_dir};

const STREAM_NAME: &str = "test_stream";
const TOPIC_NAME: &str = "test_topic";
const PARTITION_ID: u32 = 0;
const LOG_EXTENSION: &str = "log";

pub async fn run(client_factory: &dyn ClientFactory, test_server: &TestServer) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

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

    // Send 5 large messages to create multiple segments
    let large_payload = "A".repeat(1024 * 1024);

    for i in 0..5 {
        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(Bytes::from(large_payload.clone()))
            .build()
            .expect("Failed to create message");

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

    // Wait for segments to be persisted and closed
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    // Check initial segment count on filesystem
    let data_path = test_server.get_local_data_path();
    let partition_path =
        format!("{data_path}/streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}");

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    println!(
        "Initial segments: {:?}",
        initial_segments
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    assert!(
        initial_segments.len() >= 3,
        "Expected at least 3 segments but got {}. This might mean the segment size is too large or messages aren't being flushed.",
        initial_segments.len()
    );

    let initial_count = initial_segments.len();

    // Test delete segments command - keep only 2 segments
    let segments_to_keep = 2u32;
    let result = client
        .delete_segments(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            PARTITION_ID,
            segments_to_keep,
        )
        .await;

    assert!(
        result.is_ok(),
        "Delete segments command should succeed: {result:?}"
    );

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify segments were deleted from filesystem
    let remaining_segments = get_segment_paths_for_partition(&partition_path);
    println!(
        "Remaining segments: {:?}",
        remaining_segments
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // Should have at most segments_to_keep + 1 (closed + open segments)
    assert!(
        remaining_segments.len() <= (segments_to_keep + 1) as usize,
        "Expected at most {} segments but got {}",
        segments_to_keep + 1,
        remaining_segments.len()
    );

    assert!(
        remaining_segments.len() < initial_count,
        "Expected fewer segments after deletion. Initial: {}, Remaining: {}",
        initial_count,
        remaining_segments.len()
    );

    // Test edge cases
    let result = client
        .delete_segments(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            PARTITION_ID,
            0,
        )
        .await;
    assert!(
        result.is_ok(),
        "Delete segments with 0 count should succeed"
    );

    // Test error cases
    let result = client
        .delete_segments(
            &Identifier::numeric(999).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            PARTITION_ID,
            1,
        )
        .await;
    assert!(
        result.is_err(),
        "Delete segments on non-existent stream should fail"
    );

    let result = client
        .delete_segments(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::numeric(999).unwrap(),
            PARTITION_ID,
            1,
        )
        .await;
    assert!(
        result.is_err(),
        "Delete segments on non-existent topic should fail"
    );

    let result = client
        .delete_segments(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            999,
            1,
        )
        .await;
    assert!(
        result.is_err(),
        "Delete segments on non-existent partition should fail"
    );

    client
        .delete_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap();

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
