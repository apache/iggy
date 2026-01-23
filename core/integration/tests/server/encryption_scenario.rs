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

use bytes::Bytes;
use harness_derive::iggy_harness;
use iggy::prelude::*;
use integration::harness::TestHarness;
use std::collections::HashMap;
use std::str::FromStr;

const STREAM_NAME: &str = "test-stream-api";
const TOPIC_NAME: &str = "test-topic-api";
const MESSAGES_PER_BATCH: u64 = 1000;

async fn run_encryption_test(harness: &mut TestHarness, encryption: bool) {
    let client = harness.tcp_root_client().await.unwrap();

    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut messages_batch_1 = Vec::new();
    for i in 0..MESSAGES_PER_BATCH {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("batch").unwrap(),
            HeaderValue::from_uint64(1).unwrap(),
        );
        headers.insert(
            HeaderKey::new("index").unwrap(),
            HeaderValue::from_uint64(i).unwrap(),
        );
        headers.insert(
            HeaderKey::new("type").unwrap(),
            HeaderValue::from_str("test-message").unwrap(),
        );
        headers.insert(
            HeaderKey::new("encrypted").unwrap(),
            HeaderValue::from_bool(encryption).unwrap(),
        );

        let message = IggyMessage::builder()
            .id((i + 1) as u128)
            .payload(Bytes::from(format!(
                "Message batch 1 index {i} with encryption {encryption}"
            )))
            .user_headers(headers)
            .build()
            .expect("Failed to create message");

        messages_batch_1.push(message);
    }

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages_batch_1,
        )
        .await
        .unwrap();

    client
        .flush_unsaved_buffer(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            0,
            true,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let initial_stats = client.get_stats().await.unwrap();
    let initial_messages_count = initial_stats.messages_count;
    let initial_messages_size = initial_stats.messages_size_bytes;

    let consumer = Consumer::default();
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_PER_BATCH.try_into().unwrap(),
            false,
        )
        .await
        .unwrap();

    let all_polled_messages_1 = polled.messages;
    assert_eq!(all_polled_messages_1.len(), MESSAGES_PER_BATCH as usize);

    for msg in all_polled_messages_1.iter() {
        assert!(msg.user_headers.is_some());
        let headers = msg.user_headers_map().unwrap().unwrap();
        assert_eq!(
            headers
                .get(&HeaderKey::new("batch").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            1
        );
        assert_eq!(
            headers
                .get(&HeaderKey::new("type").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "test-message"
        );
        assert_eq!(
            headers
                .get(&HeaderKey::new("encrypted").unwrap())
                .unwrap()
                .as_bool()
                .unwrap(),
            encryption
        );
    }

    harness.restart_server().await.unwrap();
    let client = harness.tcp_root_client().await.unwrap();

    let mut messages_batch_2 = Vec::new();
    for i in 0..MESSAGES_PER_BATCH {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("batch").unwrap(),
            HeaderValue::from_uint64(2).unwrap(),
        );
        headers.insert(
            HeaderKey::new("index").unwrap(),
            HeaderValue::from_uint64(i).unwrap(),
        );
        headers.insert(
            HeaderKey::new("type").unwrap(),
            HeaderValue::from_str("test-message-after-restart").unwrap(),
        );
        headers.insert(
            HeaderKey::new("encrypted").unwrap(),
            HeaderValue::from_bool(encryption).unwrap(),
        );

        let message = IggyMessage::builder()
            .id((MESSAGES_PER_BATCH + i + 1) as u128)
            .payload(Bytes::from(format!(
                "Message batch 2 index {i} with encryption {encryption}"
            )))
            .user_headers(headers)
            .build()
            .expect("Failed to create message");

        messages_batch_2.push(message);
    }

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages_batch_2,
        )
        .await
        .unwrap();

    client
        .flush_unsaved_buffer(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            0,
            true,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_PER_BATCH as u32 * 2,
            false,
        )
        .await
        .unwrap();

    let all_polled_messages = polled.messages;
    assert_eq!(all_polled_messages.len(), (MESSAGES_PER_BATCH * 2) as usize);

    let mut batch_1_count = 0;
    let mut batch_2_count = 0;

    for msg in &all_polled_messages {
        assert!(msg.user_headers.is_some());
        let headers = msg.user_headers_map().unwrap().unwrap();
        let batch_num = headers
            .get(&HeaderKey::new("batch").unwrap())
            .unwrap()
            .as_uint64()
            .unwrap();

        if batch_num == 1 {
            batch_1_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::new("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message"
            );
            assert_eq!(
                headers
                    .get(&HeaderKey::new("encrypted").unwrap())
                    .unwrap()
                    .as_bool()
                    .unwrap(),
                encryption
            );
        } else if batch_num == 2 {
            batch_2_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::new("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message-after-restart"
            );
        }
    }

    assert_eq!(batch_1_count, MESSAGES_PER_BATCH as usize);
    assert_eq!(batch_2_count, MESSAGES_PER_BATCH as usize);

    let final_stats = client.get_stats().await.unwrap();
    assert_eq!(final_stats.messages_count, initial_messages_count * 2);
    assert!(
        final_stats.messages_size_bytes.as_bytes_u64() >= initial_messages_size.as_bytes_u64() * 2,
        "Final message size ({}) should be at least 2x initial size ({})",
        final_stats.messages_size_bytes.as_bytes_u64(),
        initial_messages_size.as_bytes_u64()
    );
}

#[iggy_harness(
    transport = Tcp,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn with_encryption(harness: &mut TestHarness) {
    run_encryption_test(harness, true).await;
}

#[iggy_harness(transport = Tcp)]
async fn without_encryption(harness: &mut TestHarness) {
    run_encryption_test(harness, false).await;
}
