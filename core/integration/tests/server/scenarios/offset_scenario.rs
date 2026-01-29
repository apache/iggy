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

use super::PARTITION_ID;
use bytes::BytesMut;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::collections::HashMap;

fn small_batches() -> Vec<u32> {
    vec![3, 4, 5, 6, 7]
}

fn medium_batches() -> Vec<u32> {
    vec![10, 20, 30, 40]
}

fn large_batches() -> Vec<u32> {
    vec![100, 200, 300, 400]
}

fn very_large_batches() -> Vec<u32> {
    vec![500, 1000, 1500, 2000]
}

fn all_batch_patterns() -> Vec<(&'static str, Vec<u32>)> {
    vec![
        ("small", small_batches()),
        ("medium", medium_batches()),
        ("large", large_batches()),
        ("very_large", very_large_batches()),
    ]
}

fn all_message_sizes() -> Vec<u64> {
    vec![50, 1000, 20000]
}

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);
    login_root(&client).await;

    for msg_size in all_message_sizes() {
        for (pattern_name, batch_pattern) in all_batch_patterns() {
            run_offset_test(&client, msg_size, &batch_pattern, pattern_name).await;
        }
    }
}

async fn run_offset_test(
    client: &IggyClient,
    message_size: u64,
    batch_lengths: &[u32],
    pattern_name: &str,
) {
    let stream_name = format!("test-stream-{}-{}", message_size, pattern_name);
    let topic_name = format!("test-topic-{}-{}", message_size, pattern_name);

    init_system(client, &stream_name, &topic_name).await;

    let total_messages_count: u32 = batch_lengths.iter().sum();

    let batch_offsets = send_messages_in_batches(
        client,
        &stream_name,
        &topic_name,
        message_size,
        batch_lengths,
    )
    .await;

    verify_all_messages_from_start(client, &stream_name, &topic_name, total_messages_count).await;
    verify_messages_from_middle(
        client,
        &stream_name,
        &topic_name,
        &batch_offsets,
        batch_lengths,
        total_messages_count,
    )
    .await;
    verify_no_messages_beyond_end(client, &stream_name, &topic_name, &batch_offsets).await;
    verify_small_subset_from_start(client, &stream_name, &topic_name, total_messages_count).await;
    verify_messages_spanning_batches(client, &stream_name, &topic_name, &batch_offsets).await;
    verify_message_content(
        client,
        &stream_name,
        &topic_name,
        message_size,
        &batch_offsets,
    )
    .await;
    verify_sequential_chunk_reads(client, &stream_name, &topic_name, total_messages_count).await;

    cleanup(client, &stream_name).await;
}

async fn init_system(client: &IggyClient, stream_name: &str, topic_name: &str) {
    client.create_stream(stream_name).await.unwrap();
    client
        .create_topic(
            &Identifier::named(stream_name).unwrap(),
            topic_name,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

async fn send_messages_in_batches(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    message_size: u64,
    batch_lengths: &[u32],
) -> Vec<u64> {
    let mut batch_offsets = Vec::with_capacity(batch_lengths.len());
    let mut current_offset = 0u64;
    let mut message_id = 1u32;

    for &batch_len in batch_lengths {
        let mut messages_to_send = Vec::with_capacity(batch_len as usize);

        for _ in 0..batch_len {
            let msg = create_single_message(message_id, message_size);
            messages_to_send.push(msg);
            message_id += 1;
        }

        client
            .send_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages_to_send,
            )
            .await
            .unwrap();

        current_offset += batch_len as u64;
        batch_offsets.push(current_offset - 1);
    }

    batch_offsets
}

fn create_single_message(id: u32, message_size: u64) -> IggyMessage {
    let beginning_of_payload = format!("message {id}");
    let mut payload = BytesMut::new();
    payload.extend_from_slice(beginning_of_payload.as_bytes());
    payload.resize(message_size as usize, 0xD);
    let payload = payload.freeze();

    let mut headers = HashMap::new();
    headers.insert(
        HeaderKey::try_from("key_1").unwrap(),
        HeaderValue::try_from("Value 1").unwrap(),
    );
    headers.insert(HeaderKey::try_from("key 2").unwrap(), true.into());
    headers.insert(HeaderKey::try_from("key-3").unwrap(), 123456u64.into());

    IggyMessage::builder()
        .id(id as u128)
        .payload(payload)
        .user_headers(headers)
        .build()
        .expect("Failed to create message")
}

async fn verify_all_messages_from_start(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    total_messages: u32,
) {
    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            total_messages,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled.messages.len() as u32,
        total_messages,
        "Expected {} messages from start, got {}",
        total_messages,
        polled.messages.len()
    );
}

async fn verify_messages_from_middle(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    batch_offsets: &[u64],
    batch_lengths: &[u32],
    total_messages: u32,
) {
    if batch_offsets.len() >= 3 {
        let middle_offset = batch_offsets[2];
        let prior_batches_sum: u32 = batch_lengths[..3].iter().sum();
        let remaining_messages = total_messages - prior_batches_sum;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(middle_offset + 1),
                remaining_messages,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len() as u32,
            remaining_messages,
            "Expected {} messages from middle, got {}",
            remaining_messages,
            polled.messages.len()
        );
    }
}

async fn verify_no_messages_beyond_end(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    batch_offsets: &[u64],
) {
    if let Some(&final_offset) = batch_offsets.last() {
        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(final_offset + 1),
                1,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len(),
            0,
            "Expected no messages beyond end, got {}",
            polled.messages.len()
        );
    }
}

async fn verify_small_subset_from_start(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    total_messages: u32,
) {
    let subset_size = std::cmp::min(3, total_messages);

    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            subset_size,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled.messages.len() as u32,
        subset_size,
        "Expected {} messages in subset, got {}",
        subset_size,
        polled.messages.len()
    );
}

async fn verify_messages_spanning_batches(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    batch_offsets: &[u64],
) {
    if batch_offsets.len() >= 4 {
        let span_offset = batch_offsets[1] + 1;
        let span_size = 8u32;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(span_offset),
                span_size,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len() as u32,
            span_size,
            "Expected {} messages spanning batches, got {}",
            span_size,
            polled.messages.len()
        );
    }
}

async fn verify_message_content(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    message_size: u64,
    batch_offsets: &[u64],
) {
    if batch_offsets.len() >= 4 {
        let span_offset = batch_offsets[1] + 1;
        let span_size = 8u32;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(span_offset),
                span_size,
                false,
            )
            .await
            .unwrap();

        for (i, msg) in polled.messages.iter().enumerate() {
            let expected_offset = span_offset + i as u64;
            assert!(
                msg.header.offset >= expected_offset,
                "Message offset {} should be >= {}",
                msg.header.offset,
                expected_offset
            );

            let message_id = (msg.header.offset + 1) as u32;
            let expected_payload_prefix = format!("message {message_id}");

            let payload_str = String::from_utf8_lossy(&msg.payload);
            assert!(
                payload_str.starts_with(&expected_payload_prefix),
                "Payload at offset {} should start with '{}', got '{}'",
                msg.header.offset,
                expected_payload_prefix,
                &payload_str[..std::cmp::min(payload_str.len(), 20)]
            );

            assert_eq!(
                msg.payload.len(),
                message_size as usize,
                "Payload size mismatch at offset {}",
                msg.header.offset
            );

            assert_eq!(
                msg.header.id, message_id as u128,
                "Message ID mismatch at offset {}",
                msg.header.offset
            );

            if let Some(headers) = &msg.user_headers {
                let headers_map =
                    HashMap::<HeaderKey, HeaderValue>::from_bytes(headers.clone()).unwrap();
                assert_eq!(headers_map.len(), 3, "Expected 3 headers");
            }
        }
    }
}

async fn verify_sequential_chunk_reads(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    total_messages: u32,
) {
    let chunk_size = 500u32;
    let mut verified_count = 0u32;

    for chunk_start in (0..total_messages).step_by(chunk_size as usize) {
        let read_size = std::cmp::min(chunk_size, total_messages - chunk_start);

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(chunk_start as u64),
                read_size,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len() as u32,
            read_size,
            "Failed to read chunk at offset {} with size {}",
            chunk_start,
            read_size
        );

        verified_count += polled.messages.len() as u32;
    }

    assert_eq!(
        verified_count, total_messages,
        "Sequential chunk reads didn't cover all messages"
    );
}

async fn cleanup(client: &IggyClient, stream_name: &str) {
    client
        .delete_stream(&Identifier::named(stream_name).unwrap())
        .await
        .unwrap();
}
