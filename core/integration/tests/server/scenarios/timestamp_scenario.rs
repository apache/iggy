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
use tokio::time::{Duration, sleep};

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
            run_timestamp_test(&client, msg_size, &batch_pattern, pattern_name).await;
        }
    }
}

async fn run_timestamp_test(
    client: &IggyClient,
    message_size: u64,
    batch_lengths: &[u32],
    pattern_name: &str,
) {
    let stream_name = format!("test-stream-ts-{}-{}", message_size, pattern_name);
    let topic_name = format!("test-topic-ts-{}-{}", message_size, pattern_name);

    init_system(client, &stream_name, &topic_name).await;

    let total_messages_count: u32 = batch_lengths.iter().sum();

    let (initial_timestamp, batch_timestamps) = send_messages_in_batches_with_timestamps(
        client,
        &stream_name,
        &topic_name,
        message_size,
        batch_lengths,
    )
    .await;

    verify_all_messages_from_start_timestamp(
        client,
        &stream_name,
        &topic_name,
        initial_timestamp,
        total_messages_count,
    )
    .await;
    verify_messages_from_middle_timestamp(
        client,
        &stream_name,
        &topic_name,
        &batch_timestamps,
        batch_lengths,
        total_messages_count,
    )
    .await;
    verify_no_messages_with_future_timestamp(client, &stream_name, &topic_name).await;
    verify_small_subset_from_start_timestamp(
        client,
        &stream_name,
        &topic_name,
        initial_timestamp,
        total_messages_count,
    )
    .await;
    verify_messages_spanning_batches_by_timestamp(
        client,
        &stream_name,
        &topic_name,
        &batch_timestamps,
    )
    .await;
    verify_message_content_by_timestamp(
        client,
        &stream_name,
        &topic_name,
        message_size,
        &batch_timestamps,
    )
    .await;

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

async fn send_messages_in_batches_with_timestamps(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    message_size: u64,
    batch_lengths: &[u32],
) -> (u64, Vec<u64>) {
    let mut batch_timestamps = Vec::with_capacity(batch_lengths.len());
    let mut message_id = 1u32;

    let initial_timestamp = IggyTimestamp::now().as_micros();

    for &batch_len in batch_lengths {
        sleep(Duration::from_millis(2)).await;

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

        batch_timestamps.push(IggyTimestamp::now().as_micros());

        sleep(Duration::from_millis(2)).await;
    }

    (initial_timestamp, batch_timestamps)
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

async fn verify_all_messages_from_start_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    initial_timestamp: u64,
    total_messages: u32,
) {
    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::timestamp(IggyTimestamp::from(initial_timestamp)),
            total_messages,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled.messages.len() as u32,
        total_messages,
        "Expected {} messages from initial timestamp, got {}",
        total_messages,
        polled.messages.len()
    );
}

async fn verify_messages_from_middle_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    batch_timestamps: &[u64],
    batch_lengths: &[u32],
    total_messages: u32,
) {
    if batch_timestamps.len() >= 3 {
        let middle_timestamp = batch_timestamps[2] + 1000;
        let prior_batches_sum: u32 = batch_lengths[..3].iter().sum();
        let remaining_messages = total_messages - prior_batches_sum;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::timestamp(IggyTimestamp::from(middle_timestamp)),
                remaining_messages,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len() as u32,
            remaining_messages,
            "Expected {} messages from middle timestamp, got {}",
            remaining_messages,
            polled.messages.len()
        );
    }
}

async fn verify_no_messages_with_future_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
) {
    let future_timestamp = IggyTimestamp::now().as_micros() + 3_600_000_000;

    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::timestamp(IggyTimestamp::from(future_timestamp)),
            10,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled.messages.len(),
        0,
        "Expected no messages with future timestamp, got {}",
        polled.messages.len()
    );
}

async fn verify_small_subset_from_start_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    initial_timestamp: u64,
    total_messages: u32,
) {
    let subset_size = std::cmp::min(3, total_messages);

    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::timestamp(IggyTimestamp::from(initial_timestamp)),
            subset_size,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        polled.messages.len() as u32,
        subset_size,
        "Expected {} messages in subset from initial timestamp, got {}",
        subset_size,
        polled.messages.len()
    );
}

async fn verify_messages_spanning_batches_by_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    batch_timestamps: &[u64],
) {
    if batch_timestamps.len() >= 4 {
        let span_timestamp = batch_timestamps[1] + 1000;
        let span_size = 8u32;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::timestamp(IggyTimestamp::from(span_timestamp)),
                span_size,
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            polled.messages.len() as u32,
            span_size,
            "Expected {} messages spanning multiple batches, got {}",
            span_size,
            polled.messages.len()
        );
    }
}

async fn verify_message_content_by_timestamp(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    message_size: u64,
    batch_timestamps: &[u64],
) {
    if batch_timestamps.len() >= 4 {
        let span_timestamp = batch_timestamps[1] + 1000;
        let span_size = 8u32;

        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::timestamp(IggyTimestamp::from(span_timestamp)),
                span_size,
                false,
            )
            .await
            .unwrap();

        for msg in polled.messages.iter() {
            assert!(
                msg.header.timestamp >= span_timestamp,
                "Message timestamp {} should be >= span timestamp {}",
                msg.header.timestamp,
                span_timestamp
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

async fn cleanup(client: &IggyClient, stream_name: &str) {
    client
        .delete_stream(&Identifier::named(stream_name).unwrap())
        .await
        .unwrap();
}
