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
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "single-msg-batch-stream";
const TOPIC_NAME: &str = "single-msg-batch-topic";
const POLL_BATCH_SIZE: u32 = 1000;

/// Test that simulates the bench scenario with single message per batch.
///
/// This reproduces: `iggy-bench --message-size 1 --messages-per-batch 1 --message-batches N pp -p 1 tcp`
///
/// The scenario exercises the server under conditions where:
/// - Inline persistence is delayed (high MESSAGES_REQUIRED_TO_SAVE threshold)
/// - Messages are very small (sequential number payload)
/// - No batching (1 message per send operation)
/// - Continuous sending for a fixed duration
/// - Verifies all messages are received correctly after sending
///
/// This pattern can expose issues with:
/// - Journal management under high message count with small payloads
/// - Memory allocation patterns for many small messages
/// - IOV_MAX limits when flushing many small buffers
pub async fn run(client_factory: &dyn ClientFactory, duration_secs: u64) {
    let client = client_factory.create_client().await;
    let producer = IggyClient::create(client, None, None);
    login_root(&producer).await;

    producer.create_stream(STREAM_NAME).await.unwrap();
    producer
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

    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(0);

    let test_duration = Duration::from_secs(duration_secs);
    let start = Instant::now();
    let mut messages_sent = 0u64;

    println!(
        "Starting single message per batch test: sequential payloads, duration: {}s",
        duration_secs
    );

    while start.elapsed() < test_duration {
        let payload = Bytes::from(messages_sent.to_le_bytes().to_vec());
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload)
                .build()
                .expect("Failed to create message"),
        ];

        producer
            .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
            .await
            .unwrap();

        messages_sent += 1;

        if messages_sent.is_multiple_of(1000) {
            println!(
                "Progress: {} messages sent, elapsed: {:.2}s",
                messages_sent,
                start.elapsed().as_secs_f64()
            );
        }
    }

    let send_elapsed = start.elapsed();
    println!(
        "Sending completed: {} messages in {:.2}s ({:.0} msgs/sec)",
        messages_sent,
        send_elapsed.as_secs_f64(),
        messages_sent as f64 / send_elapsed.as_secs_f64()
    );

    println!("Receiving and verifying all {} messages...", messages_sent);
    let consumer = Consumer::default();
    let mut next_offset = 0u64;
    let mut messages_received = 0u64;

    while messages_received < messages_sent {
        let remaining = messages_sent - messages_received;
        let batch_size = remaining.min(POLL_BATCH_SIZE as u64) as u32;

        let polled = producer
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &consumer,
                &PollingStrategy::offset(next_offset),
                batch_size,
                false,
            )
            .await
            .unwrap();

        assert!(
            !polled.messages.is_empty(),
            "Expected messages at offset {}, but got none (sent: {}, received so far: {})",
            next_offset,
            messages_sent,
            messages_received
        );

        for msg in &polled.messages {
            let payload = &msg.payload;
            assert_eq!(
                payload.len(),
                8,
                "Message {} has unexpected payload length: {}",
                messages_received,
                payload.len()
            );

            let expected_value = messages_received;
            let actual_value = u64::from_le_bytes(payload[..8].try_into().unwrap());
            assert_eq!(
                actual_value, expected_value,
                "Message at offset {} has wrong payload: expected {}, got {}",
                next_offset, expected_value, actual_value
            );

            messages_received += 1;
            next_offset += 1;
        }

        if messages_received.is_multiple_of(10000) {
            println!(
                "Verified: {}/{} messages ({:.1}%)",
                messages_received,
                messages_sent,
                (messages_received as f64 / messages_sent as f64) * 100.0
            );
        }
    }

    let total_elapsed = start.elapsed();
    println!(
        "Test completed: {} messages sent and verified in {:.2}s",
        messages_sent,
        total_elapsed.as_secs_f64()
    );

    producer.delete_stream(&stream_id).await.unwrap();
}
