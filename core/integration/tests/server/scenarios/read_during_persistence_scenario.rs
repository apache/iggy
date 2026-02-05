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
use integration::iggy_harness;
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "eventual-consistency-stream";
const TOPIC_NAME: &str = "eventual-consistency-topic";

/// Test that specifically targets the background message_saver race condition.
///
/// Setup:
/// - HIGH inline persistence thresholds (messages stay in journal during send)
/// - SHORT message_saver interval (background flushes happen frequently)
///
/// The race occurs when:
/// 1. Messages are in journal
/// 2. Background saver calls commit() - journal becomes EMPTY
/// 3. Background saver starts async disk write
/// 4. Poll arrives - sees empty journal, data not yet on disk
#[iggy_harness(server(
    partition.messages_required_to_save = "100000",
    partition.size_of_messages_required_to_save = "1GB",
    partition.enforce_fsync = false,
    message_saver.interval = "100ms",
    message_saver.enabled = true
))]
async fn should_read_messages_during_background_saver_flush(
    harness: &integration::harness::TestHarness,
) {
    let producer = harness.tcp_root_client().await.unwrap();

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

    let consumer_client = harness.tcp_root_client().await.unwrap();

    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let consumer_kind = Consumer::default();

    let test_duration = Duration::from_secs(10);
    let messages_per_batch = 10u32;
    let payload = "X".repeat(512);

    let start = Instant::now();
    let mut batches_sent = 0u64;
    let mut next_offset = 0u64;

    println!(
        "Starting background saver race test: {} msgs/batch, duration: {}s",
        messages_per_batch,
        test_duration.as_secs()
    );
    println!("Inline persistence DISABLED (high thresholds), background saver every 100ms");

    while start.elapsed() < test_duration {
        let base_offset = next_offset;

        let mut messages: Vec<IggyMessage> = (0..messages_per_batch)
            .map(|i| {
                IggyMessage::builder()
                    .id((base_offset + i as u64 + 1) as u128)
                    .payload(Bytes::from(format!(
                        "{} - batch {} msg {}",
                        payload, batches_sent, i
                    )))
                    .build()
                    .expect("Failed to create message")
            })
            .collect();

        producer
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();

        batches_sent += 1;

        let poll_result = consumer_client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &consumer_kind,
                &PollingStrategy::offset(next_offset),
                messages_per_batch,
                false,
            )
            .await;

        let polled_count = match &poll_result {
            Ok(polled) => polled.messages.len() as u32,
            Err(e) => {
                println!("Poll error: {:?}", e);
                0
            }
        };

        if polled_count < messages_per_batch {
            let missing = messages_per_batch - polled_count;
            let elapsed_ms = start.elapsed().as_millis();

            panic!(
                "RACE CONDITION DETECTED after {:.2}s ({} batches), polled from offset {}, expected {}, got {}. Missing: {}",
                elapsed_ms as f64 / 1000.0,
                batches_sent,
                next_offset,
                messages_per_batch,
                polled_count,
                missing
            );
        }

        next_offset += messages_per_batch as u64;

        if batches_sent.is_multiple_of(500) {
            println!(
                "Progress: {} batches, {} messages, elapsed: {:.2}s",
                batches_sent,
                next_offset,
                start.elapsed().as_secs_f64(),
            );
        }
    }

    producer.delete_stream(&stream_id).await.unwrap();
}
