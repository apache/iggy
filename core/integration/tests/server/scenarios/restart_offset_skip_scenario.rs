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

//! Regression test: consumer offset skip after server restart during concurrent
//! produce+consume.
//!
//! Root cause: after restart, `MemoryMessageJournal` defaults to `base_offset=0`
//! instead of `partition.current_offset + 1`. This causes the three-tier lookup
//! (`disk -> in-flight -> journal`) to skip disk reads, and `slice_by_offset`
//! silently returns messages from wrong offsets. Combined with `auto_commit`,
//! the skip is permanent.
//!
//! Reproduction (matches colleague's scenario):
//!   1. Start server, create stream/topic
//!   2. Send 100 messages
//!   3. Restart server
//!   4. Send more messages concurrently (every 10ms)
//!   5. Consumer group polls with batch_size=10, Next strategy, auto_commit
//!   6. Assert: no offset gaps (previously skipped 3-6 messages)

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestBinary, TestHarness};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "restart-skip-stream";
const TOPIC_NAME: &str = "restart-skip-topic";
const CONSUMER_GROUP_NAME: &str = "restart-skip-cg";
// Enough pre-restart messages so the consumer is still reading disk data when
// journal entries start appearing. With batch_size=10, the consumer needs many
// poll cycles to drain these, creating the window for the bug.
const PRE_RESTART_MESSAGES: u32 = 1000;
const POST_RESTART_MESSAGES: u64 = 200;
const PRODUCER_BATCH_SIZE: u32 = 5;
const CONSUMER_BATCH_SIZE: u32 = 10;
const MAX_TEST_DURATION: Duration = Duration::from_secs(120);

pub async fn run(harness: &mut TestHarness) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();

    // Step 1-2: Create stream/topic, send pre-restart messages
    let setup_client = harness.tcp_root_client().await.unwrap();
    setup_client.create_stream(STREAM_NAME).await.unwrap();
    setup_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    setup_client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();

    let mut pre_messages: Vec<IggyMessage> = (0..PRE_RESTART_MESSAGES)
        .map(|i| {
            IggyMessage::builder()
                .payload(Bytes::from(format!("pre-{i}")))
                .build()
                .unwrap()
        })
        .collect();

    setup_client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut pre_messages,
        )
        .await
        .unwrap();

    // Explicitly flush to disk, then wait for message_saver to persist
    setup_client
        .flush_unsaved_buffer(
            &stream_id, &topic_id, 0, true, // fsync
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(setup_client);

    // Step 3: Restart server
    harness.server_mut().stop().expect("Failed to stop server");
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness
        .server_mut()
        .start()
        .expect("Failed to start server");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Step 4-5: Concurrent produce + consume after restart.
    // Key: producer sends a burst FIRST so the journal has data (with wrong
    // base_offset=0) BEFORE the consumer starts reading disk offsets. This is
    // the window that triggers the bug.
    let producer_client = harness.tcp_root_client().await.unwrap();

    // Send an initial burst so the journal is populated before consumer starts
    for i in 0..20u32 {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(Bytes::from(format!("burst-{i}")))
                .build()
                .unwrap(),
        ];
        producer_client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let consumer_client = harness.tcp_root_client().await.unwrap();

    // Consumer joins the consumer group
    consumer_client
        .join_consumer_group(
            &stream_id,
            &topic_id,
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    let producer_done = Arc::new(AtomicBool::new(false));
    let producer_done_clone = producer_done.clone();

    // Consumer task: polls with Next + auto_commit, checks contiguous offsets
    let consumer_task = tokio::spawn(async move {
        let stream = Identifier::named(STREAM_NAME).unwrap();
        let topic = Identifier::named(TOPIC_NAME).unwrap();
        let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
        let mut last_offset: Option<u64> = None;
        let mut total_received = 0u64;
        let mut consecutive_empty = 0u32;
        let deadline = Instant::now() + MAX_TEST_DURATION;

        loop {
            if Instant::now() >= deadline {
                panic!(
                    "Consumer timed out after {MAX_TEST_DURATION:?}. \
                     Received {total_received}, last_offset: {last_offset:?}"
                );
            }

            let result = consumer_client
                .poll_messages(
                    &stream,
                    &topic,
                    Some(0),
                    &consumer,
                    &PollingStrategy::next(),
                    CONSUMER_BATCH_SIZE,
                    true, // auto_commit
                )
                .await;

            match result {
                Ok(polled) if polled.messages.is_empty() => {
                    if producer_done_clone.load(Ordering::Relaxed) {
                        consecutive_empty += 1;
                        if consecutive_empty >= 20 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                Ok(polled) => {
                    consecutive_empty = 0;
                    for msg in &polled.messages {
                        let offset = msg.header.offset;
                        if let Some(last) = last_offset {
                            assert_eq!(
                                offset,
                                last + 1,
                                "OFFSET SKIP after restart! Expected {}, got {} \
                                 (skipped {} messages). Journal base_offset was \
                                 not initialized after bootstrap.",
                                last + 1,
                                offset,
                                offset.saturating_sub(last + 1),
                            );
                        }
                        last_offset = Some(offset);
                        total_received += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Consumer poll error: {e:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        (total_received, last_offset)
    });

    // Producer: sends messages every 10ms after restart (matches colleague's scenario)
    let mut sent = 0u64;
    while sent < POST_RESTART_MESSAGES {
        let batch_count = PRODUCER_BATCH_SIZE.min((POST_RESTART_MESSAGES - sent) as u32);
        let mut messages: Vec<IggyMessage> = (0..batch_count)
            .map(|i| {
                IggyMessage::builder()
                    .payload(Bytes::from(format!("post-{}", sent + i as u64)))
                    .build()
                    .unwrap()
            })
            .collect();

        producer_client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap_or_else(|e| panic!("Producer send failed at sent={sent}: {e}"));

        sent += batch_count as u64;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    producer_done.store(true, Ordering::Relaxed);

    let (total_received, last_offset) = consumer_task.await.unwrap();

    let initial_burst = 20u64;
    let expected_total = PRE_RESTART_MESSAGES as u64 + initial_burst + POST_RESTART_MESSAGES;
    assert_eq!(
        total_received,
        expected_total,
        "Consumer received {total_received}/{expected_total} messages. \
         Last offset: {last_offset:?}. Missing {} messages.",
        expected_total - total_received,
    );

    producer_client.delete_stream(&stream_id).await.unwrap();
}
