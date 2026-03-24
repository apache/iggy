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

//! Regression test for issue #2715: consumer offset skip during concurrent produce+consume.
//!
//! Root cause ("State C"):
//!   1. Journal commit() moves data to in-flight buffer (async persist begins)
//!   2. New send arrives before persist completes - journal is non-empty again
//!   3. Consumer polls with Next + auto_commit:
//!      Old code only checked in-flight when journal was empty (Case 0).
//!      With journal non-empty (Case 1-3), in-flight was invisible.
//!      auto_commit stored the journal offset, permanently skipping the in-flight range.
//!
//! Trigger: `messages_required_to_save = "1"` forces an inline journal commit on every
//! batch. With concurrent sends, the next batch arrives while the previous is persisting,
//! reliably producing State C.

use bytes::Bytes;
use iggy::prelude::*;
use integration::iggy_harness;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "issue-2715-stream";
const TOPIC_NAME: &str = "issue-2715-topic";
const TOTAL_MESSAGES: u64 = 500;
const PRODUCER_BATCH_SIZE: u32 = 5;
const CONSUMER_BATCH_SIZE: u32 = 10;
const MAX_TEST_DURATION: Duration = Duration::from_secs(60);

/// Regression test for issue #2715: consumer must not skip offsets during
/// concurrent produce+consume when the journal commits while in-flight data exists.
///
/// `messages_required_to_save = "1"` forces every batch to trigger an inline
/// journal commit. The next send arrives before async persist completes, creating
/// State C (journal non-empty + in-flight non-empty simultaneously).
#[iggy_harness(server(
    partition.messages_required_to_save = "1",
    partition.enforce_fsync = false,
    message_saver.enabled = true,
    message_saver.interval = "100ms"
))]
async fn concurrent_produce_consume_no_offset_skip(harness: &TestHarness) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();

    let setup = harness.tcp_root_client().await.unwrap();
    setup.create_stream(STREAM_NAME).await.unwrap();
    setup
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
    drop(setup);

    let producer_client = harness.tcp_root_client().await.unwrap();
    let consumer_client = harness.tcp_root_client().await.unwrap();

    let producer_done = Arc::new(AtomicBool::new(false));
    let producer_done_clone = producer_done.clone();

    // Consumer: polls with Next + auto_commit concurrently with the producer.
    // Checks that received offsets are strictly contiguous (no gaps).
    let consumer_task = tokio::spawn(async move {
        let stream = Identifier::named(STREAM_NAME).unwrap();
        let topic = Identifier::named(TOPIC_NAME).unwrap();
        let consumer = Consumer::default();
        let mut last_offset: Option<u64> = None;
        let mut total_received = 0u64;
        let mut consecutive_empty = 0u32;
        let deadline = Instant::now() + MAX_TEST_DURATION;

        loop {
            if Instant::now() >= deadline {
                panic!(
                    "Consumer timed out after {MAX_TEST_DURATION:?}. \
                     Received {total_received}/{TOTAL_MESSAGES}, \
                     last_offset: {last_offset:?}"
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
                    true, // auto_commit - this is what makes the skip permanent
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
                                "Offset gap! Expected {}, got {} (skipped {} messages). \
                                 Issue #2715: in-flight buffer invisible when journal non-empty.",
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
                    eprintln!("Consumer poll error (transient under load): {e:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        total_received
    });

    // Producer: sends TOTAL_MESSAGES in a tight loop to maximize State C window.
    let mut sent = 0u64;
    while sent < TOTAL_MESSAGES {
        let batch_count = PRODUCER_BATCH_SIZE.min((TOTAL_MESSAGES - sent) as u32);
        let mut messages: Vec<IggyMessage> = (0..batch_count)
            .map(|i| {
                IggyMessage::builder()
                    .payload(Bytes::from(format!("msg-{}", sent + i as u64)))
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
    }

    producer_done.store(true, Ordering::Relaxed);

    let total_received = consumer_task.await.unwrap();
    assert_eq!(
        total_received,
        TOTAL_MESSAGES,
        "Consumer received {total_received}/{TOTAL_MESSAGES} - \
         missing {} messages (issue #2715 offset skip regression).",
        TOTAL_MESSAGES - total_received,
    );

    producer_client.delete_stream(&stream_id).await.unwrap();
}
