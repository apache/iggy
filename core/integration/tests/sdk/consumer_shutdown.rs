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

use std::str::FromStr;

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::{Duration, Instant, timeout};

const STREAM_NAME: &str = "consumer-shutdown-stream";
const TOPIC_NAME: &str = "consumer-shutdown-topic";
const CONSUMER_NAME: &str = "consumer-shutdown-consumer";
const PARTITION_ID: u32 = 0;
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
const OFFSET_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

// `AutoCommit::Disabled` leaves every commit to the caller, so the final flush of `shutdown()`
// must not run either: it would commit a message whose handler failed.
#[iggy_harness]
async fn given_disabled_auto_commit_when_shutdown_should_not_store_the_reading_position(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();

    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut messages = vec![
        IggyMessage::from_str("message_1").unwrap(),
        IggyMessage::from_str("message_2").unwrap(),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // Both messages must come in one batch: with nothing committed, `next()` serves the same
    // batch again and the consumer's own filter drops it, so a second poll would stall.
    let mut consumer = client
        .consumer(CONSUMER_NAME, STREAM_NAME, TOPIC_NAME, PARTITION_ID)
        .unwrap()
        .auto_commit(AutoCommit::Disabled)
        .batch_length(2)
        .offset_drain_timeout(IggyDuration::from(OFFSET_DRAIN_TIMEOUT))
        .build();
    consumer.init().await.unwrap();

    for expected_offset in [0, 1] {
        let received = timeout(POLL_TIMEOUT, consumer.next())
            .await
            .expect("Consumer should receive a message before timeout")
            .expect("Consumer stream should remain open")
            .expect("Consumer should poll a message");
        assert_eq!(received.message.header.offset, expected_offset);
    }
    assert_eq!(consumer.get_last_consumed_offset(PARTITION_ID), Some(1));

    // The store task has to exit on the shutdown flag. One that never exits only costs the drain
    // timeout and a warning, so the stored offset alone would not catch it.
    let shutdown_started = Instant::now();
    consumer.shutdown().await.unwrap();
    assert!(
        shutdown_started.elapsed() < OFFSET_DRAIN_TIMEOUT,
        "shutdown() must not wait for the offset drain timeout"
    );

    let stored_offset = client
        .get_consumer_offset(
            &Consumer::new(Identifier::named(CONSUMER_NAME).unwrap()),
            &stream_id,
            &topic_id,
            Some(PARTITION_ID),
        )
        .await
        .unwrap();
    assert!(
        stored_offset.is_none(),
        "nothing must be stored under AutoCommit::Disabled, got {stored_offset:?}"
    );
}
