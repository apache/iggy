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

//! A crash restart must not walk a stored consumer offset back below the data that the WAL
//! replay restores (apache/iggy#4726).

use iggy::prelude::*;
use iggy_common::ConsumerOffsetInfo;
use integration::harness::{TestBinaryError, TestHarness};
use integration::iggy_harness;
use std::time::Duration;
use test_case::test_matrix;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "consumer-offset-recovery-stream";
const TOPIC_NAME: &str = "consumer-offset-recovery-topic";
const PARTITION_ID: u32 = 0;
/// Below the default `messages_required_to_save`, so no segment or WAL checkpoint names any of
/// them before the crash, and only the WAL replay restores them.
const MESSAGES_COUNT: u64 = 500;
const BATCH_SIZE: u64 = 100;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// With replicated offsets the replay may not restore the offset store, so an early clamp would
/// stick. With persisted offsets it does, and only the clamp's warning would show.
#[iggy_harness(cluster_nodes = 1)]
#[test_matrix([replicated_offsets(), persisted_offsets()])]
async fn given_persisted_messages_when_the_server_crashes_should_keep_the_stored_offset(
    harness: &mut TestHarness,
    consumer_offset_durability: Durability,
) {
    let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let consumer = Consumer::new(Identifier::numeric(1).expect("consumer identifier"));
    let last_offset = MESSAGES_COUNT - 1;

    let client = harness.tcp_root_client().await.expect("TCP root client");
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Persisted,
                consumer_offset_durability,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    for batch in 0..MESSAGES_COUNT / BATCH_SIZE {
        let mut messages = (0..BATCH_SIZE)
            .map(|index| {
                IggyMessage::builder()
                    .payload(format!("message-{}", batch * BATCH_SIZE + index).into())
                    .build()
                    .expect("build message")
            })
            .collect::<Vec<_>>();
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send messages");
    }
    client
        .store_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID), last_offset)
        .await
        .expect("store the offset of the last message");
    drop(client);

    harness.kill_node(0).expect("kill the server");
    harness.restart_node(0).expect("restart the server");

    let recovered = wait_for_recovered_offset(harness, &consumer, &stream, &topic, last_offset)
        .await
        .expect("the restarted server should serve the consumer offset");
    assert_eq!(
        recovered.stored_offset, last_offset,
        "the crash must not walk the stored offset back below the recovered data"
    );
}

fn replicated_offsets() -> Durability {
    Durability::Replicated
}

fn persisted_offsets() -> Durability {
    Durability::Persisted
}

/// Waits until the restarted server answers with the partition data restored up to `last_offset`.
async fn wait_for_recovered_offset(
    harness: &TestHarness,
    consumer: &Consumer,
    stream: &Identifier,
    topic: &Identifier,
    last_offset: u64,
) -> Result<ConsumerOffsetInfo, TestBinaryError> {
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    loop {
        if let Ok(client) = harness.tcp_root_client().await
            && let Ok(Some(offset)) = client
                .get_consumer_offset(consumer, stream, topic, Some(PARTITION_ID))
                .await
            && offset.current_offset == last_offset
        {
            return Ok(offset);
        }
        if Instant::now() >= deadline {
            return Err(TestBinaryError::InvalidState {
                message: format!(
                    "the partition did not recover up to offset {last_offset} within {RECOVERY_TIMEOUT:?}"
                ),
            });
        }
        sleep(RETRY_INTERVAL).await;
    }
}
