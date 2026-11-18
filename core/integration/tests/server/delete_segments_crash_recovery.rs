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

//! A crash restart after `delete_segments` removed the segment that the WAL checkpoint cursor
//! ends must serve the data written past it (apache/iggy#4950).

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestBinaryError, TestHarness};
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "delete-segments-crash-stream";
const TOPIC_NAME: &str = "delete-segments-crash-topic";
const PARTITION_ID: u32 = 0;
/// The smallest segment a topic may declare.
const SEGMENT_SIZE: u64 = 1024 * 1024;
const SEALING_PAYLOAD_SIZE: usize = 26_000;
/// A single message batch takes 26,304 bytes of the segment with its headers, so forty messages
/// fill a [`SEGMENT_SIZE`] segment and thirty nine do not. The fortieth seals the segment and
/// the next offset starts the following one.
const SEALING_MESSAGES: u64 = 40;
/// The later batches span more offsets than the reservation lease of 64, so one of them claims a
/// new reservation, and the claim records a frontier above the sealed segment.
const LATER_BATCHES: u64 = 3;
const LATER_BATCH_SIZE: u64 = 50;
const DELETE_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// With no traffic after the sealing message, the checkpoint that the delete requests lands
/// exactly at the end of the sealed segment, and the delete then removes that segment. The
/// batches sent after it claim reservations that record a superblock frontier above that cursor.
/// A boot must plant the emptied chain at the cursor, not at the frontier.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.offset_reservation_lease = "64")
)]
async fn given_a_deleted_segment_ending_at_the_checkpoint_when_the_server_crashes_should_serve_the_later_messages(
    harness: &mut TestHarness,
) {
    let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");

    let client = harness.tcp_root_client().await.expect("TCP root client");
    let stream_details = client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let topic_details = client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                segment_size: Some(IggyByteSize::from(SEGMENT_SIZE)),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let first_segment = harness.node(0).data_path().join(format!(
        "streams/{}/topics/{}/partitions/{PARTITION_ID}/{:020}.log",
        stream_details.id, topic_details.id, 0
    ));

    for offset in 0..SEALING_MESSAGES {
        let payload = Bytes::from(vec![b'x'; SEALING_PAYLOAD_SIZE]);
        send_batch(&client, &stream, &topic, vec![(offset, payload)]).await;
    }
    client
        .delete_segments(&stream, &topic, PARTITION_ID, 1)
        .await
        .expect("delete the sealed segment");
    wait_until_removed(&first_segment).await;

    for batch in 0..LATER_BATCHES {
        let first = SEALING_MESSAGES + batch * LATER_BATCH_SIZE;
        let batch = (first..first + LATER_BATCH_SIZE)
            .map(|offset| (offset, Bytes::from(format!("message-{offset}"))))
            .collect();
        send_batch(&client, &stream, &topic, batch).await;
    }
    drop(client);

    harness.kill_node(0).expect("kill the server");
    harness.restart_node(0).expect("restart the server");

    let last_offset = SEALING_MESSAGES + LATER_BATCHES * LATER_BATCH_SIZE - 1;
    let offsets = wait_for_offsets(harness, &stream, &topic, last_offset)
        .await
        .expect("the restarted server should serve every message past the deleted segment");
    assert_eq!(
        offsets,
        (SEALING_MESSAGES..=last_offset).collect::<Vec<_>>(),
        "the restart must serve exactly the messages past the deleted segment"
    );
}

async fn send_batch(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    batch: Vec<(u64, Bytes)>,
) {
    let mut messages = batch
        .into_iter()
        .map(|(offset, payload)| {
            IggyMessage::builder()
                .id(u128::from(offset))
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect::<Vec<_>>();
    client
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send messages");
}

async fn wait_until_removed(path: &std::path::Path) {
    let deadline = Instant::now() + DELETE_TIMEOUT;
    while path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} was not removed within {DELETE_TIMEOUT:?}",
            path.display()
        );
        sleep(RETRY_INTERVAL).await;
    }
}

/// Waits until the restarted server polls every message from the first offset past the deleted
/// segment up to `last_offset`.
async fn wait_for_offsets(
    harness: &TestHarness,
    stream: &Identifier,
    topic: &Identifier,
    last_offset: u64,
) -> Result<Vec<u64>, TestBinaryError> {
    let consumer = Consumer::new(Identifier::numeric(1).expect("consumer identifier"));
    let count = u32::try_from(last_offset).expect("a small offset");
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    loop {
        if let Ok(client) = harness.tcp_root_client().await
            && let Ok(polled) = client
                .poll_messages(
                    stream,
                    topic,
                    PARTITION_ID,
                    &consumer,
                    &PollingStrategy::offset(SEALING_MESSAGES),
                    PollOptions {
                        count,
                        ..PollOptions::default()
                    },
                )
                .await
        {
            let offsets = polled
                .messages
                .iter()
                .map(|message| message.header.offset)
                .collect::<Vec<_>>();
            if offsets.last() == Some(&last_offset) {
                return Ok(offsets);
            }
        }
        if Instant::now() >= deadline {
            return Err(TestBinaryError::InvalidState {
                message: format!(
                    "the partition did not serve offset {last_offset} within {RECOVERY_TIMEOUT:?}"
                ),
            });
        }
        sleep(RETRY_INTERVAL).await;
    }
}
