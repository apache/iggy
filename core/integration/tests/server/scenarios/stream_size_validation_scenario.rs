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

use crate::server::scenarios::{PARTITION_ID, PARTITIONS_COUNT};
use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestHarness, assert_clean_system, login_root};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// The partition plane applies committed ops asynchronously on the owning
// shard (sends fold into the shared stats at commit-apply; purge/delete
// zero them when the reconciler drives the wipe), so a read racing that
// window can see a pre-apply value. Retry until the expectation holds,
// then make the terminal assertion for a real mismatch.
const STATS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const STATS_RETRY_INTERVAL: Duration = Duration::from_millis(100);

const S1_NAME: &str = "test-stream-1";
const T1_NAME: &str = "test-topic-1";
const S2_NAME: &str = "test-stream-2";
const T2_NAME: &str = "test-topic-2";
const MESSAGE_PAYLOAD_SIZE_BYTES: u64 = 57;
#[cfg(not(feature = "vsr"))]
const MSG_SIZE: u64 = IGGY_MESSAGE_HEADER_SIZE as u64 + MESSAGE_PAYLOAD_SIZE_BYTES; // number of bytes in a single message
const MSGS_COUNT: u64 = 117; // number of messages in a single topic after one pass of appending
#[cfg(not(feature = "vsr"))]
const MSGS_SIZE: u64 = MSG_SIZE * MSGS_COUNT; // number of bytes in a single topic after one pass of appending
// server-ng accounts the actual on-disk batch framing: one 256-byte
// `SendMessages2` command header per append pass plus a 48-byte per-message
// header (`server_common::send_messages2::{COMMAND_HEADER_SIZE,
// MESSAGE_HEADER_SIZE}`), instead of the legacy 64-byte per-message header.
// Each pass below sends all `MSGS_COUNT` messages in one batch.
#[cfg(feature = "vsr")]
const NG_BATCH_HEADER_SIZE: u64 = 256;
#[cfg(feature = "vsr")]
const NG_MESSAGE_HEADER_SIZE: u64 = 48;
#[cfg(feature = "vsr")]
const MSGS_SIZE: u64 =
    NG_BATCH_HEADER_SIZE + (NG_MESSAGE_HEADER_SIZE + MESSAGE_PAYLOAD_SIZE_BYTES) * MSGS_COUNT;

pub async fn run(harness: &TestHarness) {
    let client = harness
        .new_client()
        .await
        .expect("Failed to create new client");

    // 0. Ping server, login as root user and ensure that streams do not exist
    ping_login_and_validate(&client).await;

    // 1. Create first stream
    create_stream_assert_empty(&client, S1_NAME).await;

    // 2. Create second stream
    create_stream_assert_empty(&client, S2_NAME).await;

    // 3. Create first topic on the first stream
    create_topic_assert_empty(&client, S1_NAME, T1_NAME).await;

    // 4. Do operations on the first topic, first stream and validate sizes
    validate_operations_on_topic_twice(&client, S1_NAME, T1_NAME, PARTITION_ID).await;

    // 5. Validate both streams, second stream should be empty
    validate_stream(&client, S1_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_stream(&client, S2_NAME, 0, 0).await;

    // 6. Create second topic on the first stream
    create_topic_assert_empty(&client, S1_NAME, T2_NAME).await;

    // 7. Do operations on the second topic, first stream and validate sizes
    validate_operations_on_topic_twice(&client, S1_NAME, T2_NAME, PARTITION_ID).await;

    // 8. Create first topic on the second stream
    create_topic_assert_empty(&client, S2_NAME, T1_NAME).await;

    // 9. Do operations on the first topic, second stream and validate sizes
    validate_operations_on_topic_twice(&client, S2_NAME, T1_NAME, PARTITION_ID).await;

    // 10. Create second topic on the second stream
    create_topic_assert_empty(&client, S2_NAME, T2_NAME).await;

    // 11. Do operations on the second topic, second stream and validate sizes
    validate_operations_on_topic_twice(&client, S2_NAME, T2_NAME, PARTITION_ID).await;

    // 12. Validate both streams, should have exactly same sizes and number of messages
    validate_stream(&client, S1_NAME, MSGS_SIZE * 4, MSGS_COUNT * 4).await;
    validate_stream(&client, S2_NAME, MSGS_SIZE * 4, MSGS_COUNT * 4).await;

    // 13. Validate all topics, should have exactly same sizes and number of messages
    validate_topic(&client, S1_NAME, T1_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_topic(&client, S1_NAME, T2_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_topic(&client, S2_NAME, T1_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_topic(&client, S2_NAME, T2_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;

    // 13a. System stats must aggregate all streams: 2 streams × 2 topics × MSGS_COUNT*2.
    validate_system_stats(&client, MSGS_SIZE * 8, MSGS_COUNT * 8).await;

    // 13b. Purge T1 from S1 while T2 still has messages — S1's system-level stats
    // must reflect only T2, not be blindly zeroed.
    //
    // Regression: zero_out_all() propagated store(0) up to StreamStats, zeroing the
    // entire stream counter instead of subtracting only the purged topic's contribution.
    // get_stats() reads StreamStats atomics directly, so it reports 0 for the stream
    // while the sibling topic still has MSGS_COUNT*2 messages.
    purge_topic(&client, S1_NAME, T1_NAME).await;
    validate_topic(&client, S1_NAME, T1_NAME, 0, 0).await;
    validate_topic(&client, S1_NAME, T2_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_stream(&client, S1_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    // S1 lost MSGS_COUNT*2 from the purge; S2 is unchanged.
    validate_system_stats(&client, MSGS_SIZE * 6, MSGS_COUNT * 6).await;

    // 14. Delete first topic on the first stream
    delete_topic(&client, S1_NAME, T1_NAME).await;

    // 15. Validate both streams, first should have its message count and size should be reduced by 50%, second stream should be unchanged
    validate_stream(&client, S1_NAME, MSGS_SIZE * 2, MSGS_COUNT * 2).await;
    validate_stream(&client, S2_NAME, MSGS_SIZE * 4, MSGS_COUNT * 4).await;

    // 16. Purge second topic on the first stream
    purge_topic(&client, S1_NAME, T2_NAME).await;

    // 17. Validate both streams, first should be empty, second should be unchanged
    validate_stream(&client, S1_NAME, 0, 0).await;
    validate_stream(&client, S2_NAME, MSGS_SIZE * 4, MSGS_COUNT * 4).await;

    // 18. Delete first stream
    delete_stream(&client, S1_NAME).await;

    // 19. Validate second stream, should be unchanged
    validate_stream(&client, S2_NAME, MSGS_SIZE * 4, MSGS_COUNT * 4).await;

    // 20. Purge second stream
    purge_stream(&client, S2_NAME).await;

    // 21. Validate second stream and its topics, should be empty
    validate_stream(&client, S2_NAME, 0, 0).await;
    validate_topic(&client, S2_NAME, T1_NAME, 0, 0).await;
    validate_topic(&client, S2_NAME, T2_NAME, 0, 0).await;

    // 22. Delete second stream
    delete_stream(&client, S2_NAME).await;

    // 23. Validate system, should be empty
    assert_clean_system(&client).await;
}

async fn ping_login_and_validate(client: &IggyClient) {
    // 1. Ping server
    client.ping().await.unwrap();

    // 2. Login as root user
    login_root(client).await.expect("login failed");

    // 3. Ensure that streams do not exist
    let streams = client.get_streams().await.unwrap();
    assert!(streams.is_empty());
}

async fn create_topic_assert_empty(client: &IggyClient, stream_name: &str, topic_name: &str) {
    // 1. Create topic
    client
        .create_topic(
            &Identifier::from_str(stream_name).unwrap(),
            topic_name,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    // 2. Validate topic size and number of messages
    validate_topic(client, stream_name, topic_name, 0, 0).await;
}

async fn create_stream_assert_empty(client: &IggyClient, stream_name: &str) {
    // 1. Create stream
    client.create_stream(stream_name).await.unwrap();

    // 2. Validate stream size and number of messages
    validate_stream(client, stream_name, 0, 0).await;
}

async fn validate_operations_on_topic_twice(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    partition_id: u32,
) {
    // 1. Append messages to the topic
    let mut messages = create_messages();
    client
        .send_messages(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .unwrap();

    // 2. Validate topic size and number of messages
    validate_topic(client, stream_name, topic_name, MSGS_SIZE, MSGS_COUNT).await;

    // 3. Again append same number of messages to the topic
    let mut messages = create_messages();
    client
        .send_messages(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .unwrap();

    // 4. Validate topic size and number of messages
    validate_topic(
        client,
        stream_name,
        topic_name,
        MSGS_SIZE * 2,
        MSGS_COUNT * 2,
    )
    .await;
}

async fn validate_system_stats(
    client: &IggyClient,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stats = loop {
        let stats = client.get_stats().await.unwrap();
        if (stats.messages_count == expected_messages_count
            && stats.messages_size_bytes.as_bytes_u64() == expected_size)
            || Instant::now() >= deadline
        {
            break stats;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(
        stats.messages_count, expected_messages_count,
        "system stats messages_count mismatch"
    );
    assert_eq!(
        stats.messages_size_bytes.as_bytes_u64(),
        expected_size,
        "system stats messages_size_bytes mismatch"
    );
}

async fn validate_stream(
    client: &IggyClient,
    stream_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    // 1. Fetch until the async commit-apply converges (see the retry note at
    // the top of the file).
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stream = loop {
        let stream = client
            .get_stream(&Identifier::from_str(stream_name).unwrap())
            .await
            .unwrap()
            .expect("Failed to get stream");
        if (stream.size == expected_size && stream.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break stream;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };

    // 2. Validate stream size and number of messages
    assert_eq!(stream.size, expected_size);
    assert_eq!(stream.messages_count, expected_messages_count);
}

async fn validate_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    // 1. Fetch until the async commit-apply converges (see the retry note at
    // the top of the file).
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let topic = loop {
        let topic = client
            .get_topic(
                &Identifier::from_str(stream_name).unwrap(),
                &Identifier::from_str(topic_name).unwrap(),
            )
            .await
            .unwrap()
            .expect("Failed to get topic");
        if (topic.size == expected_size && topic.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break topic;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };

    // 2. Validate topic size and number of messages
    assert_eq!(topic.size, expected_size);
    assert_eq!(topic.messages_count, expected_messages_count);
}

async fn delete_topic(client: &IggyClient, stream_name: &str, topic_name: &str) {
    client
        .delete_topic(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
        )
        .await
        .unwrap();
}

async fn purge_topic(client: &IggyClient, stream_name: &str, topic_name: &str) {
    client
        .purge_topic(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
        )
        .await
        .unwrap();
}

async fn delete_stream(client: &IggyClient, stream_name: &str) {
    client
        .delete_stream(&Identifier::from_str(stream_name).unwrap())
        .await
        .unwrap();
}

async fn purge_stream(client: &IggyClient, stream_name: &str) {
    client
        .purge_stream(&Identifier::from_str(stream_name).unwrap())
        .await
        .unwrap();
}

fn create_messages() -> Vec<IggyMessage> {
    let mut messages = Vec::new();
    for offset in 0..MSGS_COUNT {
        let id = (offset + 1) as u128;
        let payload = Bytes::from(vec![0xD; MESSAGE_PAYLOAD_SIZE_BYTES as usize]);

        let message = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .build()
            .expect("Failed to create message");
        messages.push(message);
    }
    messages
}
