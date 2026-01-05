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

use crate::server::scenarios::{PARTITION_ID, STREAM_NAME, TOPIC_NAME, create_client};
use futures::StreamExt;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::time::{Duration, sleep, timeout};

const MESSAGES_COUNT: u32 = 2000;
const BATCH_LENGTH: u32 = 100;
const CONSUME_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;

    // Existing tests
    test_offset_strategy(&client).await;
    test_timestamp_strategy(&client).await;
    test_first_strategy(&client).await;
    test_last_strategy(&client).await;

    // New API tests for issue #1657
    test_offset_from_middle(&client).await;
    test_offset_beyond_end(&client).await;
    test_timestamp_from_middle(&client).await;
    test_timestamp_future(&client).await;
    test_message_content_verification(&client).await;
    test_message_headers_verification(&client).await;
}

async fn test_offset_strategy(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    let received =
        consume_with_strategy(client, "offset-consumer", PollingStrategy::offset(0)).await;
    assert_received_messages(&received, "Offset");

    cleanup(client).await;
}

async fn test_timestamp_strategy(client: &IggyClient) {
    init_system(client).await;
    let start_timestamp = IggyTimestamp::now();
    produce_messages(client).await;

    let received = consume_with_strategy(
        client,
        "timestamp-consumer",
        PollingStrategy::timestamp(start_timestamp),
    )
    .await;
    assert_received_messages(&received, "Timestamp");

    cleanup(client).await;
}

async fn test_first_strategy(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    let received = consume_with_strategy(client, "first-consumer", PollingStrategy::first()).await;
    assert_received_messages(&received, "First");

    cleanup(client).await;
}

async fn test_last_strategy(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    // Last strategy with batch_length=100 returns the last 100 messages
    let received = consume_with_strategy(client, "last-consumer", PollingStrategy::last()).await;

    assert_eq!(
        received.len(),
        BATCH_LENGTH as usize,
        "Last strategy: expected {} messages, received {}",
        BATCH_LENGTH,
        received.len()
    );

    // Verify messages are from the last batch (offsets 1900-1999 -> message_1901 to message_2000)
    let start_message_num = MESSAGES_COUNT - BATCH_LENGTH + 1;
    for (i, msg) in received.iter().enumerate() {
        let expected_payload = format!("message_{}", start_message_num + i as u32);
        let actual_payload =
            String::from_utf8(msg.payload.to_vec()).expect("Payload should be valid UTF-8");
        assert_eq!(
            actual_payload, expected_payload,
            "Last strategy: message {} payload mismatch",
            i
        );
    }

    cleanup(client).await;
}

fn assert_received_messages(received: &[IggyMessage], strategy_name: &str) {
    assert_eq!(
        received.len(),
        MESSAGES_COUNT as usize,
        "{} strategy: expected {} messages, received {}",
        strategy_name,
        MESSAGES_COUNT,
        received.len()
    );

    for (i, msg) in received.iter().enumerate() {
        let expected_payload = format!("message_{}", i + 1);
        let actual_payload =
            String::from_utf8(msg.payload.to_vec()).expect("Payload should be valid UTF-8");
        assert_eq!(
            actual_payload, expected_payload,
            "{} strategy: message {} payload mismatch",
            strategy_name, i
        );
    }
}

async fn init_system(client: &IggyClient) {
    client.create_stream(STREAM_NAME).await.unwrap();

    client
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
}

async fn produce_messages(client: &IggyClient) {
    let mut messages = Vec::with_capacity(MESSAGES_COUNT as usize);
    for i in 1..=MESSAGES_COUNT {
        let payload = format!("message_{}", i);
        let message = IggyMessage::from_str(&payload).unwrap();
        messages.push(message);
    }

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();
}

async fn consume_with_strategy(
    client: &IggyClient,
    consumer_name: &str,
    strategy: PollingStrategy,
) -> Vec<IggyMessage> {
    let expected_count = if strategy.kind == PollingKind::Last {
        BATCH_LENGTH
    } else {
        MESSAGES_COUNT
    };

    let mut consumer = client
        .consumer(consumer_name, STREAM_NAME, TOPIC_NAME, PARTITION_ID)
        .unwrap()
        .auto_commit(AutoCommit::IntervalOrWhen(
            IggyDuration::from_str("2ms").unwrap(),
            AutoCommitWhen::ConsumingAllMessages,
        ))
        .polling_strategy(strategy)
        .poll_interval(IggyDuration::from_str("2ms").unwrap())
        .batch_length(BATCH_LENGTH)
        .build();

    consumer.init().await.unwrap();

    let mut received_messages = Vec::with_capacity(expected_count as usize);
    let mut consumed_count = 0u32;

    while consumed_count < expected_count {
        match timeout(CONSUME_TIMEOUT, consumer.next()).await {
            Ok(Some(Ok(received))) => {
                received_messages.push(received.message);
                consumed_count += 1;
            }
            Ok(Some(Err(e))) => panic!("Error consuming message: {}", e),
            Ok(None) => break,
            Err(_) => panic!(
                "Timeout after {:?} waiting for message {}/{} with {:?} strategy",
                CONSUME_TIMEOUT, consumed_count, expected_count, strategy.kind
            ),
        }
    }

    received_messages
}

async fn cleanup(client: &IggyClient) {
    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

// ============================================
// New API tests for issue #1657
// ============================================

/// Test polling messages from middle offset
async fn test_offset_from_middle(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    let middle_offset = MESSAGES_COUNT / 2; // 1000
    let expected_count = MESSAGES_COUNT - middle_offset; // 1000

    let received = consume_with_strategy_and_count(
        client,
        "offset-middle-consumer",
        PollingStrategy::offset(middle_offset as u64),
        expected_count,
    )
    .await;

    assert_eq!(
        received.len(),
        expected_count as usize,
        "Offset from middle: expected {} messages, received {}",
        expected_count,
        received.len()
    );

    // Verify first message starts at middle_offset + 1
    let first_payload =
        String::from_utf8(received[0].payload.to_vec()).expect("Payload should be valid UTF-8");
    let expected_first_payload = format!("message_{}", middle_offset + 1);
    assert_eq!(
        first_payload, expected_first_payload,
        "Offset from middle: first message payload mismatch"
    );

    cleanup(client).await;
}

/// Test polling with offset beyond the last message returns empty result
async fn test_offset_beyond_end(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    let beyond_offset = MESSAGES_COUNT; // 2000 (last valid offset is 1999)

    let received =
        poll_messages_direct(client, PollingStrategy::offset(beyond_offset as u64), 10).await;

    assert!(
        received.is_empty(),
        "Offset beyond end: expected empty result, got {} messages",
        received.len()
    );

    cleanup(client).await;
}

/// Test polling messages from middle timestamp
async fn test_timestamp_from_middle(client: &IggyClient) {
    init_system(client).await;

    // Send first batch
    let first_batch_count = MESSAGES_COUNT / 2;
    let mut first_batch = Vec::with_capacity(first_batch_count as usize);
    for i in 1..=first_batch_count {
        let payload = format!("message_{}", i);
        let message = IggyMessage::from_str(&payload).unwrap();
        first_batch.push(message);
    }
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut first_batch,
        )
        .await
        .unwrap();

    // Wait and record middle timestamp
    sleep(Duration::from_millis(50)).await;
    let middle_timestamp = IggyTimestamp::now();
    sleep(Duration::from_millis(50)).await;

    // Send second batch
    let mut second_batch = Vec::with_capacity(first_batch_count as usize);
    for i in (first_batch_count + 1)..=MESSAGES_COUNT {
        let payload = format!("message_{}", i);
        let message = IggyMessage::from_str(&payload).unwrap();
        second_batch.push(message);
    }
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut second_batch,
        )
        .await
        .unwrap();

    // Poll from middle timestamp - should get second batch only
    let received = consume_with_strategy_and_count(
        client,
        "timestamp-middle-consumer",
        PollingStrategy::timestamp(middle_timestamp),
        first_batch_count,
    )
    .await;

    assert_eq!(
        received.len(),
        first_batch_count as usize,
        "Timestamp from middle: expected {} messages, received {}",
        first_batch_count,
        received.len()
    );

    // Verify first message is from second batch
    let first_payload =
        String::from_utf8(received[0].payload.to_vec()).expect("Payload should be valid UTF-8");
    let expected_first_payload = format!("message_{}", first_batch_count + 1);
    assert_eq!(
        first_payload, expected_first_payload,
        "Timestamp from middle: first message payload mismatch"
    );

    cleanup(client).await;
}

/// Test polling with future timestamp returns empty result
async fn test_timestamp_future(client: &IggyClient) {
    init_system(client).await;
    produce_messages(client).await;

    // Create a future timestamp (1 hour from now)
    let future_timestamp = IggyTimestamp::from(IggyTimestamp::now().as_micros() + 3_600_000_000);

    let received =
        poll_messages_direct(client, PollingStrategy::timestamp(future_timestamp), 10).await;

    assert!(
        received.is_empty(),
        "Future timestamp: expected empty result, got {} messages",
        received.len()
    );

    cleanup(client).await;
}

/// Test message content verification (id and payload)
async fn test_message_content_verification(client: &IggyClient) {
    init_system(client).await;

    // Create messages with specific IDs
    let test_count = 100u32;
    let mut messages = Vec::with_capacity(test_count as usize);
    for i in 1..=test_count {
        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(format!("content_{}", i).into())
            .build()
            .unwrap();
        messages.push(message);
    }

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // Poll and verify
    let received = poll_messages_direct(client, PollingStrategy::offset(0), test_count).await;

    assert_eq!(
        received.len(),
        test_count as usize,
        "Content verification: expected {} messages, received {}",
        test_count,
        received.len()
    );

    for (i, msg) in received.iter().enumerate() {
        let expected_id = (i + 1) as u128;
        let expected_payload = format!("content_{}", i + 1);
        let actual_payload =
            String::from_utf8(msg.payload.to_vec()).expect("Payload should be valid UTF-8");

        assert_eq!(
            msg.header.id, expected_id,
            "Content verification: message {} ID mismatch, expected {}, got {}",
            i, expected_id, msg.header.id
        );
        assert_eq!(
            actual_payload, expected_payload,
            "Content verification: message {} payload mismatch",
            i
        );
    }

    cleanup(client).await;
}

/// Test message headers verification
async fn test_message_headers_verification(client: &IggyClient) {
    init_system(client).await;

    // Create messages with headers
    let test_count = 10u32;
    let mut messages = Vec::with_capacity(test_count as usize);
    for i in 1..=test_count {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("string_key").unwrap(),
            HeaderValue::from_str(&format!("value_{}", i)).unwrap(),
        );
        headers.insert(
            HeaderKey::new("number_key").unwrap(),
            HeaderValue::from_uint64(i as u64).unwrap(),
        );
        headers.insert(
            HeaderKey::new("bool_key").unwrap(),
            HeaderValue::from_bool(i % 2 == 0).unwrap(),
        );

        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(format!("message_{}", i).into())
            .user_headers(headers)
            .build()
            .unwrap();
        messages.push(message);
    }

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // Poll and verify headers
    let received = poll_messages_direct(client, PollingStrategy::offset(0), test_count).await;

    assert_eq!(
        received.len(),
        test_count as usize,
        "Headers verification: expected {} messages, received {}",
        test_count,
        received.len()
    );

    for (i, msg) in received.iter().enumerate() {
        let headers = msg.user_headers.as_ref().expect("Headers should exist");
        let headers_map = HashMap::<HeaderKey, HeaderValue>::from_bytes(headers.clone()).unwrap();

        // Verify string header
        let string_value = headers_map
            .get(&HeaderKey::new("string_key").unwrap())
            .expect("string_key should exist");
        assert_eq!(
            string_value.as_str().unwrap(),
            format!("value_{}", i + 1),
            "Headers verification: message {} string_key mismatch",
            i
        );

        // Verify number header
        let number_value = headers_map
            .get(&HeaderKey::new("number_key").unwrap())
            .expect("number_key should exist");
        assert_eq!(
            number_value.as_uint64().unwrap(),
            (i + 1) as u64,
            "Headers verification: message {} number_key mismatch",
            i
        );

        // Verify bool header
        let bool_value = headers_map
            .get(&HeaderKey::new("bool_key").unwrap())
            .expect("bool_key should exist");
        assert_eq!(
            bool_value.as_bool().unwrap(),
            (i + 1) % 2 == 0,
            "Headers verification: message {} bool_key mismatch",
            i
        );
    }

    cleanup(client).await;
}

// ============================================
// Helper functions for new tests
// ============================================

/// Consume messages with a specific strategy and expected count
async fn consume_with_strategy_and_count(
    client: &IggyClient,
    consumer_name: &str,
    strategy: PollingStrategy,
    expected_count: u32,
) -> Vec<IggyMessage> {
    let mut consumer = client
        .consumer(consumer_name, STREAM_NAME, TOPIC_NAME, PARTITION_ID)
        .unwrap()
        .auto_commit(AutoCommit::IntervalOrWhen(
            IggyDuration::from_str("2ms").unwrap(),
            AutoCommitWhen::ConsumingAllMessages,
        ))
        .polling_strategy(strategy)
        .poll_interval(IggyDuration::from_str("2ms").unwrap())
        .batch_length(BATCH_LENGTH)
        .build();

    consumer.init().await.unwrap();

    let mut received_messages = Vec::with_capacity(expected_count as usize);
    let mut consumed_count = 0u32;

    while consumed_count < expected_count {
        match timeout(CONSUME_TIMEOUT, consumer.next()).await {
            Ok(Some(Ok(received))) => {
                received_messages.push(received.message);
                consumed_count += 1;
            }
            Ok(Some(Err(e))) => panic!("Error consuming message: {}", e),
            Ok(None) => break,
            Err(_) => panic!(
                "Timeout after {:?} waiting for message {}/{} with {:?} strategy",
                CONSUME_TIMEOUT, consumed_count, expected_count, strategy.kind
            ),
        }
    }

    received_messages
}

/// Poll messages directly using low-level API (for testing empty results)
async fn poll_messages_direct(
    client: &IggyClient,
    strategy: PollingStrategy,
    count: u32,
) -> Vec<IggyMessage> {
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &Consumer::new(Identifier::named("direct-consumer").unwrap()),
            &strategy,
            count,
            false,
        )
        .await
        .unwrap();

    polled.messages
}
