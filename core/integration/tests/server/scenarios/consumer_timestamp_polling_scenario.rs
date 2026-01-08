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
use std::str::FromStr;
use tokio::time::{Duration, timeout};

const MESSAGES_COUNT: u32 = 2000;
const BATCH_LENGTH: u32 = 100;
const CONSUME_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;

    test_offset_strategy(&client).await;
    test_timestamp_strategy(&client).await;
    test_first_strategy(&client).await;
    test_last_strategy(&client).await;
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
