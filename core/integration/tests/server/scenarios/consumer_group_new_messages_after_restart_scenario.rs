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

use crate::server::scenarios::{CONSUMER_GROUP_NAME, PARTITION_ID, STREAM_NAME, TOPIC_NAME};
use futures::StreamExt;
use iggy::prelude::*;
use integration::harness::TestHarness;
use std::str::FromStr;
use tokio::time::{Duration, sleep, timeout};

const INITIAL_MESSAGES_COUNT: u32 = 10;
const NEW_MESSAGES_COUNT: u32 = 5;

pub async fn run(harness: &TestHarness) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    init_system(&client).await;
    execute_scenario(harness, &client).await;
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

    client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();
}

async fn execute_scenario(harness: &TestHarness, client: &IggyClient) {
    // 1. Produce initial messages
    produce_messages(client, 1, INITIAL_MESSAGES_COUNT).await;

    // 2. Create a separate client to simulate the runtime
    let runtime_client = harness.new_client().await.unwrap();
    runtime_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    // 3. Create consumer and consume all initial messages
    let mut consumer = create_consumer(&runtime_client).await;
    let consumed_messages = consume_messages(&mut consumer, INITIAL_MESSAGES_COUNT).await;
    assert_eq!(
        consumed_messages.len(),
        INITIAL_MESSAGES_COUNT as usize,
        "Should consume all initial messages"
    );

    for (index, message) in consumed_messages.iter().enumerate() {
        let expected_payload = format!("test_message_{}", index + 1);
        let actual_payload = String::from_utf8_lossy(&message.payload);
        assert_eq!(
            actual_payload, expected_payload,
            "Message content mismatch at index {index}"
        );
    }

    // 4. Wait for auto-commit to process
    sleep(Duration::from_secs(2)).await;

    // 5. A non-member client should be able to query the consumer group offset
    let observer_client = harness.new_client().await.unwrap();
    observer_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let cg_consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let offset_info = observer_client
        .get_consumer_offset(
            &cg_consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .expect("Non-member client should be able to query consumer group offset")
        .expect("Consumer group offset should exist after auto-commit");

    assert_eq!(offset_info.partition_id, PARTITION_ID);
    assert_eq!(
        offset_info.current_offset,
        (INITIAL_MESSAGES_COUNT - 1) as u64,
        "Current offset should reflect all initial messages"
    );
    assert_eq!(
        offset_info.stored_offset,
        (INITIAL_MESSAGES_COUNT - 1) as u64,
        "Stored offset should reflect consumed position"
    );

    // 6. Disconnect the consumer and client (simulating runtime restart)
    drop(consumer);
    runtime_client.disconnect().await.unwrap();
    drop(runtime_client);
    sleep(Duration::from_millis(500)).await;

    // 7. Send new messages after consumer disconnected
    produce_messages(
        client,
        INITIAL_MESSAGES_COUNT + 1,
        INITIAL_MESSAGES_COUNT + NEW_MESSAGES_COUNT,
    )
    .await;

    // 8. Create a new client (simulating runtime restart)
    let new_runtime_client = harness.new_client().await.unwrap();
    new_runtime_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    // 9. Reconnect consumer and consume new messages
    let mut consumer = create_consumer(&new_runtime_client).await;
    let new_messages = consume_messages(&mut consumer, NEW_MESSAGES_COUNT).await;
    assert_eq!(
        new_messages.len(),
        NEW_MESSAGES_COUNT as usize,
        "Should receive all new messages sent after restart"
    );

    for (index, message) in new_messages.iter().enumerate() {
        let expected_payload =
            format!("test_message_{}", INITIAL_MESSAGES_COUNT + 1 + index as u32);
        let actual_payload = String::from_utf8_lossy(&message.payload);
        assert_eq!(
            actual_payload, expected_payload,
            "New message content mismatch at index {index}"
        );
    }

    // 10. Wait for auto-commit after consuming new messages
    sleep(Duration::from_secs(2)).await;

    // 11. Non-member observer should see updated offset after new messages were consumed
    let offset_after = observer_client
        .get_consumer_offset(
            &cg_consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .expect("Non-member client should still be able to query offset")
        .expect("Consumer group offset should exist");

    assert_eq!(
        offset_after.current_offset,
        (INITIAL_MESSAGES_COUNT + NEW_MESSAGES_COUNT - 1) as u64,
        "Current offset should reflect all messages"
    );
    assert_eq!(
        offset_after.stored_offset,
        (INITIAL_MESSAGES_COUNT + NEW_MESSAGES_COUNT - 1) as u64,
        "Stored offset should reflect newly consumed position"
    );

    drop(consumer);
    drop(new_runtime_client);
    observer_client.disconnect().await.unwrap();
    drop(observer_client);
}

async fn produce_messages(client: &IggyClient, start_id: u32, end_id: u32) {
    let mut messages = Vec::new();
    for message_id in start_id..=end_id {
        let payload = format!("test_message_{message_id}");
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

async fn create_consumer(client: &IggyClient) -> IggyConsumer {
    let mut consumer = client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(10)
        .poll_interval(IggyDuration::from_str("100ms").unwrap())
        .polling_strategy(PollingStrategy::next())
        .auto_join_consumer_group()
        .create_consumer_group_if_not_exists()
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .build();

    consumer.init().await.unwrap();
    consumer
}

async fn consume_messages(consumer: &mut IggyConsumer, expected_count: u32) -> Vec<IggyMessage> {
    let mut messages = Vec::with_capacity(expected_count as usize);

    timeout(Duration::from_secs(30), async {
        while messages.len() < expected_count as usize {
            let Some(Ok(polled_message)) = consumer.next().await else {
                continue;
            };
            messages.push(polled_message.message);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Timeout waiting for messages. Expected {expected_count}, received {}",
            messages.len()
        )
    });

    messages
}
