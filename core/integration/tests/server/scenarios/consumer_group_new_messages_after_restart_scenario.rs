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

use crate::server::scenarios::{
    CONSUMER_GROUP_NAME, PARTITION_ID, STREAM_NAME, TOPIC_NAME, create_client,
};
use futures::StreamExt;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::str::FromStr;
use tokio::time::{Duration, sleep, timeout};

const INITIAL_MESSAGES_COUNT: u32 = 10;
const NEW_MESSAGES_COUNT: u32 = 5;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;
    init_system(&client).await;
    execute_scenario(client_factory, &client).await;
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

async fn execute_scenario(client_factory: &dyn ClientFactory, client: &IggyClient) {
    // 1. Produce initial messages
    produce_messages(client, 1, INITIAL_MESSAGES_COUNT).await;

    // 2. Create a separate client to simulate the runtime
    let runtime_client = create_client(client_factory).await;
    login_root(&runtime_client).await;

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

    // 5. Disconnect the consumer and client (simulating runtime restart)
    drop(consumer);
    runtime_client.disconnect().await.unwrap();
    drop(runtime_client);
    sleep(Duration::from_millis(500)).await;

    // 6. Send new messages after consumer disconnected
    produce_messages(
        client,
        INITIAL_MESSAGES_COUNT + 1,
        INITIAL_MESSAGES_COUNT + NEW_MESSAGES_COUNT,
    )
    .await;

    // 7. Create a new client (simulating runtime restart)
    let new_runtime_client = create_client(client_factory).await;
    login_root(&new_runtime_client).await;

    // 8. Reconnect consumer and consume new messages
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

    drop(consumer);
    drop(new_runtime_client);
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
    let mut messages = Vec::new();
    let mut count = 0;

    let result = timeout(Duration::from_secs(30), async {
        while count < expected_count {
            if let Some(message_result) = consumer.next().await {
                match message_result {
                    Ok(polled_message) => {
                        messages.push(polled_message.message);
                        count += 1;
                    }
                    Err(error) => panic!("Error while consuming messages: {error}"),
                }
            }
        }
    })
    .await;

    if result.is_err() {
        panic!("Timeout waiting for messages. Expected {expected_count}, received {count}");
    }

    messages
}
