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
use iggy_common::ConsumerOffsetInfo;
use integration::test_server::{ClientFactory, login_root};
use std::str::FromStr;
use tokio::time::{Duration, sleep};

const TEST_MESSAGES_COUNT: u32 = 100;
const HALF_MESSAGES_COUNT: u32 = TEST_MESSAGES_COUNT / 2;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;
    init_system(&client).await;
    execute_auto_commit_reconnection_scenario(&client).await;
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic
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

    // 3. Create the consumer group
    client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();
}

async fn execute_auto_commit_reconnection_scenario(client: &IggyClient) {
    // Step 1: Produce messages to exactly the same stream/topic/partition
    produce_messages_to_partition(client).await;

    // Step 2: Create a consumer group with singular consumer that uses auto_commit
    let mut consumer = create_auto_commit_consumer(client).await;

    // Step 3: Consume 50% of the produced messages
    let consumed_messages = consume_half_messages(&mut consumer).await;
    assert_eq!(consumed_messages.len(), HALF_MESSAGES_COUNT as usize);

    // Wait a bit to ensure auto-commit has processed
    sleep(Duration::from_secs(2)).await;

    // Step 4: Disconnect the consumer
    drop(consumer);

    // Step 5: Check if the committed offset is at the place where it should be (halfway through)
    let committed_offset_after_half = get_committed_offset(client).await;
    assert!(committed_offset_after_half.is_some());
    let offset_info = committed_offset_after_half.unwrap();
    // The offset should be around half of the messages (offset is 0-based)
    assert_eq!(offset_info.stored_offset, (HALF_MESSAGES_COUNT - 1) as u64);

    // Step 6: Reconnect the consumer
    let mut consumer = create_auto_commit_consumer(client).await;

    // Step 7: Consume rest of the messages
    let remaining_messages = consume_remaining_messages(&mut consumer).await;
    assert_eq!(remaining_messages.len(), HALF_MESSAGES_COUNT as usize);

    // Wait a bit to ensure auto-commit has processed
    sleep(Duration::from_secs(2)).await;

    // Step 8: Check if the committed offset is at the end
    let committed_offset_final = get_committed_offset(client).await;
    assert!(committed_offset_final.is_some());
    let final_offset_info = committed_offset_final.unwrap();
    // The offset should be at the last message (offset is 0-based)
    assert_eq!(
        final_offset_info.stored_offset,
        (TEST_MESSAGES_COUNT - 1) as u64
    );

    drop(consumer);
}

async fn produce_messages_to_partition(client: &IggyClient) {
    let mut messages = Vec::new();
    for message_id in 1..=TEST_MESSAGES_COUNT {
        let payload = format!("test_message_{}", message_id);
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

async fn create_auto_commit_consumer(client: &IggyClient) -> IggyConsumer {
    let mut consumer = client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(10)
        .poll_interval(IggyDuration::from_str("100ms").expect("Invalid duration"))
        .polling_strategy(PollingStrategy::next())
        .auto_join_consumer_group()
        .auto_commit(AutoCommit::IntervalOrAfter(
            IggyDuration::from_str("1s").unwrap(),
            AutoCommitAfter::ConsumingEachMessage,
        ))
        .build();

    consumer.init().await.unwrap();
    consumer
}

async fn consume_half_messages(consumer: &mut IggyConsumer) -> Vec<IggyMessage> {
    let mut consumed_messages = Vec::new();
    let mut count = 0;

    while count < HALF_MESSAGES_COUNT {
        if let Some(message_result) = consumer.next().await {
            match message_result {
                Ok(polled_message) => {
                    consumed_messages.push(polled_message.message);
                    count += 1;
                }
                Err(e) => panic!("Error while consuming messages: {}", e),
            }
        }
    }

    consumed_messages
}

async fn consume_remaining_messages(consumer: &mut IggyConsumer) -> Vec<IggyMessage> {
    let mut consumed_messages = Vec::new();
    let mut count = 0;

    while count < HALF_MESSAGES_COUNT {
        if let Some(message_result) = consumer.next().await {
            match message_result {
                Ok(polled_message) => {
                    consumed_messages.push(polled_message.message);
                    count += 1;
                }
                Err(e) => panic!("Error while consuming remaining messages: {}", e),
            }
        }
    }

    consumed_messages
}

async fn get_committed_offset(client: &IggyClient) -> Option<ConsumerOffsetInfo> {
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    client
        .get_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .unwrap()
}
