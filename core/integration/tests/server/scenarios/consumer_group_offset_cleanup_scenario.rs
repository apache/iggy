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

use crate::server::scenarios::{PARTITION_ID, STREAM_NAME, TOPIC_NAME, cleanup, create_client};
use futures::StreamExt;
use iggy::prelude::*;
use iggy_common::ConsumerOffsetInfo;
use integration::test_server::{ClientFactory, assert_clean_system, login_root};
use std::str::FromStr;
use tokio::time::{Duration, sleep};

const TEST_MESSAGES_COUNT: u32 = 100;
const MESSAGES_TO_CONSUME: u32 = 50;
const CONSUMER_GROUP_1: &str = "test-consumer-group-1";
const CONSUMER_GROUP_2: &str = "test-consumer-group-2";
const CONSUMER_GROUP_3: &str = "test-consumer-group-3";

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;
    init_system(&client).await;
    execute_consumer_group_offset_cleanup_scenario(&client).await;
    cleanup(&client, false).await;
    assert_clean_system(&client).await;
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic with 1 partition
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

async fn execute_consumer_group_offset_cleanup_scenario(client: &IggyClient) {
    // Step 1: Produce 100 messages
    produce_messages(client).await;

    // Test with 3 different consumer groups
    let consumer_groups = [CONSUMER_GROUP_1, CONSUMER_GROUP_2, CONSUMER_GROUP_3];

    for consumer_group_name in consumer_groups {
        // Step 2: Create consumer group and consume messages
        test_consumer_group_lifecycle(client, consumer_group_name).await;
    }
}

async fn test_consumer_group_lifecycle(client: &IggyClient, consumer_group_name: &str) {
    // Create the consumer group
    let cg = client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            consumer_group_name,
        )
        .await
        .unwrap();
    assert_eq!(cg.name, consumer_group_name);
    // Create a consumer and consume some messages
    let mut consumer = create_consumer(client, consumer_group_name).await;

    // Consume 50 messages to establish an offset
    let consumed_messages = consume_messages(&mut consumer, MESSAGES_TO_CONSUME).await;
    assert_eq!(consumed_messages.len(), MESSAGES_TO_CONSUME as usize);

    // Wait a bit to ensure auto-commit has processed
    sleep(Duration::from_secs(2)).await;

    // Verify that offset exists before deletion
    let offset_before_delete = get_committed_offset(client, consumer_group_name).await;
    assert!(
        offset_before_delete.is_some(),
        "Offset should exist before deleting consumer group"
    );
    let offset_info = offset_before_delete.unwrap();
    assert_eq!(
        offset_info.stored_offset,
        (MESSAGES_TO_CONSUME - 1) as u64,
        "Committed offset should be at position {} (last consumed message offset)",
        MESSAGES_TO_CONSUME - 1
    );

    // Drop the consumer before deleting the group
    drop(consumer);

    // Step 3: Delete the consumer group
    client
        .delete_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(consumer_group_name).unwrap(),
        )
        .await
        .unwrap();

    // Step 4: Try to get offset for the deleted consumer group - should not exist
    let offset_after_delete = get_committed_offset(client, consumer_group_name).await;
    assert!(
        offset_after_delete.is_none(),
        "Offset should not exist after deleting consumer group: {}",
        consumer_group_name
    );
}

async fn produce_messages(client: &IggyClient) {
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

async fn create_consumer(client: &IggyClient, consumer_group_name: &str) -> IggyConsumer {
    let mut consumer = client
        .consumer_group(consumer_group_name, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(10)
        .poll_interval(IggyDuration::from_str("10ms").expect("Invalid duration"))
        .polling_strategy(PollingStrategy::next())
        .auto_join_consumer_group()
        .auto_commit(AutoCommit::IntervalOrAfter(
            IggyDuration::from_str("100ms").unwrap(),
            AutoCommitAfter::ConsumingEachMessage,
        ))
        .build();

    consumer.init().await.unwrap();
    consumer
}

async fn consume_messages(consumer: &mut IggyConsumer, count: u32) -> Vec<IggyMessage> {
    let mut consumed_messages = Vec::new();
    let mut consumed_count = 0;

    while consumed_count < count {
        if let Some(message_result) = consumer.next().await {
            match message_result {
                Ok(polled_message) => {
                    consumed_messages.push(polled_message.message);
                    consumed_count += 1;
                }
                Err(e) => panic!("Error while consuming messages: {}", e),
            }
        }
    }

    consumed_messages
}

async fn get_committed_offset(
    client: &IggyClient,
    consumer_group_name: &str,
) -> Option<ConsumerOffsetInfo> {
    let consumer = Consumer::group(Identifier::named(consumer_group_name).unwrap());
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
