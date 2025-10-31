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
    CONSUMER_GROUP_NAME, MESSAGES_COUNT, PARTITIONS_COUNT, STREAM_NAME, TOPIC_NAME, cleanup,
    create_client, get_consumer_group, join_consumer_group,
};
use iggy::prelude::*;
use integration::test_server::{
    ClientFactory, assert_clean_system, create_user, login_root, login_user,
};
use std::str::FromStr;

pub async fn run(client_factory: &dyn ClientFactory) {
    let system_client = create_client(client_factory).await;
    let client1 = create_client(client_factory).await;
    let client2 = create_client(client_factory).await;
    let client3 = create_client(client_factory).await;
    login_root(&system_client).await;
    init_system(&system_client, &client1, &client2, &client3, true).await;
    execute_using_messages_key_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, false).await;
    init_system(&system_client, &client1, &client2, &client3, false).await;
    execute_using_none_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, true).await;
    assert_clean_system(&system_client).await;
}

async fn init_system(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
    create_users: bool,
) {
    // 1. Create the stream
    system_client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic
    system_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    // 3. Create the consumer group
    system_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    if create_users {
        // Create the users for all clients
        create_user(system_client, "user1").await;
        create_user(system_client, "user2").await;
        create_user(system_client, "user3").await;

        // Login all the clients
        login_user(client1, "user1").await;
        login_user(client2, "user2").await;
        login_user(client3, "user3").await;
    }

    // 4. Join the consumer group by each client
    join_consumer_group(client1).await;
    join_consumer_group(client2).await;
    join_consumer_group(client3).await;

    // 5. Get the consumer group details
    let consumer_group_info = get_consumer_group(system_client).await;
    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }
}

async fn execute_using_messages_key_key(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
) {
    // 1. Send messages to the calculated partition ID on the server side by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT {
        let message = IggyMessage::from_str(&create_message_payload(entity_id)).unwrap();
        let mut messages = vec![message];
        system_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::messages_key_u32(entity_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 2. Poll the messages for each client per assigned partition in the consumer group
    let mut total_read_messages_count = 0;
    total_read_messages_count += poll_messages(client1).await;
    total_read_messages_count += poll_messages(client2).await;
    total_read_messages_count += poll_messages(client3).await;

    assert_eq!(total_read_messages_count, MESSAGES_COUNT);
}

async fn poll_messages(client: &IggyClient) -> u32 {
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut total_read_messages_count = 0;
    for _ in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
        let polled_messages = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .unwrap();

        total_read_messages_count += polled_messages.messages.len() as u32;
    }

    total_read_messages_count
}

fn create_message_payload(entity_id: u32) -> String {
    format!("message-{entity_id}")
}

async fn execute_using_none_key(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
) {
    // 1. Send messages to the calculated partition ID on the server side (round-robin) by using none key
    for entity_id in 1..=MESSAGES_COUNT * PARTITIONS_COUNT {
        let mut partition_id = entity_id % PARTITIONS_COUNT;
        if partition_id == 0 {
            partition_id = PARTITIONS_COUNT;
        }

        let message =
            IggyMessage::from_str(&create_extended_message_payload(partition_id, entity_id))
                .unwrap();
        let mut messages = vec![message];
        system_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::balanced(),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let consumer_group_info = get_consumer_group(system_client).await;
    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }

    // 2. Poll the messages for each client per assigned partition in the consumer group
    validate_message_polling(client1).await;
    validate_message_polling(client2).await;
    validate_message_polling(client3).await;
}

async fn validate_message_polling(client: &IggyClient) {
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    for i in 1..=MESSAGES_COUNT {
        let polled_messages = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .unwrap();
        assert_eq!(polled_messages.messages.len(), 1);
        let message = &polled_messages.messages[0];
        let offset = (i - 1) as u64;
        assert_eq!(message.header.offset, offset);
    }

    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            true,
        )
        .await
        .unwrap();
    assert!(polled_messages.messages.is_empty())
}

fn create_extended_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{partition_id}-{entity_id}")
}
