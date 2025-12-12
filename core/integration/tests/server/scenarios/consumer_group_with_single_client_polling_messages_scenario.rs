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
use integration::test_server::{ClientFactory, assert_clean_system, login_root};
use std::str::{FromStr, from_utf8};

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    login_root(&client).await;
    init_system(&client).await;
    execute_using_messages_key_key(&client).await;
    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
    init_system(&client).await;
    execute_using_none_key(&client).await;
    cleanup(&client, false).await;
    assert_clean_system(&client).await;
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic
    client
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
    client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // 4. Join the consumer group by client
    join_consumer_group(client).await;

    // 5. Validate that group contains the single client with all partitions assigned
    let consumer_group_info = get_consumer_group(client).await;

    let _ = client.get_me().await.unwrap();

    assert_eq!(consumer_group_info.members_count, 1);
    assert_eq!(consumer_group_info.members.len(), 1);
    let member = &consumer_group_info.members[0];
    assert_eq!(member.partitions.len() as u32, PARTITIONS_COUNT);
    assert_eq!(member.partitions_count, PARTITIONS_COUNT);
}

async fn execute_using_messages_key_key(client: &IggyClient) {
    // 1. Send messages to the calculated partition ID on the server side by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT {
        let message = IggyMessage::from_str(&create_message_payload(entity_id)).unwrap();
        let mut messages = vec![message];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::messages_key_u32(entity_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 2. Poll the messages for the single client which has assigned all partitions in the consumer group
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

    assert_eq!(total_read_messages_count, MESSAGES_COUNT);
}

fn create_message_payload(entity_id: u32) -> String {
    format!("message-{entity_id}")
}

async fn execute_using_none_key(client: &IggyClient) {
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
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::balanced(),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 2. Poll the messages for the single client which has assigned all partitions in the consumer group
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut partition_id = 1;
    let mut offset = 0;
    let mut entity_id = 1;
    for i in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
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

        assert_eq!(
            polled_messages.messages.len(),
            1,
            "polled messages count is not 1 at iteration {i}"
        );
        let message = &polled_messages.messages[0];
        assert_eq!(message.header.offset, offset);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(
            payload,
            &create_extended_message_payload(partition_id, entity_id)
        );
        partition_id += 1;
        entity_id += 1;
        if partition_id > PARTITIONS_COUNT {
            partition_id = 1;
            offset += 1;
        }
    }

    for _ in 1..=PARTITIONS_COUNT {
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
        assert!(polled_messages.messages.is_empty());
    }
}

fn create_extended_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{partition_id}-{entity_id}")
}
