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

use crate::server::scenarios::{PARTITION_ID, PARTITIONS_COUNT, create_client};
use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, assert_clean_system, login_root};
use std::str::FromStr;

const S1_NAME: &str = "test-stream-1";
const T1_NAME: &str = "test-topic-1";
const S2_NAME: &str = "test-stream-2";
const T2_NAME: &str = "test-topic-2";
const MESSAGE_PAYLOAD_SIZE_BYTES: u64 = 57;
const MSG_SIZE: u64 = IGGY_MESSAGE_HEADER_SIZE as u64 + MESSAGE_PAYLOAD_SIZE_BYTES; // number of bytes in a single message
const MSGS_COUNT: u64 = 117; // number of messages in a single topic after one pass of appending
const MSGS_SIZE: u64 = MSG_SIZE * MSGS_COUNT; // number of bytes in a single topic after one pass of appending

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;

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

    // 14. Delete first topic on the first stream
    delete_topic(&client, S1_NAME, T1_NAME).await;

    // 15. Validate both streams, first should have it's message count and size should be reduced by 50%, second stream should be unchanged
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

    // 21. Validate second stream and it's topics, should be empty
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
    login_root(client).await;

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

async fn validate_stream(
    client: &IggyClient,
    stream_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    // 1. Validate stream size and number of messages
    let stream = client
        .get_stream(&Identifier::from_str(stream_name).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");

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
    // 1. Validate topic size and number of messages
    let topic = client
        .get_topic(
            &Identifier::from_str(stream_name).unwrap(),
            &Identifier::from_str(topic_name).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

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
