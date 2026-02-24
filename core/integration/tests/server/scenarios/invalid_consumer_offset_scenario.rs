/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use bytes::Bytes;
use iggy::prelude::*;
use iggy_common::IggyError;
use integration::harness::{TestHarness, assert_clean_system};

const STREAM_NAME: &str = "test-stream-offsets";
const TOPIC_NAME: &str = "test-topic-offsets";
const PARTITIONS_COUNT: u32 = 1;
const PARTITION_ID: u32 = 0;
const CONSUMER_NAME: &str = "test-consumer";
const CONSUMER_GROUP_NAME: &str = "test-consumer-group-offsets";
const MESSAGES_COUNT: u32 = 5;

fn assert_invalid_offset_error(err: &IggyError, expected_offset: u64) {
    let expected_code = IggyError::InvalidOffset(expected_offset).as_code();

    // HTTP client currently doesn't deserialize the ErrorResponse body into specific IggyError
    // variants for 400 Bad Request. Until it does, we must manually check the JSON body for the expected error ID.
    let is_match = err.as_code() == expected_code
        || matches!(
            err,
            IggyError::HttpResponseError(400, reason)
            if reason.contains(&format!("\"id\":{}", expected_code))
        );

    assert!(
        is_match,
        "Expected error code {}, got {:?}",
        expected_code, err
    );
}

pub async fn run(harness: &TestHarness) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");

    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();

    let joined_group = initialize(&client, &stream, &topic).await;

    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::named(CONSUMER_NAME).unwrap(),
    };
    let consumer_group = Consumer {
        kind: ConsumerKind::ConsumerGroup,
        id: Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
    };

    // 1. Empty partition scenarios
    test_offset_for_empty_partition(&client, &consumer, &stream, &topic).await;
    if joined_group {
        test_offset_for_empty_partition(&client, &consumer_group, &stream, &topic).await;
    }

    // 2. Send messages to create non-empty partition scenarios
    send_messages(&client, &stream, &topic).await;

    // 3. Non-empty partition scenarios
    test_offset_for_non_empty_partition(&client, &consumer, &stream, &topic).await;
    if joined_group {
        test_offset_for_non_empty_partition(&client, &consumer_group, &stream, &topic).await;
    }

    cleanup(&client, &stream, &topic, joined_group).await;
}

async fn initialize(client: &IggyClient, stream: &Identifier, topic: &Identifier) -> bool {
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            stream,
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
        .await
        .unwrap();

    client
        .create_consumer_group(stream, topic, CONSUMER_GROUP_NAME)
        .await
        .unwrap();

    let join_result = client
        .join_consumer_group(
            stream,
            topic,
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await;
    match join_result {
        Ok(_) => true,
        Err(e) => {
            assert_eq!(e.as_code(), IggyError::FeatureUnavailable.as_code());
            false
        }
    }
}

async fn cleanup(client: &IggyClient, stream: &Identifier, topic: &Identifier, joined_group: bool) {
    if joined_group {
        client
            .leave_consumer_group(
                stream,
                topic,
                &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
            )
            .await
            .unwrap();
    }

    client
        .delete_consumer_group(
            stream,
            topic,
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    client.delete_stream(stream).await.unwrap();
    assert_clean_system(client).await;
}

async fn test_offset_for_empty_partition(
    client: &IggyClient,
    consumer: &Consumer,
    stream: &Identifier,
    topic: &Identifier,
) {
    // Attempt to store offset 0 on a newly created, empty partition.
    // This operation should fail to prevent prematurely advancing the offset.
    let err = client
        .store_consumer_offset(consumer, stream, topic, Some(PARTITION_ID), 0)
        .await
        .expect_err("Storing offset 0 on empty partition should fail");
    assert_invalid_offset_error(&err, 0);

    // Attempt to store current_offset + 1 (should fail with specific error)
    let err = client
        .store_consumer_offset(consumer, stream, topic, Some(PARTITION_ID), 1)
        .await
        .expect_err("Storing offset 1 on empty partition should fail");
    assert_invalid_offset_error(&err, 1);
}

async fn test_offset_for_non_empty_partition(
    client: &IggyClient,
    consumer: &Consumer,
    stream: &Identifier,
    topic: &Identifier,
) {
    // Attempt to store offset less than current_offset (should succeed)
    // For 5 messages, offsets are 0, 1, 2, 3, 4, so current_offset is 4.
    client
        .store_consumer_offset(consumer, stream, topic, Some(PARTITION_ID), 2)
        .await
        .unwrap();

    let offset_info = client
        .get_consumer_offset(consumer, stream, topic, Some(PARTITION_ID))
        .await
        .unwrap()
        .expect("Failed to get offset");
    assert_eq!(offset_info.stored_offset, 2);

    // Attempt to store exactly current_offset (should succeed)
    let max_offset = (MESSAGES_COUNT - 1) as u64;
    client
        .store_consumer_offset(consumer, stream, topic, Some(PARTITION_ID), max_offset)
        .await
        .unwrap();

    let offset_info = client
        .get_consumer_offset(consumer, stream, topic, Some(PARTITION_ID))
        .await
        .unwrap()
        .expect("Failed to get offset");
    assert_eq!(offset_info.stored_offset, max_offset);

    // Attempt to store current_offset + 1 (should fail)
    let invalid_offset = max_offset + 1;
    let err = client
        .store_consumer_offset(consumer, stream, topic, Some(PARTITION_ID), invalid_offset)
        .await
        .expect_err("Storing offset max_offset + 1 should fail");
    assert_invalid_offset_error(&err, invalid_offset);
}

async fn send_messages(client: &IggyClient, stream: &Identifier, topic: &Identifier) {
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        messages.push(
            IggyMessage::builder()
                .id((offset + 1) as u128)
                .payload(Bytes::from(format!("message {}", offset)))
                .build()
                .expect("Failed to create message"),
        );
    }
    client
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();
}
