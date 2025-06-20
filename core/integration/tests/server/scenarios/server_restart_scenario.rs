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

use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::ClientFactory;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const PARTITIONS_COUNT: u32 = 1;
const MESSAGE_SIZE: usize = 50_000;

pub async fn run(client_factory: &dyn ClientFactory, expected_messages_before_restart: u32) {
    if expected_messages_before_restart == 0 {
        run_initial_phase(client_factory).await;
    } else {
        run_after_restart_phase(client_factory, expected_messages_before_restart).await;
    }
}

async fn run_initial_phase(client_factory: &dyn ClientFactory) {
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };

    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    client
        .create_stream("test-stream", Some(STREAM_ID))
        .await
        .unwrap();

    let stream = client
        .get_stream(&Identifier::numeric(STREAM_ID).unwrap())
        .await
        .unwrap()
        .expect("Initial stream should exist");

    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, "test-stream");

    client
        .create_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            "test-topic",
            PARTITIONS_COUNT,
            CompressionAlgorithm::None,
            None,
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let messages_per_batch = 4;
    let mut total_messages = 0;

    for batch in 0..3 {
        let mut messages = create_messages(batch * messages_per_batch, messages_per_batch);
        client
            .send_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
        total_messages += messages_per_batch;
    }

    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            total_messages,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len() as u32, total_messages);
    verify_messages(&polled_messages.messages, 0, total_messages);
}

async fn run_after_restart_phase(
    client_factory: &dyn ClientFactory,
    expected_messages_before_restart: u32,
) {
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };

    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let stream = client
        .get_stream(&Identifier::numeric(STREAM_ID).unwrap())
        .await
        .unwrap()
        .expect("Stream should exist after restart");

    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, "test-stream");

    let topic = client
        .get_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
        )
        .await
        .unwrap()
        .expect("Topic should exist after restart");

    assert_eq!(topic.id, TOPIC_ID);
    assert_eq!(topic.name, "test-topic");
    assert_eq!(
        topic.messages_count,
        expected_messages_before_restart as u64
    );

    let messages_per_batch = 4;
    let second_phase_messages = messages_per_batch * 2;

    for batch in 0..2 {
        let start_id = expected_messages_before_restart + (batch * messages_per_batch);
        let mut messages = create_messages(start_id, messages_per_batch);
        client
            .send_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let total_messages = expected_messages_before_restart + second_phase_messages;

    // First, poll messages one-by-one to verify individual message access
    for i in 0..total_messages {
        let single_message = client
            .poll_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::offset(i as u64),
                1,
                false,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to poll message at offset {i}: {error}");
            });

        assert_eq!(single_message.messages.len(), 1);
        let msg = &single_message.messages[0];
        assert_eq!(msg.header.offset, i as u64);

        let payload_str = String::from_utf8_lossy(&msg.payload);
        assert!(payload_str.starts_with(&format!("Message {} -", i)));
    }

    // Then poll all messages at once
    let all_messages = client
        .poll_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            total_messages,
            false,
        )
        .await
        .unwrap();

    assert_eq!(all_messages.messages.len() as u32, total_messages);

    verify_messages(&all_messages.messages, 0, total_messages);

    let new_messages = client
        .poll_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(expected_messages_before_restart as u64),
            second_phase_messages,
            false,
        )
        .await
        .unwrap();

    assert_eq!(new_messages.messages.len() as u32, second_phase_messages);
    verify_messages(
        &new_messages.messages,
        expected_messages_before_restart,
        second_phase_messages,
    );

    let final_topic = client
        .get_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

    assert_eq!(final_topic.messages_count, total_messages as u64);
    println!(
        "Test completed: {} total messages across {} segments",
        total_messages, final_topic.partitions[0].segments_count
    );
}

fn create_messages(start_id: u32, count: u32) -> Vec<IggyMessage> {
    let mut messages = Vec::new();
    let large_payload = "x".repeat(MESSAGE_SIZE);

    for i in 0..count {
        let id = (start_id + i + 1) as u128;
        let payload = format!("Message {} - {}", start_id + i, large_payload);
        messages.push(
            IggyMessage::builder()
                .id(id)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to create message"),
        );
    }
    messages
}

fn verify_messages(messages: &[IggyMessage], start_offset: u32, _count: u32) {
    for (idx, message) in messages.iter().enumerate() {
        let expected_offset = start_offset + idx as u32;
        assert_eq!(message.header.offset, expected_offset as u64);

        let payload_str = String::from_utf8_lossy(&message.payload);
        assert!(payload_str.starts_with(&format!("Message {} -", expected_offset)));
        assert!(payload_str.len() > MESSAGE_SIZE);
    }
}
