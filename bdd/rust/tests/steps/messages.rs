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

use crate::common::global_context::GlobalContext;
use crate::helpers::test_data::create_test_messages;
use bytes::Bytes;
use cucumber::{then, when};
use iggy::prelude::{
    Consumer, ConsumerKind, Identifier, IggyMessage, MessageClient, Partitioning, PollingStrategy,
};

#[when(regex = r"^I send (\d+) messages to stream (\d+), topic (\d+), partition (\d+)$")]
pub async fn when_send_messages(
    world: &mut GlobalContext,
    message_count: u32,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let mut messages = create_test_messages(message_count);

    if let Some(last_message) = messages.last() {
        let sent_msg = IggyMessage::builder()
            .id(last_message.header.id)
            .payload(last_message.payload.clone())
            .build()
            .expect("Should be able to create message");
        world.last_sent_message = Some(sent_msg);
    }

    client
        .send_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut messages,
        )
        .await
        .expect("Should be able to send messages");
}

#[when(
    regex = r"^I poll messages from stream (\d+), topic (\d+), partition (\d+) starting from offset (\d+)$"
)]
pub async fn when_poll_messages(
    world: &mut GlobalContext,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    start_offset: u64,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let consumer = Consumer {
        kind: ConsumerKind::Consumer,
        id: Identifier::numeric(1).unwrap(),
    };

    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            Some(partition_id),
            &consumer,
            &PollingStrategy::offset(start_offset),
            100,
            false,
        )
        .await
        .expect("Should be able to poll messages");

    world.last_polled_messages = Some(polled_messages);
}

#[then("all messages should be sent successfully")]
pub async fn then_messages_sent_successfully(_world: &mut GlobalContext) {}

#[then(regex = r"^I should receive (\d+) messages$")]
pub async fn then_should_receive_messages(world: &mut GlobalContext, expected_count: u32) {
    let polled_messages = world
        .last_polled_messages
        .as_ref()
        .expect("Should have polled messages");
    assert_eq!(
        polled_messages.messages.len() as u32,
        expected_count,
        "Should receive exactly {} messages",
        expected_count
    );
}

#[then(regex = r"^the messages should have sequential offsets from (\d+) to (\d+)$")]
pub async fn then_messages_have_sequential_offsets(
    world: &mut GlobalContext,
    start_offset: u64,
    end_offset: u64,
) {
    let polled_messages = world
        .last_polled_messages
        .as_ref()
        .expect("Should have polled messages");

    for (index, message) in polled_messages.messages.iter().enumerate() {
        let expected_offset = start_offset + index as u64;
        assert_eq!(
            message.header.offset, expected_offset,
            "Message at index {} should have offset {}",
            index, expected_offset
        );
    }

    let last_message = polled_messages.messages.last().unwrap();
    assert_eq!(
        last_message.header.offset, end_offset,
        "Last message should have offset {}",
        end_offset
    );
}

#[then("each message should have the expected payload content")]
pub async fn then_messages_have_expected_payload(world: &mut GlobalContext) {
    let polled_messages = world
        .last_polled_messages
        .as_ref()
        .expect("Should have polled messages");

    for (index, message) in polled_messages.messages.iter().enumerate() {
        let expected_payload = format!("test message {}", index);
        assert_eq!(
            message.payload,
            Bytes::from(expected_payload.clone()),
            "Message at offset {} should have payload '{}'",
            index,
            expected_payload
        );
    }
}

#[then("the last polled message should match the last sent message")]
pub async fn then_last_polled_message_matches_sent(world: &mut GlobalContext) {
    let polled_messages = world
        .last_polled_messages
        .as_ref()
        .expect("Should have polled messages");

    let sent_message = world
        .last_sent_message
        .as_ref()
        .expect("Should have a sent message to compare");

    assert!(
        !polled_messages.messages.is_empty(),
        "Should have at least one polled message"
    );

    let last_polled = polled_messages.messages.last().unwrap();

    assert_eq!(
        last_polled.header.id, sent_message.header.id,
        "Message IDs should match: sent {} vs polled {}",
        sent_message.header.id, last_polled.header.id
    );

    assert_eq!(
        last_polled.payload, sent_message.payload,
        "Message payloads should match"
    );
}
