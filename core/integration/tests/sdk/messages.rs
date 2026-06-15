// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! VSR data-plane round trip: `send_messages` / `poll_messages` and the
//! consumer-offset store/get pair against a 3-node cluster, exercising the
//! partition-reconciliation loop end to end.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use integration::iggy_harness;

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;
const MESSAGES_COUNT: u32 = 10;

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn send_and_poll_messages_round_trip(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    client.create_stream("data-stream").await.unwrap();
    client
        .create_topic(
            &Identifier::named("data-stream").unwrap(),
            "data-topic",
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut messages: Vec<IggyMessage> = (0..MESSAGES_COUNT)
        .map(|i| {
            IggyMessage::builder()
                .id(u128::from(i + 1))
                .payload(format!("payload-{i}").into())
                .build()
                .expect("message build")
        })
        .collect();

    client
        .send_messages(
            &Identifier::named("data-stream").unwrap(),
            &Identifier::named("data-topic").unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send_messages");

    let consumer = Consumer::default();
    let polled = client
        .poll_messages(
            &Identifier::named("data-stream").unwrap(),
            &Identifier::named("data-topic").unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .expect("poll_messages");

    assert_eq!(
        polled.messages.len() as u32,
        MESSAGES_COUNT,
        "all sent messages must come back"
    );
    for (i, message) in polled.messages.iter().enumerate() {
        assert_eq!(message.header.offset, i as u64, "offsets must be dense");
        assert_eq!(
            message.payload,
            bytes::Bytes::from(format!("payload-{i}")),
            "payload round trip for message {i}"
        );
    }

    client.logout_user().await.unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn consumer_offset_store_get_round_trip(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    client.create_stream("offset-stream").await.unwrap();
    client
        .create_topic(
            &Identifier::named("offset-stream").unwrap(),
            "offset-topic",
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut messages: Vec<IggyMessage> = (0..5u32)
        .map(|i| {
            IggyMessage::builder()
                .id(u128::from(i + 1))
                .payload(format!("offset-payload-{i}").into())
                .build()
                .expect("message build")
        })
        .collect();
    client
        .send_messages(
            &Identifier::named("offset-stream").unwrap(),
            &Identifier::named("offset-topic").unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send_messages");

    let consumer = Consumer::default();
    client
        .store_consumer_offset(
            &consumer,
            &Identifier::named("offset-stream").unwrap(),
            &Identifier::named("offset-topic").unwrap(),
            Some(PARTITION_ID),
            3,
        )
        .await
        .expect("store_consumer_offset");

    let stored = client
        .get_consumer_offset(
            &consumer,
            &Identifier::named("offset-stream").unwrap(),
            &Identifier::named("offset-topic").unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .expect("get_consumer_offset")
        .expect("offset must exist after store");

    assert_eq!(stored.stored_offset, 3, "stored offset must round trip");
    assert_eq!(stored.partition_id, PARTITION_ID);

    client.logout_user().await.unwrap();
}
