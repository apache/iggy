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

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{QuickwitFixture, QuickwitOps, QuickwitPreCreatedFixture};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;
use serde::{Deserialize, Serialize};

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_existent_quickwit_index_should_store(
    harness: &TestHarness,
    fixture: QuickwitPreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 11;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let search = fixture
        .wait_for_documents(seeds::names::TOPIC, message_count)
        .await
        .expect("search");

    assert_eq!(search.num_hits, message_count);
    for (hit, payload) in search.hits.iter().zip(payloads.iter()) {
        assert_eq!(
            hit,
            &serde_json::from_slice::<serde_json::Value>(payload).unwrap()
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_nonexistent_quickwit_index_should_create_and_store(
    harness: &TestHarness,
    fixture: QuickwitFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 13;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let search = fixture
        .wait_for_documents(seeds::names::TOPIC, message_count)
        .await
        .expect("search");

    assert_eq!(search.num_hits, message_count);
    for (hit, payload) in search.hits.iter().zip(payloads.iter()) {
        assert_eq!(
            hit,
            &serde_json::from_slice::<serde_json::Value>(payload).unwrap()
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_message_send_should_store(harness: &TestHarness, fixture: QuickwitFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 1000;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let search = fixture
        .wait_for_documents(seeds::names::TOPIC, message_count)
        .await
        .expect("search");

    assert_eq!(search.num_hits, message_count);
    for (hit, payload) in search.hits.iter().zip(payloads.iter()) {
        assert_eq!(
            hit,
            &serde_json::from_slice::<serde_json::Value>(payload).unwrap()
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_invalid_messages_should_not_store(harness: &TestHarness, fixture: QuickwitFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let first_valid =
        Bytes::from(serde_json::to_vec(&create_test_messages(1)[0]).expect("serialize"));
    let second_valid =
        Bytes::from(serde_json::to_vec(&create_test_messages(1)[0]).expect("serialize"));

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct NotTestMessage {
        not_a_test_message_field: f64,
    }
    let first_invalid = Bytes::from(
        serde_json::to_vec(&NotTestMessage {
            not_a_test_message_field: 17.,
        })
        .expect("serialize"),
    );

    for (message_index, message_payload) in
        [first_valid.clone(), first_invalid, second_valid.clone()]
            .into_iter()
            .enumerate()
    {
        let mut messages = vec![
            IggyMessage::builder()
                .id(message_index as u128 + 1)
                .payload(message_payload)
                .build()
                .expect("build message"),
        ];

        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("send messages");
    }

    let search = fixture
        .wait_for_documents(seeds::names::TOPIC, 2)
        .await
        .expect("search");

    assert_eq!(search.num_hits, 2);
    let expected_payloads = [first_valid, second_valid];
    for (hit, payload) in search.hits.iter().zip(expected_payloads.iter()) {
        assert_eq!(
            hit,
            &serde_json::from_slice::<serde_json::Value>(payload).unwrap()
        );
    }
}
