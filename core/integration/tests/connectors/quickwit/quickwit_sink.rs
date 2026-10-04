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

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    QuickwitFixture, QuickwitOps, QuickwitPreCreatedFixture, QuickwitRawFixture,
    QuickwitTextFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use serde::{Deserialize, Serialize};

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
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
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
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
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_message_send_should_store(
    harness: &TestHarness,
    fixture: QuickwitPreCreatedFixture,
) {
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
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_raw_messages_should_store_objects_and_encoded_wrappers(
    harness: &TestHarness,
    fixture: QuickwitRawFixture,
) {
    let cases = [
        (
            Bytes::from_static(br#"{"message":"raw JSON object"}"#),
            serde_json::json!({"message": "raw JSON object"}),
        ),
        (
            Bytes::from_static(b"YWJj"),
            serde_json::json!({"data": "YWJj", "data_type": "raw", "data_encoding": "utf8"}),
        ),
        (
            Bytes::from_static(b"\xff\x00\x80"),
            serde_json::json!({"data": "/wCA", "data_type": "raw", "data_encoding": "base64"}),
        ),
        (
            Bytes::from_static(br#"{"message":"escaped\ntext",broken}"#),
            serde_json::json!({
                "data": r#"{"message":"escaped\ntext",broken}"#,
                "data_type": "raw",
                "data_encoding": "utf8"
            }),
        ),
        (
            Bytes::from_static(b"42"),
            serde_json::json!({"data": "42", "data_type": "raw", "data_encoding": "utf8"}),
        ),
        (
            Bytes::from_static(br#""text""#),
            serde_json::json!({"data": "\"text\"", "data_type": "raw", "data_encoding": "utf8"}),
        ),
        (
            Bytes::from_static(b"[1,2]"),
            serde_json::json!({"data": "[1,2]", "data_type": "raw", "data_encoding": "utf8"}),
        ),
        (
            Bytes::from_static(b"null"),
            serde_json::json!({"data": "null", "data_type": "raw", "data_encoding": "utf8"}),
        ),
    ];

    assert_documents_stored(harness, &fixture, &cases).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/quickwit/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_text_messages_should_store_text_wrappers(
    harness: &TestHarness,
    fixture: QuickwitTextFixture,
) {
    let cases = [
        (
            Bytes::from_static(b"log entry\n\"ready\""),
            serde_json::json!({"text": "log entry\n\"ready\"", "data_type": "text"}),
        ),
        (
            Bytes::from_static(br#"{"message":"text containing JSON"}"#),
            serde_json::json!({
                "text": r#"{"message":"text containing JSON"}"#,
                "data_type": "text"
            }),
        ),
    ];

    assert_documents_stored(harness, &fixture, &cases).await;
}

async fn assert_documents_stored(
    harness: &TestHarness,
    fixture: &QuickwitFixture,
    cases: &[(Bytes, serde_json::Value)],
) {
    let client = harness.root_client().await.expect("create Iggy client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("stream identifier");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic identifier");
    let mut messages: Vec<IggyMessage> = cases
        .iter()
        .enumerate()
        .map(|(message_index, (payload, _))| {
            IggyMessage::builder()
                .id(message_index as u128 + 1)
                .payload(payload.clone())
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
        .wait_for_documents(seeds::names::TOPIC, cases.len())
        .await
        .expect("wait for indexed documents");

    assert_eq!(search.num_hits, cases.len());
    assert_eq!(search.hits.len(), cases.len());
    for (_, expected) in cases {
        assert!(
            search.hits.contains(expected),
            "Missing document {expected}; search hits: {:?}",
            search.hits
        );
    }
}
