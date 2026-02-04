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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    PostgresOps, PostgresSinkByteaFixture, PostgresSinkFixture, PostgresSinkJsonFixture,
};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;

const SINK_TABLE: &str = "iggy_messages";

type SinkRow = (i64, String, String, Vec<u8>);
type SinkJsonRow = (i64, serde_json::Value);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(harness: &TestHarness, fixture: PostgresSinkFixture) {
    let client = harness.client();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    fixture.wait_for_table(&pool, SINK_TABLE).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {SINK_TABLE} ORDER BY iggy_offset"
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, stream, topic, payload)) in rows.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset mismatch at row {i}");
        assert_eq!(stream, seeds::names::STREAM, "Stream mismatch at row {i}");
        assert_eq!(topic, seeds::names::TOPIC, "Topic mismatch at row {i}");

        let stored: TestMessage =
            serde_json::from_slice(payload).expect("Failed to deserialize stored payload");
        assert_eq!(stored, messages_data[i], "Message data mismatch at row {i}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn binary_messages_sink_stores_as_bytea(
    harness: &TestHarness,
    fixture: PostgresSinkByteaFixture,
) {
    let client = harness.client();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    fixture.wait_for_table(&pool, SINK_TABLE).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let raw_payloads: Vec<Vec<u8>> = vec![
        b"plain text message".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD],
        vec![0xDE, 0xAD, 0xBE, 0xEF],
    ];

    let mut messages: Vec<IggyMessage> = raw_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload.clone()))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {SINK_TABLE} ORDER BY iggy_offset"
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, _, _, payload)) in rows.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset mismatch at row {i}");
        assert_eq!(payload, &raw_payloads[i], "Payload mismatch at row {i}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_jsonb(
    harness: &TestHarness,
    fixture: PostgresSinkJsonFixture,
) {
    let client = harness.client();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    fixture.wait_for_table(&pool, SINK_TABLE).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "age": 30}),
        serde_json::json!({"items": [1, 2, 3], "active": true}),
        serde_json::json!({"nested": {"key": "value"}, "count": 42}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize json");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(bytes))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    let query = format!("SELECT iggy_offset, payload FROM {SINK_TABLE} ORDER BY iggy_offset");
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, payload)) in rows.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset mismatch at row {i}");
        assert_eq!(
            payload, &json_payloads[i],
            "JSON payload mismatch at row {i}"
        );
    }
}
