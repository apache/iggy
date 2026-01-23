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
use iggy::prelude::IggyMessage;
use iggy_binary_protocol::MessageClient;
use iggy_common::{Identifier, Partitioning};
use integration::harness::fixtures::{
    PostgresSinkByteaFixture, PostgresSinkFixture, PostgresSinkJsonFixture,
};
use integration::harness::seeds;
use integration::iggy_harness;
// IggyClient is used in the function signature for macro parameter detection
#[allow(unused_imports)]
use iggy::prelude::IggyClient;

const SINK_TABLE: &str = "iggy_messages";
const TEST_STREAM: &str = "test_stream";
const TEST_TOPIC: &str = "test_topic";
const TEST_MESSAGE_COUNT: usize = 3;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct TestMessage {
    pub id: u64,
    pub name: String,
    pub count: u32,
    pub amount: f64,
    pub active: bool,
    pub timestamp: i64,
}

fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = iggy_common::IggyTimestamp::now().as_micros();
    let one_day_micros: u64 = 24 * 60 * 60 * 1_000_000;
    (1..=count)
        .map(|i| TestMessage {
            id: i as u64,
            name: format!("user_{}", i - 1),
            count: ((i - 1) * 10) as u32,
            amount: (i - 1) as f64 * 99.99,
            active: (i - 1) % 2 == 0,
            timestamp: (base_timestamp + (i - 1) as u64 * one_day_micros) as i64,
        })
        .collect()
}

type SinkRow = (i64, String, String, Vec<u8>);
type SinkJsonRow = (i64, serde_json::Value);

#[iggy_harness(
    server(connector(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(client: &IggyClient, fixture: PostgresSinkFixture) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.wait_for_table(&pool, SINK_TABLE).await;

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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();

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
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {} ORDER BY iggy_offset",
        SINK_TABLE
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await;

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, stream, topic, payload)) in rows.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset mismatch at row {i}");
        assert_eq!(stream, TEST_STREAM, "Stream mismatch at row {i}");
        assert_eq!(topic, TEST_TOPIC, "Topic mismatch at row {i}");

        let stored: TestMessage =
            serde_json::from_slice(payload).expect("Failed to deserialize stored payload");
        assert_eq!(stored, messages_data[i], "Message data mismatch at row {i}");
    }
}

#[iggy_harness(
    server(connector(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn binary_messages_sink_stores_as_bytea(
    client: &IggyClient,
    fixture: PostgresSinkByteaFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.wait_for_table(&pool, SINK_TABLE).await;

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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();

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
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {} ORDER BY iggy_offset",
        SINK_TABLE
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await;

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
    server(connector(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_jsonb(client: &IggyClient, fixture: PostgresSinkJsonFixture) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.wait_for_table(&pool, SINK_TABLE).await;

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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();

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
        "SELECT iggy_offset, payload FROM {} ORDER BY iggy_offset",
        SINK_TABLE
    );
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await;

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
