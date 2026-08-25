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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    RedshiftSinkFixture, RedshiftSinkJsonFixture, RedshiftSinkNoArchiveFixture,
    RedshiftSinkVarbyteFixture,
};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::SinkInfoResponse;
use integration::harness::seeds;
use integration::iggy_harness;

use reqwest::Client;

const SINK_TABLE: &str = "iggy_messages";
const API_KEY: &str = "test-api-key";
const REDSHIFT_SINK_KEY: &str = "redshift";

type SinkRow = (String, String, String, String, String);
type SinkRawRow = (String, String, String, String, String);
type SinkJsonRow = (String, String, String);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn redshift_sink_initializes_and_runs(harness: &TestHarness, fixture: RedshiftSinkFixture) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();

    let http_client = Client::new();

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].key, REDSHIFT_SINK_KEY);
    assert!(sinks[0].enabled);

    drop(fixture);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_text(
    harness: &TestHarness,
    fixture: RedshiftSinkJsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture
        .target_pool()
        .await
        .expect("Failed to create target postgres pool");

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
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset"
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

    for (i, (offset, stream, topic, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*offset, i.to_string(), "Offset mismatch at row {i}");
        assert_eq!(stream, seeds::names::STREAM, "Stream mismatch at row {i}");
        assert_eq!(topic, seeds::names::TOPIC, "Topic mismatch at row {i}");

        let stored: TestMessage =
            serde_json::from_str(payload).expect("Failed to deserialize stored payload");
        assert_eq!(stored, messages_data[i], "Message data mismatch at row {i}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn binary_messages_sink_stores_as_bytea(
    harness: &TestHarness,
    fixture: RedshiftSinkVarbyteFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.target_pool().await.expect("Failed to create pool");

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

    // For live redshift
    // "SELECT iggy_offset, iggy_stream, iggy_topic, FROM_VARBYTE(payload, 'hex') AS payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset"
    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, ENCODE(payload::BYTEA, 'hex') AS payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset"
    );
    let rows: Vec<SinkRawRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, _, _, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*offset, i.to_string(), "Offset mismatch at row {i}");
        assert_eq!(
            &hex::decode(payload).expect("Failed to decode payload"),
            &raw_payloads[i],
            "Payload mismatch at row {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_json(
    harness: &TestHarness,
    fixture: RedshiftSinkJsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.target_pool().await.expect("Failed to create pool");

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

    let query =
        format!("SELECT iggy_offset, payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset");
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*offset, i.to_string(), "Offset mismatch at row {i}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(payload)
                .expect("Failed to parse JSON string"),
            json_payloads[i],
            "JSON payload mismatch at row {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(
    harness: &TestHarness,
    fixture: RedshiftSinkVarbyteFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.target_pool().await.expect("Failed to create pool");

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

    // For live redshift
    // "SELECT iggy_offset, iggy_stream, iggy_topic, FROM_VARBYTE(payload, 'hex') AS payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset"
    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, ENCODE(payload::BYTEA, 'hex') AS payload, created_at FROM \"{SINK_TABLE}\" ORDER BY iggy_offset"
    );
    tracing::info!("Query: {}", query);

    let rows: Vec<SinkRawRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, _, _, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*offset, i.to_string(), "Offset mismatch at row {i}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &hex::decode(payload).expect("Failed to decode payload")
            )
            .expect("Failed to parse bytes"),
            json_payloads[i],
            "Payload mismatch at row {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redshift/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_with_no_archive_deletes_s3_artefact(
    harness: &TestHarness,
    fixture: RedshiftSinkNoArchiveFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.target_pool().await.expect("Failed to create pool");

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

    let query =
        format!("SELECT iggy_offset, payload, created_at FROM {SINK_TABLE} ORDER BY iggy_offset");
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in PostgreSQL table"
    );

    for (i, (offset, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*offset, i.to_string(), "Offset mismatch at row {i}");
        assert_eq!(
            payload,
            &json_payloads[i].to_string(),
            "JSON payload mismatch at row {i}"
        );

        assert!(
            fixture
                .confirm_empty_bucket()
                .await
                .expect("Failed to read empty bucket")
        );
    }
}
