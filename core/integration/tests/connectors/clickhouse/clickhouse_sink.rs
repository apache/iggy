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
    ClickHouseOps, ClickHouseSinkJsonFieldMappingsFixture, ClickHouseSinkJsonFixture,
    ClickHouseSinkJsonWithMetadataFixture, ClickHouseSinkRowBinaryFixture,
    ClickHouseSinkRowBinaryWithMetadataFixture,
};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use clickhouse::Row;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;
use serde::Deserialize;

// ── Row types for querying ClickHouse ────────────────────────────────────────

#[derive(Debug, Row, Deserialize)]
struct PayloadOnlyRow {
    id: u64,
    name: String,
    count: u32,
    amount: f64,
    active: bool,
    timestamp: i64,
}

#[allow(dead_code)]
#[derive(Debug, Row, Deserialize)]
struct MetadataRow {
    id: u64,
    name: String,
    count: u32,
    amount: f64,
    active: bool,
    timestamp: i64,
    iggy_stream: String,
    iggy_topic: String,
    iggy_partition_id: u32,
    iggy_id: String,
    iggy_offset: u64,
    iggy_checksum: u64,
    iggy_timestamp: u64,
    iggy_origin_timestamp: u64,
}

#[derive(Debug, Row, Deserialize)]
struct FieldMappedRow {
    msg_id: u64,
    msg_name: String,
    msg_amount: f64,
}

#[derive(Debug, Row, Deserialize)]
struct RowBinaryPayloadRow {
    payload: String,
}

#[allow(dead_code)]
#[derive(Debug, Row, Deserialize)]
struct RowBinaryMetadataRow {
    iggy_stream: String,
    iggy_topic: String,
    iggy_partition_id: u32,
    iggy_id: String,
    iggy_offset: u64,
    iggy_checksum: u64,
    iggy_timestamp: u64,
    iggy_origin_timestamp: u64,
    payload: String,
}

// ── Helper: build and send test messages ─────────────────────────────────────

async fn send_test_messages(
    client: &impl MessageClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    messages_data: &[TestMessage],
) {
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
            stream_id,
            topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");
}

// ── Test 1: JSON insert, no metadata ─────────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_insert_stores_test_messages(
    harness: &TestHarness,
    fixture: ClickHouseSinkJsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let ch_client = fixture.create_client();

    fixture.wait_for_table(&ch_client, "iggy_sink").await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    send_test_messages(&client, &stream_id, &topic_id, &messages_data).await;

    let query = "SELECT id, name, count, amount, active, timestamp FROM iggy_sink ORDER BY id";
    let rows: Vec<PayloadOnlyRow> = fixture
        .fetch_rows(&ch_client, query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.id, messages_data[i].id, "id mismatch at row {i}");
        assert_eq!(row.name, messages_data[i].name, "name mismatch at row {i}");
        assert_eq!(
            row.count, messages_data[i].count,
            "count mismatch at row {i}"
        );
        assert!(
            (row.amount - messages_data[i].amount).abs() < f64::EPSILON,
            "amount mismatch at row {i}"
        );
        assert_eq!(
            row.active, messages_data[i].active,
            "active mismatch at row {i}"
        );
        assert_eq!(
            row.timestamp, messages_data[i].timestamp,
            "timestamp mismatch at row {i}"
        );
    }
}

// ── Test 2: JSON insert, with metadata ───────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_insert_with_metadata_stores_test_messages(
    harness: &TestHarness,
    fixture: ClickHouseSinkJsonWithMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let ch_client = fixture.create_client();

    fixture.wait_for_table(&ch_client, "iggy_sink_meta").await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    send_test_messages(&client, &stream_id, &topic_id, &messages_data).await;

    let query = "SELECT id, name, count, amount, active, timestamp, \
                 iggy_stream, iggy_topic, iggy_partition_id, iggy_id, \
                 iggy_offset, iggy_checksum, iggy_timestamp, iggy_origin_timestamp \
                 FROM iggy_sink_meta ORDER BY iggy_offset";
    let rows: Vec<MetadataRow> = fixture
        .fetch_rows(&ch_client, query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.id, messages_data[i].id, "id mismatch at row {i}");
        assert_eq!(row.name, messages_data[i].name, "name mismatch at row {i}");
        assert_eq!(
            row.iggy_stream,
            seeds::names::STREAM,
            "stream mismatch at row {i}"
        );
        assert_eq!(
            row.iggy_topic,
            seeds::names::TOPIC,
            "topic mismatch at row {i}"
        );
        assert_eq!(row.iggy_offset, i as u64, "offset mismatch at row {i}");
    }
}

// ── Test 3: JSON insert, field mappings ──────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink_field_mappings.toml")),
    seed = seeds::connector_stream
)]
async fn json_insert_with_field_mappings_stores_mapped_fields(
    harness: &TestHarness,
    fixture: ClickHouseSinkJsonFieldMappingsFixture,
) {
    let client = harness.root_client().await.unwrap();
    let ch_client = fixture.create_client();

    fixture.wait_for_table(&ch_client, "iggy_sink_mapped").await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    send_test_messages(&client, &stream_id, &topic_id, &messages_data).await;

    let query = "SELECT msg_id, msg_name, msg_amount FROM iggy_sink_mapped ORDER BY msg_id";
    let rows: Vec<FieldMappedRow> = fixture
        .fetch_rows(&ch_client, query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.msg_id, messages_data[i].id,
            "msg_id mismatch at row {i}"
        );
        assert_eq!(
            row.msg_name, messages_data[i].name,
            "msg_name mismatch at row {i}"
        );
        assert!(
            (row.msg_amount - messages_data[i].amount).abs() < f64::EPSILON,
            "msg_amount mismatch at row {i}"
        );
    }
}

// ── Test 4: RowBinary insert, no metadata ────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn rowbinary_insert_stores_payload_as_string(
    harness: &TestHarness,
    fixture: ClickHouseSinkRowBinaryFixture,
) {
    let client = harness.root_client().await.unwrap();
    let ch_client = fixture.create_client();

    fixture.wait_for_table(&ch_client, "iggy_sink_rb").await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    send_test_messages(&client, &stream_id, &topic_id, &messages_data).await;

    let query = "SELECT payload FROM iggy_sink_rb";
    let rows: Vec<RowBinaryPayloadRow> = fixture
        .fetch_rows(&ch_client, query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);

    let mut stored: Vec<TestMessage> = rows
        .iter()
        .map(|r| serde_json::from_str(&r.payload).expect("Failed to deserialize payload"))
        .collect();
    stored.sort_by_key(|m| m.id);

    for (i, msg) in stored.iter().enumerate() {
        assert_eq!(msg, &messages_data[i], "message data mismatch at row {i}");
    }
}

// ── Test 5: RowBinary insert, with metadata ──────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn rowbinary_insert_with_metadata_stores_test_messages(
    harness: &TestHarness,
    fixture: ClickHouseSinkRowBinaryWithMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let ch_client = fixture.create_client();

    fixture
        .wait_for_table(&ch_client, "iggy_sink_rb_meta")
        .await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    send_test_messages(&client, &stream_id, &topic_id, &messages_data).await;

    let query = "SELECT iggy_stream, iggy_topic, iggy_partition_id, iggy_id, \
                 iggy_offset, iggy_checksum, iggy_timestamp, iggy_origin_timestamp, \
                 payload \
                 FROM iggy_sink_rb_meta ORDER BY iggy_offset";
    let rows: Vec<RowBinaryMetadataRow> = fixture
        .fetch_rows(&ch_client, query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.iggy_stream,
            seeds::names::STREAM,
            "stream mismatch at row {i}"
        );
        assert_eq!(
            row.iggy_topic,
            seeds::names::TOPIC,
            "topic mismatch at row {i}"
        );
        assert_eq!(row.iggy_offset, i as u64, "offset mismatch at row {i}");

        let stored: TestMessage =
            serde_json::from_str(&row.payload).expect("Failed to deserialize payload");
        assert_eq!(stored, messages_data[i], "message data mismatch at row {i}");
    }
}
