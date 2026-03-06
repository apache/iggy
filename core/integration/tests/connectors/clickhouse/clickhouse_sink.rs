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
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    ClickHouseSinkFixture, ClickHouseSinkRowBinaryFixture, ClickHouseSinkStringFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;

// ── json_each_row: basic ──────────────────────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn clickhouse_sink_stores_json_messages(
    harness: &TestHarness,
    fixture: ClickHouseSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

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

    fixture
        .wait_for_rows(TEST_MESSAGE_COUNT)
        .await
        .expect("Timed out waiting for rows in ClickHouse");

    let rows = fixture
        .fetch_rows()
        .await
        .expect("Failed to fetch rows from ClickHouse");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in ClickHouse"
    );

    for (i, row) in rows.iter().enumerate() {
        let expected = &messages_data[i];

        let id = row["id"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| row["id"].as_u64())
            .unwrap_or_else(|| panic!("Missing 'id' at row {i}"));
        assert_eq!(id, expected.id, "id mismatch at row {i}");

        let name = row["name"]
            .as_str()
            .unwrap_or_else(|| panic!("Missing 'name' at row {i}"));
        assert_eq!(name, expected.name, "name mismatch at row {i}");

        let amount = row["amount"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| row["amount"].as_f64())
            .unwrap_or_else(|| panic!("Missing 'amount' at row {i}"));
        assert!(
            (amount - expected.amount).abs() < 1e-6,
            "amount mismatch at row {i}: got {amount}, expected {}",
            expected.amount
        );

        // ClickHouse returns Bool columns as JSON true/false
        let active = row["active"]
            .as_bool()
            .or_else(|| row["active"].as_u64().map(|v| v != 0))
            .unwrap_or_else(|| panic!("Missing 'active' at row {i}"));
        assert_eq!(active, expected.active, "active mismatch at row {i}");
    }
}

// ── json_each_row: bulk ───────────────────────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn clickhouse_sink_handles_bulk_messages(
    harness: &TestHarness,
    fixture: ClickHouseSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let bulk_count = 50;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(bulk_count);
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

    fixture
        .wait_for_rows(bulk_count)
        .await
        .expect("Timed out waiting for bulk rows in ClickHouse");

    let row_count = fixture
        .count_rows()
        .await
        .expect("Failed to count rows from ClickHouse");

    assert!(
        row_count >= bulk_count,
        "Expected at least {bulk_count} rows, got {row_count}"
    );
}

// ── row_binary ────────────────────────────────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn clickhouse_sink_stores_messages_with_row_binary(
    harness: &TestHarness,
    fixture: ClickHouseSinkRowBinaryFixture,
) {
    let client = harness.root_client().await.unwrap();

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

    fixture
        .wait_for_rows(TEST_MESSAGE_COUNT)
        .await
        .expect("Timed out waiting for row_binary rows in ClickHouse");

    let rows = fixture
        .fetch_rows()
        .await
        .expect("Failed to fetch rows from ClickHouse");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in ClickHouse (row_binary)"
    );

    for (i, row) in rows.iter().enumerate() {
        let expected = &messages_data[i];

        let id = row["id"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| row["id"].as_u64())
            .unwrap_or_else(|| panic!("Missing 'id' at row {i}"));
        assert_eq!(id, expected.id, "id mismatch at row {i}");

        let name = row["name"]
            .as_str()
            .unwrap_or_else(|| panic!("Missing 'name' at row {i}"));
        assert_eq!(name, expected.name, "name mismatch at row {i}");

        let count = row["count"]
            .as_str()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| row["count"].as_u64().map(|v| v as u32))
            .unwrap_or_else(|| panic!("Missing 'count' at row {i}"));
        assert_eq!(count, expected.count, "count mismatch at row {i}");
    }
}

// ── string passthrough (CSV) ──────────────────────────────────────────────────

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/sink.toml")),
    seed = seeds::connector_stream
)]
async fn clickhouse_sink_stores_string_passthrough_csv(
    harness: &TestHarness,
    fixture: ClickHouseSinkStringFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Build CSV rows matching the iggy_messages table schema:
    // id UInt64, name String, count UInt32, amount Float64, active UInt8, timestamp Int64
    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let csv = format!(
                "{},{},{},{},{},{}\n",
                msg.id, msg.name, msg.count, msg.amount, msg.active as u8, msg.timestamp,
            );
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(csv.into_bytes()))
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

    fixture
        .wait_for_rows(TEST_MESSAGE_COUNT)
        .await
        .expect("Timed out waiting for CSV rows in ClickHouse");

    let row_count = fixture
        .count_rows()
        .await
        .expect("Failed to count rows from ClickHouse");

    assert_eq!(
        row_count, TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in ClickHouse (string/CSV)"
    );
}