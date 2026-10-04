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
    POSTGRES_LARGE_BATCH_SIZE, PostgresOps, PostgresSinkByteaFixture, PostgresSinkFixture,
    PostgresSinkJsonFixture, PostgresSinkLargeBatchFixture,
};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use iggy::prelude::{IggyClient, IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::ConnectorRuntimeStats;
use integration::harness::seeds;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const SINK_TABLE: &str = "iggy_messages";
const DEFAULT_INSERT_PARAMETERS: usize = 9;
const MAX_DEFAULT_INSERT_ROWS: usize = u16::MAX as usize / DEFAULT_INSERT_PARAMETERS;
const STATS_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const STATS_POLL_INTERVAL: Duration = Duration::from_millis(100);

type SinkRow = (i64, String, String, Vec<u8>);
type SinkJsonRow = (i64, serde_json::Value);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = large_postgres_batch
)]
async fn oversized_callback_stores_all_rows(fixture: PostgresSinkLargeBatchFixture) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(
            &pool,
            "SELECT iggy_offset, payload FROM iggy_messages ORDER BY iggy_offset",
            POSTGRES_LARGE_BATCH_SIZE,
        )
        .await
        .expect("Every row must survive bind-limit chunking");
    assert_eq!(rows.len(), POSTGRES_LARGE_BATCH_SIZE);
    for (sequence, (offset, payload)) in rows.into_iter().enumerate() {
        assert_eq!(offset, sequence as i64);
        assert_eq!(payload, serde_json::json!({"sequence": sequence}));
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = large_postgres_batch_with_invalid_record
)]
async fn failed_chunk_reports_runtime_error_and_preserves_later_chunks(
    harness: &TestHarness,
    fixture: PostgresSinkLargeBatchFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    let expected_rows = POSTGRES_LARGE_BATCH_SIZE - MAX_DEFAULT_INSERT_ROWS;
    let rows: Vec<SinkJsonRow> = fixture
        .fetch_rows_as(
            &pool,
            "SELECT iggy_offset, payload FROM iggy_messages ORDER BY iggy_offset",
            expected_rows,
        )
        .await
        .expect("The chunk after the rejected chunk must still be inserted");
    assert_eq!(rows.len(), expected_rows);
    for (position, (offset, payload)) in rows.into_iter().enumerate() {
        let sequence = MAX_DEFAULT_INSERT_ROWS + position;
        assert_eq!(offset, sequence as i64);
        assert_eq!(payload, serde_json::json!({"sequence": sequence}));
    }

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let stats_url = format!("{}/stats", runtime.http_url());
    let http_client = reqwest::Client::new();
    let stats = timeout(STATS_WAIT_TIMEOUT, async {
        loop {
            let snapshot: ConnectorRuntimeStats = http_client
                .get(&stats_url)
                .send()
                .await
                .expect("stats request")
                .error_for_status()
                .expect("stats status")
                .json()
                .await
                .expect("stats response");
            let sink = snapshot
                .connectors
                .into_iter()
                .find(|sink| sink.key == "postgres")
                .expect("PostgreSQL sink must be reported");
            if sink.errors > 0 {
                break sink;
            }
            sleep(STATS_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("The failed chunk must reach runtime error statistics");
    assert_eq!(stats.errors, 1);
    assert_eq!(
        stats.messages_consumed,
        Some(POSTGRES_LARGE_BATCH_SIZE as u64)
    );
    assert_eq!(stats.messages_processed, Some(0));
}

async fn large_postgres_batch(client: &IggyClient) -> Result<(), seeds::SeedError> {
    seed_large_postgres_batch(client, false).await
}

async fn large_postgres_batch_with_invalid_record(
    client: &IggyClient,
) -> Result<(), seeds::SeedError> {
    seed_large_postgres_batch(client, true).await
}

async fn seed_large_postgres_batch(
    client: &IggyClient,
    invalid_first_record: bool,
) -> Result<(), seeds::SeedError> {
    seeds::connector_stream(client).await?;
    let stream_id: Identifier = seeds::names::STREAM.try_into()?;
    let topic_id: Identifier = seeds::names::TOPIC.try_into()?;
    let mut messages = Vec::with_capacity(POSTGRES_LARGE_BATCH_SIZE);
    for sequence in 0..POSTGRES_LARGE_BATCH_SIZE {
        let payload = if invalid_first_record && sequence == 0 {
            b"invalid JSON".to_vec()
        } else {
            serde_json::to_vec(&serde_json::json!({"sequence": sequence}))?
        };
        messages.push(
            IggyMessage::builder()
                .id(sequence as u128 + 1)
                .payload(Bytes::from(payload))
                .build()?,
        );
    }
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await?;
    Ok(())
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(harness: &TestHarness, fixture: PostgresSinkFixture) {
    let client = harness.root_client().await.unwrap();
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
    let client = harness.root_client().await.unwrap();
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
    let client = harness.root_client().await.unwrap();
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
