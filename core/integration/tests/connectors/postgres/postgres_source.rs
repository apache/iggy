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

use super::{DatabaseRecord, POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    PostgresOps, PostgresSourceByteaFixture, PostgresSourceDeleteFixture,
    PostgresSourceJsonFixture, PostgresSourceJsonbFixture, PostgresSourceMarkFixture,
    PostgresSourceOps, PostgresSourceTextKeyFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::ConnectorState;
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn json_rows_source_produces_messages_to_iggy(
    harness: &TestHarness,
    fixture: PostgresSourceJsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let test_messages = create_test_messages(TEST_MESSAGE_COUNT);
    for msg in &test_messages {
        fixture
            .insert_row(
                &pool,
                msg.id as i32,
                &msg.name,
                msg.count as i32,
                msg.amount,
                msg.active,
                msg.timestamp,
            )
            .await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<DatabaseRecord> = Vec::new();
    let mut raw_payloads: Vec<Vec<u8>> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice(&msg.payload) {
                    raw_payloads.push(msg.payload.to_vec());
                    received.push(record);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, record) in received.iter().enumerate() {
        assert_eq!(
            record.table_name,
            fixture.table_name(),
            "Table name mismatch at record {i}"
        );
        assert_eq!(
            record.operation_type, "SELECT",
            "Operation type mismatch at record {i}"
        );
        assert_eq!(
            record.data, test_messages[i],
            "Message data mismatch at record {i}"
        );
    }

    // Verify BPCHAR (CHAR(n)) column extraction — Postgres reports CHAR(n) as BPCHAR
    for (i, raw) in raw_payloads.iter().enumerate() {
        let json: serde_json::Value =
            serde_json::from_slice(raw).expect("Failed to parse raw payload");
        let tag = json["data"]["tag"]
            .as_str()
            .unwrap_or_else(|| panic!("Missing BPCHAR 'tag' field in record {i}"));
        let expected_tag = format!("tag_{}", test_messages[i].id);
        assert_eq!(
            tag.trim(),
            expected_tag,
            "BPCHAR tag mismatch at record {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn bytea_rows_source_produces_raw_messages_to_iggy(
    harness: &TestHarness,
    fixture: PostgresSourceByteaFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let payloads: Vec<Vec<u8>> = vec![
        b"hello world".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE],
        serde_json::to_vec(&serde_json::json!({"key": "value", "number": 42}))
            .expect("Failed to serialize json"),
    ];

    for (i, payload) in payloads.iter().enumerate() {
        fixture.insert_payload(&pool, (i + 1) as i32, payload).await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<Vec<u8>> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                received.push(msg.payload.to_vec());
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        assert_eq!(payload, &payloads[i], "Payload mismatch at index {i}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn jsonb_rows_source_produces_json_messages_to_iggy(
    harness: &TestHarness,
    fixture: PostgresSourceJsonbFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "score": 100}),
        serde_json::json!({"items": ["a", "b", "c"]}),
        serde_json::json!({"nested": {"deep": {"value": 42}}}),
    ];

    for (i, payload) in json_payloads.iter().enumerate() {
        fixture.insert_json(&pool, (i + 1) as i32, payload).await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        assert_eq!(
            payload, &json_payloads[i],
            "JSON payload mismatch at index {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn delete_after_read_source_removes_rows_after_producing(
    harness: &TestHarness,
    fixture: PostgresSourceDeleteFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), (i * 10) as i32)
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let mut final_count = -1i64;
    for _ in 0..POLL_ATTEMPTS {
        final_count = fixture.count_rows(&pool).await;
        if final_count == 0 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        final_count, 0,
        "Expected 0 rows after delete_after_read, got {final_count}"
    );

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn processed_column_source_marks_rows_after_producing(
    harness: &TestHarness,
    fixture: PostgresSourceMarkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), (i * 10) as i32)
            .await;
    }

    let initial_unprocessed = fixture.count_unprocessed(&pool).await;
    let initial_processed = fixture.count_processed(&pool).await;
    assert_eq!(
        initial_unprocessed + initial_processed,
        TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} total rows before processing, got {} unprocessed + {} processed",
        initial_unprocessed,
        initial_processed
    );

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let mut final_unprocessed = -1i64;
    let mut final_processed = -1i64;
    for _ in 0..POLL_ATTEMPTS {
        final_unprocessed = fixture.count_unprocessed(&pool).await;
        final_processed = fixture.count_processed(&pool).await;
        if final_unprocessed == 0 && final_processed == TEST_MESSAGE_COUNT as i64 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        final_unprocessed, 0,
        "Expected 0 unprocessed rows after processing, got {final_unprocessed}"
    );
    assert_eq!(
        final_processed, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} processed rows after processing, got {final_processed}"
    );

    let total_count = fixture.count_rows(&pool).await;
    assert_eq!(
        total_count, TEST_MESSAGE_COUNT as i64,
        "Rows should not be deleted, expected {TEST_MESSAGE_COUNT}, got {total_count}"
    );

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_persists_across_connector_restart(
    harness: &mut TestHarness,
    fixture: PostgresSourceJsonFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let first_batch = create_test_messages(TEST_MESSAGE_COUNT);
    for msg in &first_batch {
        fixture
            .insert_row(
                &pool,
                msg.id as i32,
                &msg.name,
                msg.count as i32,
                msg.amount,
                msg.active,
                msg.timestamp,
            )
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "state_test_consumer".try_into().unwrap();

    let client = harness.root_client().await.unwrap();
    let received_before = {
        let mut received: Vec<DatabaseRecord> = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(polled) = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    10,
                    true,
                )
                .await
            {
                for msg in polled.messages {
                    if let Ok(record) = serde_json::from_slice(&msg.payload) {
                        received.push(record);
                    }
                }
                if received.len() >= TEST_MESSAGE_COUNT {
                    break;
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        received
    };
    assert_eq!(received_before.len(), TEST_MESSAGE_COUNT);

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");

    let second_batch_start_id = (TEST_MESSAGE_COUNT + 1) as i32;
    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(
                &pool,
                second_batch_start_id + i as i32,
                &format!("user_batch2_{i}"),
                ((TEST_MESSAGE_COUNT + i) * 10) as i32,
                (TEST_MESSAGE_COUNT + i) as f64 * 99.99,
                i % 2 == 0,
                iggy_common::IggyTimestamp::now().as_micros() as i64,
            )
            .await;
    }

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");
    sleep(Duration::from_secs(2)).await;

    let mut received_after: Vec<DatabaseRecord> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice(&msg.payload) {
                    received_after.push(record);
                }
            }
            if received_after.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(received_after.len(), TEST_MESSAGE_COUNT);

    for record in &received_after {
        assert!(
            record.data.id > TEST_MESSAGE_COUNT as u64,
            "After restart, got ID {} from first batch",
            record.data.id
        );
    }

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn checkpoint_failure_source_replays_rows_until_acknowledged(
    harness: &TestHarness,
    fixture: PostgresSourceByteaFixture,
) {
    let runtime = harness.connectors_runtime().expect("connector runtime");
    let checkpoint = runtime.state_path().join("source_postgres.state");
    fs::create_dir(&checkpoint).expect("force checkpoint rename failure");

    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    let payload = b"replay after checkpoint failure";
    let query = format!(
        "CREATE TABLE {} AS SELECT 1 AS id, $1::BYTEA AS payload",
        fixture.table_name()
    );
    sqlx::query(sqlx::AssertSqlSafe(query))
        .bind(payload.as_slice())
        .execute(&pool)
        .await
        .expect("create the table and its row atomically");
    pool.close().await;

    let checkpoint_failed = wait_for_failed_checkpoint(harness).await;
    fs::remove_dir(&checkpoint).expect("restore checkpoint destination");
    assert!(checkpoint_failed, "the produced row must receive a Nack");

    let received = poll_payloads(harness, "checkpoint_failure", 2).await;

    for _ in 0..POLL_ATTEMPTS {
        if checkpoint.is_file() {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert!(
        checkpoint.is_file(),
        "checkpoint must recover after the failure"
    );
    assert!(
        received.len() >= 2,
        "a Nacked row must be replayed; received {} copies",
        received.len()
    );
    for message in received {
        assert_eq!(message, payload);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn text_keys_source_tracks_and_deletes_numeric_and_quoted_values(
    harness: &TestHarness,
    fixture: PostgresSourceDeleteFixture,
) {
    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    let query = format!(
        "CREATE TABLE {} AS SELECT id FROM (VALUES ('01'), ('02'), ('O''Brien')) rows(id)",
        fixture.table_name()
    );
    sqlx::query(sqlx::AssertSqlSafe(query))
        .execute(&pool)
        .await
        .expect("create TEXT keys and rows atomically");

    let expected = ["01", "02", "O'Brien"];
    let received: Vec<_> = poll_payloads(harness, "text_keys", expected.len())
        .await
        .into_iter()
        .map(|payload| {
            let row: serde_json::Value =
                serde_json::from_slice(&payload).expect("source JSON envelope");
            row["data"]["id"].as_str().expect("TEXT key").to_string()
        })
        .collect();
    assert_eq!(
        received, expected,
        "TEXT keys must retain their exact values"
    );
    assert_eq!(
        fixture.count_rows(&pool).await,
        0,
        "all selected keys must be deleted"
    );
    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn numeric_rows_source_preserves_numbers_and_nulls(
    harness: &TestHarness,
    fixture: PostgresSourceJsonFixture,
) {
    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    let query = format!(
        "CREATE TABLE {} AS SELECT * FROM (VALUES
            (1, 12.50::NUMERIC), (2, -98765.125), (3, 0),
            (4, 123456789.875), (5, NULL)) rows(id, amount)",
        fixture.table_name()
    );
    sqlx::query(sqlx::AssertSqlSafe(query))
        .execute(&pool)
        .await
        .expect("create NUMERIC rows atomically");
    pool.close().await;

    let expected = [
        Some(12.5),
        Some(-98765.125),
        Some(0.0),
        Some(123456789.875),
        None,
    ];
    let received = poll_payloads(harness, "numeric_rows", expected.len()).await;
    assert_eq!(
        received.len(),
        expected.len(),
        "every NUMERIC row must be delivered"
    );
    for (payload, expected) in received.iter().zip(expected) {
        let row: serde_json::Value = serde_json::from_slice(payload).expect("source JSON envelope");
        assert_eq!(
            row["data"]["amount"].as_f64(),
            expected,
            "NUMERIC value in {row}"
        );
        assert_eq!(row["data"]["amount"].is_null(), expected.is_none());
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn selected_payload_key_source_tracks_and_marks_rows(
    harness: &TestHarness,
    fixture: PostgresSourceTextKeyFixture,
) {
    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    let query = format!(
        "CREATE TABLE {} AS SELECT id, FALSE AS processed FROM (VALUES ('01'), ('02'), ('03')) rows(id)",
        fixture.table_name()
    );
    sqlx::query(sqlx::AssertSqlSafe(query))
        .execute(&pool)
        .await
        .expect("create rows whose payload is also the tracking and primary key");

    let expected = [b"01".to_vec(), b"02".to_vec(), b"03".to_vec()];
    let received = poll_payloads(harness, "payload_key", expected.len()).await;
    assert_eq!(received, expected, "selected keys must be delivered once");

    let query = format!(
        "SELECT COUNT(*) FROM {} WHERE processed",
        fixture.table_name()
    );
    let checkpoint = harness
        .connectors_runtime()
        .expect("connector runtime")
        .state_path()
        .join("source_postgres.state");
    let mut processed = 0i64;
    for _ in 0..POLL_ATTEMPTS {
        processed = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(&pool)
            .await
            .expect("count marked rows");
        if processed == expected.len() as i64 && checkpoint.is_file() {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        processed,
        expected.len() as i64,
        "selected payload keys must still identify rows to mark"
    );

    let state = ConnectorState(fs::read(checkpoint).expect("acknowledged checkpoint"))
        .deserialize::<serde_json::Value>("PostgreSQL test", 1)
        .expect("existing MessagePack state");
    assert_eq!(
        state[1][fixture.table_name()],
        "03",
        "selected payload key must still advance the cursor"
    );
    assert_eq!(state[2], 3, "each row must be acknowledged once");
    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn delete_source_retains_rows_until_acknowledged(
    harness: &TestHarness,
    fixture: PostgresSourceDeleteFixture,
) {
    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    assert_cleanup_after_ack(harness, &pool, fixture.table_name(), "TRUE").await;
    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn mark_source_retains_unprocessed_rows_until_acknowledged(
    harness: &TestHarness,
    fixture: PostgresSourceMarkFixture,
) {
    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    assert_cleanup_after_ack(harness, &pool, fixture.table_name(), "NOT is_processed").await;
    pool.close().await;
}

async fn assert_cleanup_after_ack(
    harness: &TestHarness,
    pool: &sqlx::PgPool,
    table: &str,
    pending_condition: &str,
) {
    let checkpoint = harness
        .connectors_runtime()
        .expect("connector runtime")
        .state_path()
        .join("source_postgres.state");
    fs::create_dir(&checkpoint).expect("force checkpoint rename failure");
    let query = format!("CREATE TABLE {table} AS SELECT 1 AS id, FALSE AS is_processed");
    sqlx::query(sqlx::AssertSqlSafe(query))
        .execute(pool)
        .await
        .expect("create the table and row atomically");
    let failed = wait_for_failed_checkpoint(harness).await;
    let query = format!("SELECT COUNT(*) FROM {table} WHERE {pending_condition}");
    let pending: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
        .fetch_one(pool)
        .await
        .expect("count rows before Ack");
    fs::remove_dir(&checkpoint).expect("restore checkpoint destination");
    assert!(failed, "the produced batch must receive Nack");
    assert_eq!(pending, 1, "Nack must leave the row available for replay");

    let received = poll_payloads(harness, "cleanup_ack", 2).await;
    assert!(received.len() >= 2, "the rejected row must be replayed");
    for payload in received {
        let record: serde_json::Value = serde_json::from_slice(&payload).expect("JSON envelope");
        assert_eq!(record["data"]["id"], 1);
    }
    let mut pending = 1i64;
    for _ in 0..POLL_ATTEMPTS {
        pending = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(pool)
            .await
            .expect("count rows after Ack");
        if pending == 0 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(pending, 0, "Ack must apply the requested row cleanup");
    let state = ConnectorState(fs::read(checkpoint).expect("acknowledged checkpoint"))
        .deserialize::<serde_json::Value>("PostgreSQL test", 1)
        .expect("MessagePack state");
    assert_eq!(
        state[2], 1,
        "a replay must not double-count the acknowledged row"
    );
}

async fn wait_for_failed_checkpoint(harness: &TestHarness) -> bool {
    let runtime = harness.connectors_runtime().expect("connector runtime");
    for _ in 0..POLL_ATTEMPTS {
        if runtime
            .collect_logs()
            .0
            .contains("Failed to save state for source connector")
        {
            return true;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    false
}

async fn poll_payloads(
    harness: &TestHarness,
    consumer_name: &str,
    expected: usize,
) -> Vec<Vec<u8>> {
    let client = harness.root_client().await.expect("root client");
    let stream: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::new(consumer_name.try_into().unwrap());
    let mut received = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received.extend(
                polled
                    .messages
                    .into_iter()
                    .map(|message| message.payload.to_vec()),
            );
            if received.len() >= expected {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}
