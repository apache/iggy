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

use std::time::Duration;

use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use tokio::time::{sleep, timeout};

use super::{DatabaseRecord, POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    PostgresOps, PostgresSourceByteaFixture, PostgresSourceDeleteFixture,
    PostgresSourceJsonFixture, PostgresSourceJsonbFixture, PostgresSourceMarkFixture,
    PostgresSourceOps,
};

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "postgres";
const SEND_FAILURE_TIMEOUT: Duration = Duration::from_secs(25);

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
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_rows_in_postgres_when_iggy_server_crashes_should_redeliver_after_restart(
    harness: &mut TestHarness,
    fixture: PostgresSourceJsonFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        // Keep a failed send bounded instead of waiting indefinitely for Iggy to return.
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    let errors_before_failure = source_errors(&http, &api_url).await;

    harness.kill_node(0).expect("Failed to kill Iggy server");

    let expected = create_test_messages(TEST_MESSAGE_COUNT);
    let mut transaction = pool.begin().await.expect("Failed to begin transaction");
    let insert = format!(
        "INSERT INTO {} (id, name, count, amount, active, timestamp, tag) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        fixture.table_name()
    );
    for message in &expected {
        let tag = format!("{:<10}", format!("tag_{}", message.id));
        sqlx::query(sqlx::AssertSqlSafe(insert.as_str()))
            .bind(message.id as i32)
            .bind(&message.name)
            .bind(message.count as i32)
            .bind(message.amount)
            .bind(message.active)
            .bind(message.timestamp)
            .bind(tag)
            .execute(&mut *transaction)
            .await
            .expect("Failed to insert source row");
    }
    transaction
        .commit()
        .await
        .expect("Failed to commit source rows");

    // The second error proves that NACK made the same rows eligible for another poll.
    wait_for_source_errors(&http, &api_url, errors_before_failure + 2).await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .restart_node(0)
        .expect("Failed to restart Iggy server");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "send_failure_consumer".try_into().unwrap();
    let mut received = Vec::new();

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
            received.extend(polled.messages.into_iter().filter_map(|message| {
                serde_json::from_slice::<DatabaseRecord>(&message.payload).ok()
            }));
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(
        received.len(),
        TEST_MESSAGE_COUNT,
        "Rows polled during the failed send should be delivered after restart"
    );
    for (record, expected) in received.iter().zip(expected) {
        assert_eq!(record.data.id, expected.id);
    }

    pool.close().await;
}

async fn source_errors(http: &Client, api_url: &str) -> u64 {
    http.get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("runtime stats should be available")
        .json::<ConnectorRuntimeStats>()
        .await
        .expect("runtime stats should be valid")
        .connectors
        .into_iter()
        .find(|connector| connector.key == SOURCE_KEY)
        .expect("PostgreSQL source stats should be present")
        .errors
}

async fn wait_for_source_errors(http: &Client, api_url: &str, minimum_errors: u64) {
    timeout(SEND_FAILURE_TIMEOUT, async {
        loop {
            if let Ok(response) = http
                .get(format!("{api_url}/stats"))
                .header("api-key", API_KEY)
                .send()
                .await
                && let Ok(stats) = response.json::<ConnectorRuntimeStats>().await
                && let Some(source) = stats
                    .connectors
                    .iter()
                    .find(|connector| connector.key == SOURCE_KEY)
                && source.status == ConnectorStatus::Error
                && source.errors >= minimum_errors
            {
                break;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("PostgreSQL source did not retry the NACKed batch");
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
