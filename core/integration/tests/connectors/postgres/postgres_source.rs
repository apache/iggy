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

// IggyClient is used in the function signature for macro parameter detection
#[allow(unused_imports)]
use iggy::prelude::IggyClient;
use iggy_binary_protocol::MessageClient;
use iggy_common::{Consumer, Identifier, IggyTimestamp, PollingStrategy};
use integration::harness::fixtures::{
    PostgresSourceByteaFixture, PostgresSourceDeleteFixture, PostgresSourceJsonFixture,
    PostgresSourceJsonbFixture, PostgresSourceMarkFixture,
};
use integration::harness::seeds;
use integration::iggy_harness;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;

const TEST_STREAM: &str = "test_stream";
const TEST_TOPIC: &str = "test_topic";
const TEST_MESSAGE_COUNT: usize = 3;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct TestMessage {
    pub id: u64,
    pub name: String,
    pub count: u32,
    pub amount: f64,
    pub active: bool,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseRecord {
    pub table_name: String,
    pub operation_type: String,
    pub data: TestMessage,
}

fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = IggyTimestamp::now().as_micros();
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

#[iggy_harness(
    server(connector(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn json_rows_source_produces_messages_to_iggy(
    client: &IggyClient,
    fixture: PostgresSourceJsonFixture,
) {
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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

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
}

#[iggy_harness(
    server(connector(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn bytea_rows_source_produces_raw_messages_to_iggy(
    client: &IggyClient,
    fixture: PostgresSourceByteaFixture,
) {
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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();
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
    server(connector(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn jsonb_rows_source_produces_json_messages_to_iggy(
    client: &IggyClient,
    fixture: PostgresSourceJsonbFixture,
) {
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

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();
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
                if let Ok(value) = serde_json::from_slice(&msg.payload) {
                    received.push(value);
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
    server(connector(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn delete_after_read_source_removes_rows_after_producing(
    client: &IggyClient,
    fixture: PostgresSourceDeleteFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), i as i32 * 10)
            .await;
    }

    let initial_count = fixture.count_rows(&pool).await;
    assert_eq!(
        initial_count, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} rows before processing"
    );

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();
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
                if let Ok(value) = serde_json::from_slice(&msg.payload) {
                    received.push(value);
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

    let final_count = fixture.count_rows(&pool).await;
    assert_eq!(
        final_count, 0,
        "Expected 0 rows after delete_after_read, got {final_count}"
    );

    pool.close().await;
}

#[iggy_harness(
    server(connector(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn processed_column_source_marks_rows_after_producing(
    client: &IggyClient,
    fixture: PostgresSourceMarkFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), i as i32 * 10)
            .await;
    }

    let initial_unprocessed = fixture.count_unprocessed(&pool).await;
    let initial_processed = fixture.count_processed(&pool).await;
    assert_eq!(
        initial_unprocessed, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} unprocessed rows before processing"
    );
    assert_eq!(
        initial_processed, 0,
        "Expected 0 processed rows before processing"
    );

    let stream_id: Identifier = TEST_STREAM.try_into().unwrap();
    let topic_id: Identifier = TEST_TOPIC.try_into().unwrap();
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
                if let Ok(value) = serde_json::from_slice(&msg.payload) {
                    received.push(value);
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

    let final_unprocessed = fixture.count_unprocessed(&pool).await;
    let final_processed = fixture.count_processed(&pool).await;
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
