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

use super::{
    DatabaseRecord, SOURCE_TABLE, SOURCE_TABLE_DELETE, SOURCE_TABLE_MARK, TEST_MESSAGE_COUNT,
    setup_source, setup_source_bytea, setup_source_delete, setup_source_json, setup_source_mark,
};
use crate::connectors::create_test_messages;

#[tokio::test]
async fn given_json_rows_source_connector_should_produce_messages_to_iggy() {
    let setup = setup_source().await;
    let pool = setup.create_pool().await;
    let test_messages = create_test_messages(TEST_MESSAGE_COUNT);

    setup.insert_test_messages(&pool, &test_messages).await;
    pool.close().await;

    let received: Vec<DatabaseRecord> = setup
        .poll_messages(TEST_MESSAGE_COUNT, |payload| {
            serde_json::from_slice(payload).ok()
        })
        .await;

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, record) in received.iter().enumerate() {
        assert_eq!(
            record.table_name, SOURCE_TABLE,
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

#[tokio::test]
async fn given_bytea_rows_source_connector_should_produce_raw_messages_to_iggy() {
    let setup = setup_source_bytea().await;
    let pool = setup.create_pool().await;

    let payloads: Vec<Vec<u8>> = vec![
        b"hello world".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE],
        serde_json::to_vec(&serde_json::json!({"key": "value", "number": 42}))
            .expect("Failed to serialize json"),
    ];

    setup.insert_bytea_payloads(&pool, &payloads).await;
    pool.close().await;

    let received: Vec<Vec<u8>> = setup
        .poll_messages(TEST_MESSAGE_COUNT, |payload| Some(payload.to_vec()))
        .await;

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        assert_eq!(payload, &payloads[i], "Payload mismatch at index {i}");
    }
}

#[tokio::test]
async fn given_jsonb_rows_source_connector_should_produce_json_messages_to_iggy() {
    let setup = setup_source_json().await;
    let pool = setup.create_pool().await;

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "score": 100}),
        serde_json::json!({"items": ["a", "b", "c"]}),
        serde_json::json!({"nested": {"deep": {"value": 42}}}),
    ];

    setup.insert_json_payloads(&pool, &json_payloads).await;
    pool.close().await;

    let received: Vec<serde_json::Value> = setup
        .poll_messages(TEST_MESSAGE_COUNT, |payload| {
            serde_json::from_slice(payload).ok()
        })
        .await;

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

#[tokio::test]
async fn given_delete_after_read_source_connector_should_remove_rows_after_producing() {
    let setup = setup_source_delete().await;
    let pool = setup.create_pool().await;

    setup.insert_delete_rows(&pool, TEST_MESSAGE_COUNT).await;

    let initial_count = setup.count_rows(&pool, SOURCE_TABLE_DELETE).await;
    assert_eq!(
        initial_count, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} rows before processing"
    );

    let received: Vec<serde_json::Value> = setup
        .poll_messages(TEST_MESSAGE_COUNT, |payload| {
            serde_json::from_slice(payload).ok()
        })
        .await;

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let final_count = setup.count_rows(&pool, SOURCE_TABLE_DELETE).await;
    assert_eq!(
        final_count, 0,
        "Expected 0 rows after delete_after_read, got {final_count}"
    );

    pool.close().await;
}

#[tokio::test]
async fn given_processed_column_source_connector_should_mark_rows_after_producing() {
    let setup = setup_source_mark().await;
    let pool = setup.create_pool().await;

    setup.insert_mark_rows(&pool, TEST_MESSAGE_COUNT).await;

    let initial_unprocessed = setup.count_unprocessed_rows(&pool, SOURCE_TABLE_MARK).await;
    let initial_processed = setup.count_processed_rows(&pool, SOURCE_TABLE_MARK).await;
    assert_eq!(
        initial_unprocessed, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} unprocessed rows before processing"
    );
    assert_eq!(
        initial_processed, 0,
        "Expected 0 processed rows before processing"
    );

    let received: Vec<serde_json::Value> = setup
        .poll_messages(TEST_MESSAGE_COUNT, |payload| {
            serde_json::from_slice(payload).ok()
        })
        .await;

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let final_unprocessed = setup.count_unprocessed_rows(&pool, SOURCE_TABLE_MARK).await;
    let final_processed = setup.count_processed_rows(&pool, SOURCE_TABLE_MARK).await;
    assert_eq!(
        final_unprocessed, 0,
        "Expected 0 unprocessed rows after processing, got {final_unprocessed}"
    );
    assert_eq!(
        final_processed, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} processed rows after processing, got {final_processed}"
    );

    let total_count = setup.count_rows(&pool, SOURCE_TABLE_MARK).await;
    assert_eq!(
        total_count, TEST_MESSAGE_COUNT as i64,
        "Rows should not be deleted, expected {TEST_MESSAGE_COUNT}, got {total_count}"
    );

    pool.close().await;
}
