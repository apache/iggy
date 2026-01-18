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
    SINK_TABLE, TEST_MESSAGE_COUNT, TEST_STREAM, TEST_TOPIC, setup_sink, setup_sink_bytea,
    setup_sink_json,
};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use iggy::prelude::IggyMessage;

#[tokio::test]
async fn given_json_messages_sink_connector_should_store_as_bytea() {
    let setup = setup_sink().await;
    let client = setup.runtime.create_client().await;
    let pool = setup.create_pool().await;

    setup.wait_for_table(&pool, SINK_TABLE).await;

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

    client.send_messages(&mut messages).await;

    let rows = setup.fetch_sink_rows(&pool, TEST_MESSAGE_COUNT).await;

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

#[tokio::test]
async fn given_binary_messages_sink_connector_should_store_as_bytea() {
    let setup = setup_sink_bytea().await;
    let client = setup.runtime.create_client().await;
    let pool = setup.create_pool().await;

    setup.wait_for_table(&pool, SINK_TABLE).await;

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

    client.send_messages(&mut messages).await;

    let rows = setup.fetch_sink_rows(&pool, TEST_MESSAGE_COUNT).await;

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

#[tokio::test]
async fn given_json_messages_sink_connector_should_store_as_jsonb() {
    let setup = setup_sink_json().await;
    let client = setup.runtime.create_client().await;
    let pool = setup.create_pool().await;

    setup.wait_for_table(&pool, SINK_TABLE).await;

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

    client.send_messages(&mut messages).await;

    let rows = setup.fetch_sink_json_rows(&pool, TEST_MESSAGE_COUNT).await;

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
