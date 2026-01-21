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

use super::setup_quickwit_sink;
use bytes::Bytes;
use iggy::prelude::IggyMessage;
use serde_json::json;

/// Test 1: Basic Indexing
/// Verifies that JSON messages sent to Iggy are indexed in Quickwit
/// and can be retrieved via the search API.
#[tokio::test]
async fn given_json_messages_should_be_indexed_in_quickwit() {
    let setup = setup_quickwit_sink().await;
    let client = setup.runtime.create_client().await;

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&json!({"message": "hello"})).unwrap(),
            ))
            .build()
            .unwrap(),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from(
                serde_json::to_vec(&json!({"message": "world"})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client.send_messages(&mut messages).await;

    let docs = setup.wait_for_documents(2).await;

    assert_eq!(
        docs.len(),
        2,
        "Expected 2 documents to be indexed in Quickwit"
    );

    // Verify document contents
    let contents: Vec<String> = docs
        .iter()
        .filter_map(|doc| doc.get("message").and_then(|m| m.as_str()))
        .map(|s| s.to_string())
        .collect();

    assert!(
        contents.contains(&"hello".to_string()),
        "Expected 'hello' message to be indexed"
    );
    assert!(
        contents.contains(&"world".to_string()),
        "Expected 'world' message to be indexed"
    );
}

/// Test 2: Bulk Ingest Behavior
/// Verifies that multiple messages in one batch are ingested as NDJSON
/// and all documents appear in the search results.
#[tokio::test]
async fn given_bulk_json_messages_should_be_ingested_as_ndjson() {
    let setup = setup_quickwit_sink().await;
    let client = setup.runtime.create_client().await;

    const BATCH_SIZE: usize = 10;

    let mut messages: Vec<IggyMessage> = (1..=BATCH_SIZE)
        .map(|i| {
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(&json!({
                        "message": format!("bulk_message_{}", i),
                        "sequence": i
                    }))
                    .unwrap(),
                ))
                .build()
                .unwrap()
        })
        .collect();

    client.send_messages(&mut messages).await;

    let docs = setup.wait_for_documents(BATCH_SIZE).await;

    assert_eq!(
        docs.len(),
        BATCH_SIZE,
        "Expected {} documents to be indexed in Quickwit",
        BATCH_SIZE
    );

    // Verify all sequence numbers are present
    let sequences: Vec<i64> = docs
        .iter()
        .filter_map(|doc| doc.get("sequence").and_then(|s| s.as_i64()))
        .collect();

    for i in 1..=BATCH_SIZE {
        assert!(
            sequences.contains(&(i as i64)),
            "Expected sequence {} to be indexed",
            i
        );
    }
}

/// Test 3: Error Handling for Invalid Payloads
/// Verifies that non-JSON payloads are ignored gracefully
/// and the connector continues processing valid messages without crashing.
#[tokio::test]
async fn given_invalid_payloads_should_be_ignored_gracefully() {
    let setup = setup_quickwit_sink().await;
    let client = setup.runtime.create_client().await;

    let mut messages = vec![
        // Valid JSON message
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&json!({"message": "valid_before"})).unwrap(),
            ))
            .build()
            .unwrap(),
        // Invalid: raw binary data (not JSON)
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from(vec![0x00, 0x01, 0xFF, 0xFE]))
            .build()
            .unwrap(),
        // Invalid: plain text (not valid JSON)
        IggyMessage::builder()
            .id(3)
            .payload(Bytes::from("this is not json"))
            .build()
            .unwrap(),
        // Valid JSON message
        IggyMessage::builder()
            .id(4)
            .payload(Bytes::from(
                serde_json::to_vec(&json!({"message": "valid_after"})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client.send_messages(&mut messages).await;

    // Wait for valid documents to be indexed
    // Only 2 valid JSON messages should be indexed, invalid ones ignored
    let docs = setup.wait_for_documents(2).await;

    assert_eq!(
        docs.len(),
        2,
        "Expected exactly 2 valid JSON documents to be indexed (invalid payloads should be ignored)"
    );

    // Verify only valid messages are present
    let messages_content: Vec<String> = docs
        .iter()
        .filter_map(|doc| doc.get("message").and_then(|m| m.as_str()))
        .map(|s| s.to_string())
        .collect();

    assert!(
        messages_content.contains(&"valid_before".to_string()),
        "Expected 'valid_before' message to be indexed"
    );
    assert!(
        messages_content.contains(&"valid_after".to_string()),
        "Expected 'valid_after' message to be indexed"
    );
}
