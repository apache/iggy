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
    HttpSinkIndividualFixture, HttpSinkJsonArrayFixture, HttpSinkNdjsonFixture,
    HttpSinkNoMetadataFixture, HttpSinkRawFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;

/// Send JSON messages to Iggy via individual batch mode and verify each arrives
/// as a separate HTTP POST with the metadata envelope.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn individual_json_messages_delivered_as_separate_posts(
    harness: &TestHarness,
    fixture: HttpSinkIndividualFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "age": 30}),
        serde_json::json!({"name": "Bob", "score": 99}),
        serde_json::json!({"name": "Carol", "active": true}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize");
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

    // In individual mode, each message becomes a separate HTTP request.
    let requests = fixture
        .container()
        .wait_for_requests(TEST_MESSAGE_COUNT)
        .await
        .expect("WireMock did not receive expected number of requests");

    assert_eq!(
        requests.len(),
        TEST_MESSAGE_COUNT,
        "Expected exactly {TEST_MESSAGE_COUNT} individual requests, got {}",
        requests.len()
    );

    // Verify each request is a POST to /ingest with JSON content type.
    for req in &requests {
        assert_eq!(req.method, "POST", "Expected POST method");
        assert_eq!(req.url, "/ingest", "Expected /ingest URL");

        let body = req.body_as_json().expect("Body should be valid JSON");

        // Metadata envelope should be present.
        assert!(
            body.get("metadata").is_some(),
            "Expected metadata envelope in individual mode, got: {body}"
        );
        assert!(
            body.get("payload").is_some(),
            "Expected payload field in individual mode, got: {body}"
        );

        // Verify metadata fields.
        let metadata = &body["metadata"];
        assert!(
            metadata.get("iggy_stream").is_some(),
            "Expected iggy_stream in metadata"
        );
        assert!(
            metadata.get("iggy_topic").is_some(),
            "Expected iggy_topic in metadata"
        );
        assert!(
            metadata.get("iggy_offset").is_some(),
            "Expected iggy_offset in metadata"
        );
    }

    // Verify the content type header.
    let ct = requests[0]
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/json"),
        "Expected application/json content type, got: {ct}"
    );
}

/// Send JSON messages via NDJSON batch mode and verify they arrive as a single
/// request with newline-delimited JSON body.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn ndjson_messages_delivered_as_single_request(
    harness: &TestHarness,
    fixture: HttpSinkNdjsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"event": "login", "user": 1}),
        serde_json::json!({"event": "click", "user": 2}),
        serde_json::json!({"event": "logout", "user": 3}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize");
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

    // In NDJSON mode, all messages should arrive in a single HTTP request.
    let requests = fixture
        .container()
        .wait_for_requests(1)
        .await
        .expect("WireMock did not receive NDJSON request");

    let req = &requests[0];
    assert_eq!(req.method, "POST", "Expected POST method");
    assert_eq!(req.url, "/ingest", "Expected /ingest URL");

    // NDJSON body: each line is a valid JSON object.
    let lines: Vec<&str> = req.body.trim().lines().collect();
    assert_eq!(
        lines.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} NDJSON lines, got {}",
        lines.len()
    );

    for (i, line) in lines.iter().enumerate() {
        let parsed: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("NDJSON line {i} invalid: {e}"));
        assert!(
            parsed.get("metadata").is_some(),
            "Expected metadata in NDJSON line {i}"
        );
        assert!(
            parsed.get("payload").is_some(),
            "Expected payload in NDJSON line {i}"
        );
    }

    // Verify content type is NDJSON.
    let ct = req
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/x-ndjson"),
        "Expected application/x-ndjson content type, got: {ct}"
    );
}

/// Send JSON messages via JSON array batch mode and verify they arrive as a
/// single request with a JSON array body.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_array_messages_delivered_as_single_request(
    harness: &TestHarness,
    fixture: HttpSinkJsonArrayFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"id": 1, "type": "order"}),
        serde_json::json!({"id": 2, "type": "payment"}),
        serde_json::json!({"id": 3, "type": "refund"}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize");
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

    // In JSON array mode, all messages arrive in a single request.
    let requests = fixture
        .container()
        .wait_for_requests(1)
        .await
        .expect("WireMock did not receive JSON array request");

    let req = &requests[0];
    assert_eq!(req.method, "POST", "Expected POST method");
    assert_eq!(req.url, "/ingest", "Expected /ingest URL");

    let body = req.body_as_json().expect("Body should be valid JSON");
    assert!(body.is_array(), "Expected JSON array body, got: {body}");

    let arr = body.as_array().unwrap();
    assert_eq!(
        arr.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} items in JSON array, got {}",
        arr.len()
    );

    for (i, item) in arr.iter().enumerate() {
        assert!(
            item.get("metadata").is_some(),
            "Expected metadata in array item {i}"
        );
        assert!(
            item.get("payload").is_some(),
            "Expected payload in array item {i}"
        );
    }

    // Verify content type is JSON.
    let ct = req
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/json"),
        "Expected application/json content type, got: {ct}"
    );
}

/// Send binary messages via raw batch mode and verify each arrives as a
/// separate HTTP POST with raw bytes (no metadata envelope).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn raw_binary_messages_delivered_without_envelope(
    harness: &TestHarness,
    fixture: HttpSinkRawFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let raw_payloads: Vec<Vec<u8>> = vec![
        b"plain text message".to_vec(),
        b"another raw payload".to_vec(),
        b"third raw message".to_vec(),
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

    // Raw mode: one request per message, raw bytes in body.
    let requests = fixture
        .container()
        .wait_for_requests(TEST_MESSAGE_COUNT)
        .await
        .expect("WireMock did not receive expected raw requests");

    assert_eq!(
        requests.len(),
        TEST_MESSAGE_COUNT,
        "Expected exactly {TEST_MESSAGE_COUNT} raw requests, got {}",
        requests.len()
    );

    for req in &requests {
        assert_eq!(req.method, "POST", "Expected POST method");
        assert_eq!(req.url, "/ingest", "Expected /ingest URL");

        // Raw mode should NOT have metadata envelope — body is raw payload.
        // The body should NOT parse as a JSON object with "metadata" key.
        if let Ok(json) = req.body_as_json() {
            assert!(
                json.get("metadata").is_none(),
                "Raw mode should not include metadata envelope"
            );
        }
    }

    // Verify content type is octet-stream for raw mode.
    let ct = requests[0]
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/octet-stream"),
        "Expected application/octet-stream for raw mode, got: {ct}"
    );
}

/// Send JSON messages with metadata disabled and verify payloads arrive
/// without the metadata envelope wrapper.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn metadata_disabled_sends_bare_payload(
    harness: &TestHarness,
    fixture: HttpSinkNoMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"key": "value1"}),
        serde_json::json!({"key": "value2"}),
        serde_json::json!({"key": "value3"}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize");
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

    let requests = fixture
        .container()
        .wait_for_requests(TEST_MESSAGE_COUNT)
        .await
        .expect("WireMock did not receive requests");

    for (i, req) in requests.iter().enumerate() {
        let body = req
            .body_as_json()
            .unwrap_or_else(|e| panic!("Request {i} body should be valid JSON: {e}"));

        // Without metadata, the body should be the bare payload — no "metadata" wrapper.
        assert!(
            body.get("metadata").is_none(),
            "Expected no metadata envelope when include_metadata=false, got: {body}"
        );

        // The payload should be the original JSON object directly.
        assert!(
            body.get("key").is_some(),
            "Expected bare payload with 'key' field, got: {body}"
        );
    }
}

/// Verify that offsets in metadata are sequential across individual messages.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn individual_messages_have_sequential_offsets(
    harness: &TestHarness,
    fixture: HttpSinkIndividualFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..5)
        .map(|i| {
            let payload = serde_json::to_vec(&serde_json::json!({"idx": i}))
                .expect("Failed to serialize");
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

    let requests = fixture
        .container()
        .wait_for_requests(5)
        .await
        .expect("WireMock did not receive all 5 requests");

    // Collect offsets from metadata and verify contiguous sequential ordering.
    // Note: offsets may not start at 0 if the topic already had messages.
    let mut offsets: Vec<i64> = requests
        .iter()
        .filter_map(|r| {
            r.body_as_json()
                .ok()
                .and_then(|b| b["metadata"]["iggy_offset"].as_i64())
        })
        .collect();

    offsets.sort();
    assert_eq!(offsets.len(), 5, "Expected 5 offsets, got {}", offsets.len());

    // Verify offsets are contiguous (each +1 from previous), regardless of base.
    for window in offsets.windows(2) {
        assert_eq!(
            window[1],
            window[0] + 1,
            "Offsets must be contiguous: got {} then {}",
            window[0],
            window[1]
        );
    }
}
