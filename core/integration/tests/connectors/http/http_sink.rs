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

//! HTTP Sink Connector: Integration Tests
//!
//! **Purpose**: End-to-end validation of the HTTP sink connector — messages flow from
//! Iggy streams through the connector runtime, get transformed by the sink plugin, and
//! arrive at a real HTTP endpoint where we verify format, headers, metadata, and content.
//!
//! ## Connector Architecture
//!
//! The HTTP sink runs inside the Iggy connector runtime as a dynamically loaded plugin:
//!
//! ```text
//! ┌──────────────┐    ┌──────────────────────┐    ┌──────────────────┐
//! │  Test Code   │    │  Connector Runtime   │    │    WireMock      │
//! │              │    │                      │    │                  │
//! │ send_messages├───►│  iggy-server (poll)  │    │  /__admin/       │
//! │              │    │        │             │    │  (verify reqs)   │
//! │              │    │  ┌─────▼──────────┐  │    │                  │
//! │ wait_for_    │    │  │ HTTP Sink      │  │    │  /ingest         │
//! │ requests ◄───┼────┤  │ (.so/.dylib)   ├──┼───►│  (accept POST)   │
//! │              │    │  └────────────────┘  │    │                  │
//! └──────────────┘    └──────────────────────┘    └──────────────────┘
//! ```
//!
//! **Key components**:
//! 1. **iggy-server**: Stores messages in streams/topics, serves them to consumers
//! 2. **Connector runtime**: `iggy-connectors` binary, loads the HTTP sink `.so`/`.dylib`
//!    plugin via FFI, polls topics, calls `iggy_sink_consume()` per batch
//! 3. **HTTP sink plugin**: Transforms messages into HTTP requests (4 batch modes),
//!    applies metadata envelope, retries on failure
//! 4. **WireMock**: Docker container accepting all POSTs to `/ingest`, recording
//!    requests for later verification via `/__admin/requests`
//!
//! **Runtime model**: 1 process = 1 config = 1 plugin. The runtime reads `config.toml`,
//! loads the plugin binary, iterates `for topic in stream.topics`, and spawns one
//! `tokio::spawn` task per topic. Each task creates an `IggyConsumer` and polls
//! sequentially — `consume()` is awaited before the next poll.
//!
//! See `setup_sink_consumers()` and `spawn_consume_tasks()` in `runtime/src/sink.rs`.
//!
//! ## What These Tests Validate
//!
//! **Test 1 — Individual Mode**: Each message becomes a separate HTTP POST with
//! metadata envelope (`{metadata: {...}, payload: {...}}`). Validates envelope
//! structure, content type, and per-message delivery.
//!
//! **Test 2 — NDJSON Batch Mode**: All messages arrive in one HTTP request as
//! newline-delimited JSON. Validates line count, per-line envelope structure,
//! and `application/x-ndjson` content type.
//!
//! **Test 3 — JSON Array Batch Mode**: All messages arrive in one HTTP request
//! as a JSON array. Validates array length, per-item envelope structure, and
//! `application/json` content type.
//!
//! **Test 4 — Raw Mode**: Each message sent as raw bytes without metadata envelope.
//! Validates `application/octet-stream` content type and absence of envelope wrapper.
//!
//! **Test 5 — Metadata Disabled**: Individual mode with `include_metadata=false`.
//! Validates that the bare payload arrives without the `{metadata, payload}` wrapper.
//!
//! **Test 6 — Sequential Offsets**: Sends 5 messages and verifies `iggy_offset` values
//! in metadata are contiguous (each offset = previous + 1). Validates that the
//! connector preserves Iggy's offset ordering through the HTTP delivery pipeline.
//!
//! **Test 7 — Multi-Topic**: One connector consuming from two topics on the same
//! stream. Validates that `iggy_topic` metadata correctly identifies the source topic,
//! and that messages from both topics arrive at the shared endpoint. Exercises the
//! runtime's per-topic task spawning (`spawn_consume_tasks()` in `runtime/src/sink.rs`).
//!
//! ## Test Infrastructure
//!
//! **Full-Stack Integration** (all components are real — no mocks):
//! - **iggy-server**: Started by `#[iggy_harness]` macro, in-process
//! - **Connector runtime**: Started by harness with `connectors_runtime(config_path = ...)`
//! - **HTTP sink plugin**: Built from `core/connectors/sinks/http_sink/` (must be compiled)
//! - **WireMock**: Docker container (`wiremock/wiremock:3.13.2`) via testcontainers
//! - **Test fixtures**: `HttpSink*Fixture` structs configure batch mode, metadata, topics
//!   via environment variables that override `config.toml` fields
//!
//! **Fixture Architecture**:
//! Each fixture implements `TestFixture` trait, returning `connectors_runtime_envs()` that
//! override the plugin config. The base configuration (`HttpSinkIndividualFixture::base_envs`)
//! sets URL, method, timeout, retries, stream/topic, and schema. Specialized fixtures
//! (NDJSON, JSON array, raw, no-metadata, multi-topic) override specific fields.
//!
//! **WireMock Container**:
//! Accepts all POSTs to `/ingest` (via `accept-ingest.json` mapping). Exposes
//! `/__admin/requests` for polling received requests. The container uses a bind mount
//! for mappings and a health check wait strategy for readiness.
//!
//! **Seed Data**:
//! `seeds::connector_stream` creates the stream (`test_stream`) and first topic
//! (`test_topic`). The multi-topic test creates a second topic inline to avoid
//! polluting the shared harness with HTTP-sink-specific constants.
//!
//! **Configuration** (`tests/connectors/http/sink.toml`):
//! ```toml
//! [connectors]
//! config_type = "local"
//! config_dir = "../connectors/sinks/http_sink"
//! ```
//! Environment variables override `config.toml` fields at runtime. Convention:
//! `IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_<FIELD>` (e.g., `..._BATCH_MODE=ndjson`).
//!
//! ## Running Tests
//!
//! ```bash
//! # Prerequisites: Docker running, HTTP sink plugin compiled
//! cargo build -p iggy_connector_http_sink
//!
//! # Run all HTTP sink integration tests
//! cargo test -p integration --test connectors -- http_sink --nocapture
//!
//! # Run a specific test
//! cargo test -p integration --test connectors -- individual_json_messages --nocapture
//!
//! # Run with test isolation (sequential)
//! cargo test -p integration --test connectors -- http_sink --test-threads=1 --nocapture
//! ```
//!
//! ## Success Criteria
//!
//! - **All 4 batch modes**: Messages arrive in correct format (individual, ndjson, json_array, raw)
//! - **Metadata envelope**: Present when `include_metadata=true`, absent when `false`
//! - **Content types**: `application/json` (individual/json_array), `application/x-ndjson`,
//!   `application/octet-stream` (raw)
//! - **Offset ordering**: Sequential, contiguous offsets in metadata
//! - **Multi-topic routing**: `iggy_topic` metadata matches source topic for each message
//! - **Message counts**: Exact match between sent and received message counts
//!
//! ## Related Documentation
//!
//! - **HTTP Sink README**: `core/connectors/sinks/http_sink/README.md` — Config reference,
//!   deployment patterns, retry strategy, connection pooling, message flow
//! - **Connector Runtime**: `runtime/src/sink.rs` — `setup_sink_consumers()`,
//!   `spawn_consume_tasks()`, `consume_messages()`, FFI boundary
//! - **SDK Macro**: `sdk/src/sink.rs` — `sink_connector!` macro, `SinkContainer`, DashMap
//! - **Fixtures**: `tests/connectors/fixtures/http/` — WireMock container, fixture structs
//! - **PR**: https://github.com/apache/iggy/pull/2925
//! - **Discussion**: https://github.com/apache/iggy/discussions/2919
//!
//! ## Known Limitations
//!
//! 1. **FFI return value ignored**: The runtime's `process_messages()` discards `consume()`'s
//!    `i32` return code. Errors are logged by the sink but invisible to the runtime.
//!    See [#2927](https://github.com/apache/iggy/issues/2927).
//! 2. **Offsets committed before processing**: `PollingMessages` auto-commit strategy commits
//!    offsets before `consume()`. Combined with (1), effective guarantee is at-most-once.
//!    See [#2928](https://github.com/apache/iggy/issues/2928).
//!
//! ## Test History
//!
//! - **2026-03-10**: Initial test suite — 6 tests covering all batch modes, metadata toggle,
//!   and sequential offset verification.
//! - **2026-03-11**: Added multi-topic test (Test 7). Initially used shared harness seed
//!   (`connector_multi_topic_stream`) with `TOPIC_2` constant in `seeds.rs`. Removed during
//!   code review remediation — second topic now created inline to keep harness generic.
//! - **2026-03-12**: Code review rounds 3+4 (double-review protocol). Fixed: magic string
//!   match arms replaced with constants (M9), harness pollution removed (H1).

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    HttpSinkIndividualFixture, HttpSinkJsonArrayFixture, HttpSinkMultiTopicFixture,
    HttpSinkNdjsonFixture, HttpSinkNoMetadataFixture, HttpSinkRawFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;

// ============================================================================
// Test 1: Individual Batch Mode
// ============================================================================

/// Test 1: Individual JSON Messages Delivered as Separate HTTP POSTs
///
/// **Purpose**: Validates that `batch_mode=individual` sends one HTTP request per Iggy
/// message, with each request containing the full metadata envelope.
///
/// **Behavior Under Test**:
/// When configured with `batch_mode=individual`, the HTTP sink's `send_individual()` method
/// iterates over each message in the consumed batch and calls `send_with_retry()` for each
/// one independently. The metadata envelope wraps each message with Iggy context:
/// ```json
/// {
///   "metadata": { "iggy_stream": "...", "iggy_topic": "...", "iggy_offset": N, ... },
///   "payload": { ... original message ... }
/// }
/// ```
///
/// **Why This Matters**:
/// Individual mode is the simplest and most compatible delivery pattern. It works with any
/// HTTP endpoint that accepts POST requests — no special parsing logic needed on the receiver.
/// Each message is independently retryable: if message 2 of 5 fails, only message 2 is retried.
/// This is the default batch mode and the most common deployment pattern.
///
/// **Test Flow**:
/// 1. Send 3 JSON messages to Iggy (`test_stream`/`test_topic`, partition 0)
/// 2. Wait for WireMock to receive exactly 3 HTTP requests
/// 3. Verify each request: POST method, `/ingest` URL
/// 4. Verify each body: `metadata` and `payload` fields present
/// 5. Verify metadata: `iggy_stream`, `iggy_topic`, `iggy_offset` fields present
/// 6. Verify content type: `application/json`
///
/// **Key Validations**:
/// - Request count = message count (1:1 mapping)
/// - Metadata envelope structure is correct
/// - Content-Type header is `application/json`
/// - All 3 standard metadata fields present (`iggy_stream`, `iggy_topic`, `iggy_offset`)
///
/// **Related Code**:
/// - `send_individual()` in `sinks/http_sink/src/lib.rs` — per-message delivery loop
/// - `build_envelope()` in `sinks/http_sink/src/lib.rs` — metadata envelope construction
/// - `HttpSinkIndividualFixture` in `fixtures/http/sink.rs` — base fixture with env overrides
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

    // Step 1: Build 3 JSON messages with distinct payloads
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

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for WireMock to receive all 3 individual HTTP requests
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

    // Step 4: Verify each request has correct method, URL, and envelope structure
    for req in &requests {
        assert_eq!(req.method, "POST", "Expected POST method");
        assert_eq!(req.url, "/ingest", "Expected /ingest URL");

        let body = req.body_as_json().expect("Body should be valid JSON");

        // Metadata envelope: {metadata: {...}, payload: {...}}
        assert!(
            body.get("metadata").is_some(),
            "Expected metadata envelope in individual mode, got: {body}"
        );
        assert!(
            body.get("payload").is_some(),
            "Expected payload field in individual mode, got: {body}"
        );

        // Verify standard metadata fields from Iggy context
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

    // Step 5: Verify content type header
    let ct = requests[0]
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/json"),
        "Expected application/json content type, got: {ct}"
    );
}

// ============================================================================
// Test 2: NDJSON Batch Mode
// ============================================================================

/// Test 2: NDJSON Batch Mode — All Messages in One Newline-Delimited Request
///
/// **Purpose**: Validates that `batch_mode=ndjson` combines all messages into a single
/// HTTP request with newline-delimited JSON body (`application/x-ndjson`).
///
/// **Behavior Under Test**:
/// The HTTP sink's `send_ndjson()` method serializes each message as a JSON envelope,
/// joins them with `\n`, and sends the result as a single HTTP request. This mode is
/// optimal for endpoints that accept streaming JSON (e.g., Elasticsearch `_bulk` API,
/// cloud logging services, data lake ingestion). The `send_batch_body()` helper handles
/// the post-send accounting (error counting, skip warnings) shared with `send_json_array`.
///
/// **Why This Matters**:
/// NDJSON reduces HTTP overhead from N requests to 1 request for a batch of N messages.
/// For high-throughput streams (thousands of messages per second), this can reduce
/// connection overhead by orders of magnitude. Individual serialization failures are
/// skipped (with error counting) rather than aborting the entire batch — partial delivery
/// is preferred over total failure.
///
/// **Test Flow**:
/// 1. Send 3 JSON event messages to Iggy
/// 2. Wait for WireMock to receive exactly 1 HTTP request
/// 3. Split response body by newlines — expect 3 lines
/// 4. Parse each line as JSON, verify `metadata` and `payload` fields
/// 5. Verify content type: `application/x-ndjson`
///
/// **Key Validations**:
/// - Single HTTP request (all messages batched)
/// - Line count = message count
/// - Each NDJSON line is valid JSON with metadata envelope
/// - Content-Type is `application/x-ndjson`
///
/// **Related Code**:
/// - `send_ndjson()` in `sinks/http_sink/src/lib.rs` — NDJSON serialization and size check
/// - `send_batch_body()` in `sinks/http_sink/src/lib.rs` — shared batch delivery + accounting
/// - `HttpSinkNdjsonFixture` in `fixtures/http/sink.rs` — overrides `BATCH_MODE=ndjson`
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

    // Step 1: Build 3 JSON event messages
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

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for single NDJSON request (all messages batched into one)
    let requests = fixture
        .container()
        .wait_for_requests(1)
        .await
        .expect("WireMock did not receive NDJSON request");

    let req = &requests[0];
    assert_eq!(req.method, "POST", "Expected POST method");
    assert_eq!(req.url, "/ingest", "Expected /ingest URL");

    // Step 4: Parse NDJSON body — each line is a separate JSON envelope
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

    // Step 5: Verify NDJSON content type
    let ct = req
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/x-ndjson"),
        "Expected application/x-ndjson content type, got: {ct}"
    );
}

// ============================================================================
// Test 3: JSON Array Batch Mode
// ============================================================================

/// Test 3: JSON Array Batch Mode — All Messages as a Single JSON Array
///
/// **Purpose**: Validates that `batch_mode=json_array` combines all messages into a single
/// HTTP request with a JSON array body (`[{envelope1}, {envelope2}, ...]`).
///
/// **Behavior Under Test**:
/// The HTTP sink's `send_json_array()` method builds envelope structs for each message,
/// collects them into a `Vec`, and serializes the entire vector as a JSON array via
/// `serde_json::to_vec()`. Like NDJSON, individual serialization failures are skipped.
/// The whole-batch serialization (the final `to_vec` call) is a separate failure point —
/// if it fails, all successfully-built envelopes are counted as errors.
///
/// **Why This Matters**:
/// JSON array mode is compatible with APIs that expect a standard JSON array (e.g., REST
/// bulk endpoints, webhook aggregators). Unlike NDJSON, the entire body is a single valid
/// JSON document, which simplifies parsing on the receiver side. The trade-off is that the
/// entire body must fit in memory as a single allocation.
///
/// **Test Flow**:
/// 1. Send 3 JSON messages to Iggy (order, payment, refund events)
/// 2. Wait for WireMock to receive exactly 1 HTTP request
/// 3. Parse body as JSON array, verify array length = 3
/// 4. Verify each array item has `metadata` and `payload` fields
/// 5. Verify content type: `application/json`
///
/// **Key Validations**:
/// - Single HTTP request (all messages batched)
/// - Body is a valid JSON array
/// - Array length = message count
/// - Each item has metadata envelope
/// - Content-Type is `application/json`
///
/// **Related Code**:
/// - `send_json_array()` in `sinks/http_sink/src/lib.rs` — array serialization + size check
/// - `send_batch_body()` in `sinks/http_sink/src/lib.rs` — shared batch delivery + accounting
/// - `HttpSinkJsonArrayFixture` in `fixtures/http/sink.rs` — overrides `BATCH_MODE=json_array`
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

    // Step 1: Build 3 JSON messages representing different event types
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

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for single JSON array request (all messages in one body)
    let requests = fixture
        .container()
        .wait_for_requests(1)
        .await
        .expect("WireMock did not receive JSON array request");

    let req = &requests[0];
    assert_eq!(req.method, "POST", "Expected POST method");
    assert_eq!(req.url, "/ingest", "Expected /ingest URL");

    // Step 4: Parse body as JSON array and verify structure
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

    // Step 5: Verify JSON content type
    let ct = req
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/json"),
        "Expected application/json content type, got: {ct}"
    );
}

// ============================================================================
// Test 4: Raw Batch Mode
// ============================================================================

/// Test 4: Raw Binary Messages Delivered Without Metadata Envelope
///
/// **Purpose**: Validates that `batch_mode=raw` sends each message as raw bytes in a
/// separate HTTP request, without the metadata envelope wrapper.
///
/// **Behavior Under Test**:
/// The HTTP sink's `send_raw()` method extracts raw bytes from each message payload
/// via `try_into_vec()` and sends them directly as the HTTP body. No JSON serialization,
/// no metadata envelope — the body is exactly the bytes that were published to Iggy.
/// This mode is intended for binary protocols (Protobuf, FlatBuffers) or when the
/// receiver expects unmodified passthrough.
///
/// **Why This Matters**:
/// Raw mode enables the HTTP sink to forward arbitrary binary data — protocol buffers,
/// Avro records, compressed payloads, or any format the receiver understands. The connector
/// acts as a transparent bridge between Iggy and the HTTP endpoint. The `include_metadata`
/// config is ignored in raw mode (metadata requires JSON serialization which contradicts
/// raw byte passthrough).
///
/// **Test Flow**:
/// 1. Send 3 raw byte messages to Iggy (plain text for verification simplicity)
/// 2. Wait for WireMock to receive exactly 3 HTTP requests (1:1, like individual)
/// 3. Verify each request: POST method, `/ingest` URL
/// 4. Verify body does NOT contain metadata envelope
/// 5. Verify content type: `application/octet-stream`
///
/// **Key Validations**:
/// - Request count = message count (raw is always 1:1)
/// - No metadata envelope in body (raw bytes only)
/// - Content-Type is `application/octet-stream`
///
/// **Related Code**:
/// - `send_raw()` in `sinks/http_sink/src/lib.rs` — raw byte extraction and delivery
/// - `content_type()` in `sinks/http_sink/src/lib.rs` — returns `application/octet-stream` for raw
/// - `HttpSinkRawFixture` in `fixtures/http/sink.rs` — overrides `BATCH_MODE=raw`, `SCHEMA=raw`
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

    // Step 1: Build 3 raw byte messages
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

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for all 3 raw HTTP requests (raw mode is always 1:1)
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

    // Step 4: Verify raw mode — no metadata envelope
    for req in &requests {
        assert_eq!(req.method, "POST", "Expected POST method");
        assert_eq!(req.url, "/ingest", "Expected /ingest URL");

        // Raw mode: body is raw bytes, NOT a JSON envelope.
        // If the body happens to parse as JSON, it must NOT have "metadata" key.
        if let Ok(json) = req.body_as_json() {
            assert!(
                json.get("metadata").is_none(),
                "Raw mode should not include metadata envelope"
            );
        }
    }

    // Step 5: Verify raw content type
    let ct = requests[0]
        .header("Content-Type")
        .expect("Content-Type header must be present");
    assert!(
        ct.contains("application/octet-stream"),
        "Expected application/octet-stream for raw mode, got: {ct}"
    );
}

// ============================================================================
// Test 5: Metadata Disabled
// ============================================================================

/// Test 5: Metadata Disabled — Bare Payload Without Envelope
///
/// **Purpose**: Validates that `include_metadata=false` sends the original message payload
/// directly as the HTTP body, without the `{metadata, payload}` envelope wrapper.
///
/// **Behavior Under Test**:
/// When `include_metadata=false`, the `build_envelope()` method is skipped and the
/// serialized payload JSON is sent directly. For a message containing `{"key": "value1"}`,
/// the HTTP body is exactly `{"key": "value1"}` — not `{"metadata": {...}, "payload": {"key": "value1"}}`.
///
/// **Why This Matters**:
/// Many webhook receivers and REST APIs expect a specific JSON schema and cannot handle
/// unexpected wrapper fields. Disabling metadata allows the HTTP sink to act as a transparent
/// JSON forwarder. This is the correct setting when the receiver already has its own
/// deduplication/ordering mechanism and doesn't need Iggy's stream/topic/offset context.
///
/// **Test Flow**:
/// 1. Send 3 simple JSON messages to Iggy
/// 2. Wait for WireMock to receive all 3 requests
/// 3. Verify each body: NO `metadata` field present
/// 4. Verify each body: original `key` field present at top level
///
/// **Key Validations**:
/// - No `metadata` field in body (envelope disabled)
/// - Original payload fields at top level (not nested under `payload`)
///
/// **Related Code**:
/// - `consume()` in `sinks/http_sink/src/lib.rs` — conditional envelope based on `include_metadata`
/// - `HttpSinkNoMetadataFixture` in `fixtures/http/sink.rs` — overrides `INCLUDE_METADATA=false`
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

    // Step 1: Build 3 simple JSON messages
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

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for WireMock to receive all requests
    let requests = fixture
        .container()
        .wait_for_requests(TEST_MESSAGE_COUNT)
        .await
        .expect("WireMock did not receive requests");

    // Step 4: Verify bare payload — no metadata wrapper
    for (i, req) in requests.iter().enumerate() {
        let body = req
            .body_as_json()
            .unwrap_or_else(|e| panic!("Request {i} body should be valid JSON: {e}"));

        // Without metadata, the body IS the payload — no wrapping
        assert!(
            body.get("metadata").is_none(),
            "Expected no metadata envelope when include_metadata=false, got: {body}"
        );

        // The original payload fields should be at the top level
        assert!(
            body.get("key").is_some(),
            "Expected bare payload with 'key' field, got: {body}"
        );
    }
}

// ============================================================================
// Test 6: Sequential Offset Verification
// ============================================================================

/// Test 6: Individual Messages Have Sequential Contiguous Offsets
///
/// **Purpose**: Validates that `iggy_offset` values in metadata are contiguous (each
/// offset = previous + 1), proving the connector preserves Iggy's message ordering
/// through the entire delivery pipeline.
///
/// **Behavior Under Test**:
/// Each message published to an Iggy topic partition receives a monotonically increasing
/// offset. The HTTP sink includes this offset in the metadata envelope as `iggy_offset`.
/// This test verifies that the sink faithfully reproduces these offsets without gaps,
/// reordering, or duplication — critical for consumers that use offsets for deduplication
/// or ordering guarantees.
///
/// **Why This Matters**:
/// Offset integrity is the foundation for exactly-once processing at the application level.
/// If offsets arrive out of order or with gaps, downstream consumers cannot reliably detect
/// duplicates or missing messages. A broken offset chain could indicate a bug in the
/// connector's message handling, a race condition in multi-topic task scheduling, or a
/// fundamental issue with how the runtime passes messages to the plugin.
///
/// **Test Flow**:
/// 1. Send 5 JSON messages to Iggy (more than default 3 to better validate ordering)
/// 2. Wait for WireMock to receive all 5 requests
/// 3. Extract `iggy_offset` from each request's metadata
/// 4. Sort offsets (delivery order may differ from publish order)
/// 5. Verify offsets are contiguous: each offset = previous + 1
///
/// **Key Validations**:
/// - All 5 messages delivered
/// - Offsets are contiguous (no gaps)
/// - Offsets use sliding window check (`windows(2)`) — works regardless of starting offset
///
/// **Related Code**:
/// - `build_envelope()` in `sinks/http_sink/src/lib.rs` — writes `iggy_offset` from
///   `ConsumedMessage.offset`
/// - Iggy server assigns offsets sequentially per partition
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

    // Step 1: Build 5 messages (more than default 3 to better test ordering)
    let mut messages: Vec<IggyMessage> = (0..5)
        .map(|i| {
            let payload =
                serde_json::to_vec(&serde_json::json!({"idx": i})).expect("Failed to serialize");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
        })
        .collect();

    // Step 2: Publish messages to Iggy
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Step 3: Wait for all 5 requests
    let requests = fixture
        .container()
        .wait_for_requests(5)
        .await
        .expect("WireMock did not receive all 5 requests");

    // Step 4: Extract offsets from metadata
    // Note: offsets may not start at 0 if the seed already published messages.
    let mut offsets: Vec<i64> = requests
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let body = r
                .body_as_json()
                .unwrap_or_else(|e| panic!("Request {i} body is not valid JSON: {e}"));
            body["metadata"]["iggy_offset"].as_i64().unwrap_or_else(|| {
                panic!(
                    "Request {i} missing or non-integer iggy_offset in metadata: {}",
                    body["metadata"]
                )
            })
        })
        .collect();

    // Step 5: Sort and verify contiguous offsets (delivery order may vary)
    offsets.sort();
    assert_eq!(
        offsets.len(),
        5,
        "Expected 5 offsets, got {}",
        offsets.len()
    );

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

// ============================================================================
// Test 7: Multi-Topic Delivery
// ============================================================================

/// Second topic name for the multi-topic test. Defined locally to avoid
/// polluting the shared harness seeds with HTTP-sink-specific constants.
///
/// **Design Decision**: The shared `seeds.rs` module provides generic seed functions
/// (`connector_stream`, `mcp_standard`) used by all connector types. Adding HTTP-sink-specific
/// constants there would create coupling. Instead, this test creates the second topic inline
/// after the seed runs, keeping the harness generic. See code review finding H1.
const TEST_TOPIC_2: &str = seeds::names::TOPIC_2;

/// Test 7: Multi-Topic Messages Delivered with Correct Topic Metadata
///
/// **Purpose**: Validates the multi-topic single-connector deployment pattern — one
/// connector consuming from two topics on the same stream. Messages from each topic
/// must arrive with the correct `iggy_topic` metadata value.
///
/// **Behavior Under Test**:
/// The connector runtime's `setup_sink_consumers()` iterates over `stream.topics` in
/// the config, and `spawn_consume_tasks()` creates one `tokio::spawn` per topic. Each
/// task creates an independent `IggyConsumer` and polls its topic sequentially. All tasks
/// share the same `Client` instance (via `Arc` — connection pool is shared) and the same
/// WireMock endpoint URL. The `iggy_topic` field in the metadata envelope identifies which
/// topic each message originated from.
///
/// **Why This Matters**:
/// Multi-topic subscriptions are a common deployment pattern: a single connector instance
/// consuming events from related topics (e.g., `orders` and `payments` on the same stream)
/// and forwarding them to one HTTP endpoint. The receiver uses `iggy_topic` to route or
/// process messages differently. If topic metadata is incorrect, the receiver cannot
/// distinguish message origins — a data integrity issue.
///
/// This test also exercises the runtime's task spawning and concurrent consumption,
/// verifying that independent topic tasks don't interfere with each other.
///
/// **Test Flow**:
/// 1. Seed creates stream + `test_topic` (via `connector_stream`)
/// 2. Create second topic (`test_topic_2`) inline
/// 3. Send 2 messages to `test_topic` with `{"source": "topic_1"}`
/// 4. Send 1 message to `test_topic_2` with `{"source": "topic_2"}`
/// 5. Wait for WireMock to receive all 3 requests
/// 6. Group requests by `iggy_topic` metadata value
/// 7. Verify: 2 requests from `test_topic`, 1 from `test_topic_2`
/// 8. Verify: payload `source` field matches topic origin
///
/// **Key Validations**:
/// - Total request count = 3 (2 + 1)
/// - `iggy_topic` metadata correctly identifies source topic
/// - Payload content matches expected topic origin
/// - Both topics consumed and delivered independently
///
/// **Configuration**:
/// The `HttpSinkMultiTopicFixture` sets `STREAMS_0_TOPICS=[test_topic,test_topic_2]`
/// in the connector runtime environment. The runtime parses this and spawns one task
/// per topic.
///
/// **Related Code**:
/// - `setup_sink_consumers()` in `runtime/src/sink.rs` — topic iteration
/// - `spawn_consume_tasks()` in `runtime/src/sink.rs` — per-topic task spawning
/// - `build_envelope()` in `sinks/http_sink/src/lib.rs` — writes `iggy_topic` from
///   `TopicMetadata.topic`
/// - `HttpSinkMultiTopicFixture` in `fixtures/http/sink.rs` — two-topic env config
///
/// **Test History**:
/// - **2026-03-11**: Created with shared harness seed (`connector_multi_topic_stream`).
/// - **2026-03-12**: Code review H1 — removed `TOPIC_2` and `connector_multi_topic_stream`
///   from shared `seeds.rs`. Second topic now created inline.
/// - **2026-03-13**: Restored `connector_multi_topic_stream` seed and `names::TOPIC_2` in
///   shared `seeds.rs` — connector runtime health check requires all configured topics to
///   exist before startup (CI failure: 1000 retry timeout).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn multi_topic_messages_delivered_with_correct_topic_metadata(
    harness: &TestHarness,
    fixture: HttpSinkMultiTopicFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_1_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Step 1: Both topics created by connector_multi_topic_stream seed (runs before
    // connector runtime starts — runtime health check requires all configured topics).
    let topic_2_id: Identifier = TEST_TOPIC_2.try_into().unwrap();

    // Step 2: Send 2 messages to topic 1 with source identifier in payload
    let mut topic_1_messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"source": "topic_1", "idx": 0})).unwrap(),
            ))
            .build()
            .unwrap(),
        IggyMessage::builder()
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"source": "topic_1", "idx": 1})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_1_id,
            &Partitioning::partition_id(0),
            &mut topic_1_messages,
        )
        .await
        .expect("Failed to send messages to topic 1");

    // Step 3: Send 1 message to topic 2 with different source identifier
    let mut topic_2_messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"source": "topic_2", "idx": 0})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_2_id,
            &Partitioning::partition_id(0),
            &mut topic_2_messages,
        )
        .await
        .expect("Failed to send messages to topic 2");

    // Step 4: Wait for all 3 messages (2 from topic 1 + 1 from topic 2)
    let requests = fixture
        .container()
        .wait_for_requests(3)
        .await
        .expect("WireMock did not receive all 3 requests");

    // Step 5: Group by iggy_topic metadata and verify counts + payload content
    let mut topic_1_count = 0usize;
    let mut topic_2_count = 0usize;

    for (i, req) in requests.iter().enumerate() {
        let body = req
            .body_as_json()
            .unwrap_or_else(|e| panic!("Request {i} body is not valid JSON: {e}"));

        let iggy_topic = body["metadata"]["iggy_topic"].as_str().unwrap_or_else(|| {
            panic!(
                "Request {i} missing iggy_topic in metadata: {}",
                body["metadata"]
            )
        });

        // Match against constants — not magic strings (code review M9)
        match iggy_topic {
            t if t == seeds::names::TOPIC => {
                topic_1_count += 1;
                let source = body["payload"]["source"]
                    .as_str()
                    .expect("Missing source field");
                assert_eq!(source, "topic_1", "Topic 1 message has wrong source");
            }
            t if t == TEST_TOPIC_2 => {
                topic_2_count += 1;
                let source = body["payload"]["source"]
                    .as_str()
                    .expect("Missing source field");
                assert_eq!(source, "topic_2", "Topic 2 message has wrong source");
            }
            other => panic!("Unexpected iggy_topic value: {other}"),
        }
    }

    // Step 6: Verify exact message counts per topic
    assert_eq!(
        topic_1_count, 2,
        "Expected 2 messages from topic 1, got {topic_1_count}"
    );
    assert_eq!(
        topic_2_count, 1,
        "Expected 1 message from topic 2, got {topic_2_count}"
    );
}
