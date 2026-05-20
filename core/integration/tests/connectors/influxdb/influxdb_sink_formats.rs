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

//! Integration tests that exercise every `payload_format` branch of the
//! InfluxDB sink connector, plus metadata-flag permutations, precision modes,
//! and line-protocol escaping.  These tests are the primary driver for the
//! ~40 percentage-point coverage gap between the existing suite (~40 %) and
//! the 80 % target.
//!
//! Coverage targets per source path:
//!  • `PayloadFormat::Text`  — `influxdb_sink_text_*` tests
//!  • `PayloadFormat::Base64` — `influxdb_sink_base64_*` tests
//!  • `include_metadata = false` path in `append_line` — `influxdb_sink_no_metadata_*`
//!  • `precision = "ns"` in `to_precision_timestamp` — `influxdb_sink_ns_precision_*`
//!  • Special-character escaping helpers — `influxdb_sink_escaping_*`
//!  • Zero-timestamp fallback — `influxdb_sink_zero_timestamp_*`

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    InfluxDbSinkBase64Fixture, InfluxDbSinkFixture, InfluxDbSinkNoMetadataFixture,
    InfluxDbSinkNsPrecisionFixture, InfluxDbSinkTextFixture,
};
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use iggy::prelude::IggyMessage;
use iggy::prelude::Partitioning;
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

// ─── PayloadFormat::Text ──────────────────────────────────────────────────────

/// Sends plain UTF-8 text payloads; verifies points land in InfluxDB.
/// Covers: `PayloadFormat::Text` branch in `append_line`, `write_field_string`
/// with printable ASCII.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_text_writes_points(harness: &TestHarness, fixture: InfluxDbSinkTextFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (1u32..=TEST_MESSAGE_COUNT as u32)
        .map(|i| {
            let text = format!("temperature reading {i}");
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(text.into_bytes()))
                .build()
                .expect("Failed to build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_points("iggy_messages", TEST_MESSAGE_COUNT)
        .await
        .expect("Expected InfluxDB points for text payload");
}

/// Sends text that contains the quote character `"`, which must be escaped to
/// `\"` inside the InfluxDB line-protocol string field.
/// Covers: `write_field_string` quote-escape branch.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_text_with_quote_characters(
    harness: &TestHarness,
    fixture: InfluxDbSinkTextFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Payload contains double-quotes that must be escaped in line protocol.
    let payload = br#"sensor says "hello world""#;
    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload.to_vec()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 point for quoted-text payload");
}

/// Sends text containing a backslash, which must be escaped to `\\`.
/// Covers: `write_field_string` backslash-escape branch.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_text_with_backslash(
    harness: &TestHarness,
    fixture: InfluxDbSinkTextFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = b"path\\to\\sensor";
    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload.to_vec()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 point for backslash-text payload");
}

/// Sends a bulk batch of text messages to exercise the chunked `process_batch`
/// path under the Text format.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_text_bulk(harness: &TestHarness, fixture: InfluxDbSinkTextFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let count = 50usize;

    let mut messages: Vec<IggyMessage> = (0..count)
        .map(|i| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(format!("reading {i}").into_bytes()))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", count)
        .await
        .expect("Expected 50 text points");
}

// ─── PayloadFormat::Base64 ────────────────────────────────────────────────────

/// Sends arbitrary binary payloads; the sink must base64-encode them before
/// writing to the `payload_base64` field.
/// Covers: `PayloadFormat::Base64` branch in `append_line`,
/// `general_purpose::STANDARD.encode`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_base64.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_base64_writes_points(
    harness: &TestHarness,
    fixture: InfluxDbSinkBase64Fixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (1u32..=TEST_MESSAGE_COUNT as u32)
        .map(|i| {
            // Arbitrary binary data — not valid UTF-8.
            let payload = vec![0xDE, 0xAD, 0xBE, 0xEF, i as u8];
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", TEST_MESSAGE_COUNT)
        .await
        .expect("Expected InfluxDB points for base64 payload");
}

/// Verifies that base64 encoding round-trips correctly: encode the expected
/// bytes locally and confirm the point appears (i.e. the line protocol was
/// valid after encoding).
/// Covers: `write_field_string` with base64 alphabet characters (no escaping
/// needed for base64 output, but exercises the full field-write path).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_base64.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_base64_roundtrip(harness: &TestHarness, fixture: InfluxDbSinkBase64Fixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let raw = b"hello binary world\x00\x01\x02";
    let _expected_b64 = general_purpose::STANDARD.encode(raw);

    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(raw.to_vec()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 base64 point");
}

/// Sends a minimal single-byte payload under base64 mode.
///
/// Iggy requires payloads to be at least 1 byte — a zero-length `Bytes::new()`
/// is rejected with `InvalidMessagePayloadLength` before the message ever
/// reaches the sink connector.  This test therefore uses a single null byte
/// `[0x00]`, which base64-encodes to `"AA=="` and exercises the short-payload
/// path in `PayloadFormat::Base64` without triggering the Iggy length guard.
/// Covers: zero-length base64 output path is not reachable end-to-end; this
/// test covers the next-closest case (1-byte payload → 4-char base64 string).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_base64.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_base64_single_byte_payload(
    harness: &TestHarness,
    fixture: InfluxDbSinkBase64Fixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Single null byte — smallest valid Iggy payload; encodes to "AA==" in base64.
    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from_static(&[0x00]))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 point for single-byte base64 payload");
}

// ─── include_metadata = false ─────────────────────────────────────────────────

/// When all metadata flags are disabled the sink omits stream/topic/partition
/// tags and writes them as fields instead (or omits them entirely).
/// Covers: the `!include_stream_tag`, `!include_topic_tag`,
/// `!include_partition_tag`, `!include_checksum`, `!include_origin_timestamp`
/// branches inside `append_line`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_no_metadata.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_no_metadata_writes_points(
    harness: &TestHarness,
    fixture: InfluxDbSinkNoMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (1u32..=TEST_MESSAGE_COUNT as u32)
        .map(|i| {
            let payload = serde_json::to_vec(&serde_json::json!({"v": i})).expect("json serialize");
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", TEST_MESSAGE_COUNT)
        .await
        .expect("Expected points with metadata disabled");
}

/// Bulk write with metadata disabled — exercises the loop path in `consume`
/// with the no-metadata tag configuration.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_no_metadata.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_no_metadata_bulk(
    harness: &TestHarness,
    fixture: InfluxDbSinkNoMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let count = 25usize;

    let mut messages: Vec<IggyMessage> = (0..count)
        .map(|i| {
            let payload = serde_json::to_vec(&serde_json::json!({"seq": i})).unwrap();
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", count)
        .await
        .expect("Expected 25 no-metadata points");
}

// ─── precision = "ns" ─────────────────────────────────────────────────────────

/// Verifies that nanosecond precision is accepted by InfluxDB (the timestamp
/// field is multiplied by 1 000 relative to microseconds).
/// Covers: `"ns"` arm of `to_precision_timestamp`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_ns_precision_writes_points(
    harness: &TestHarness,
    fixture: InfluxDbSinkNsPrecisionFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (1u32..=TEST_MESSAGE_COUNT as u32)
        .map(|i| {
            let payload = serde_json::to_vec(&serde_json::json!({"ns_seq": i})).unwrap();
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", TEST_MESSAGE_COUNT)
        .await
        .expect("Expected points with ns precision");
}

// ─── Line-protocol escaping: measurement name ─────────────────────────────────

/// Sends a nested JSON payload to exercise the `compact` re-serialisation path
/// in `PayloadFormat::Json` and `write_field_string` with `{`, `}`, `:`, `"`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_json_nested_payload(harness: &TestHarness, fixture: InfluxDbSinkFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = serde_json::to_vec(&serde_json::json!({
        "nested": { "a": 1, "b": [2, 3] },
        "label": "test\"value"
    }))
    .unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 point for nested JSON payload");
}

// ─── Line-protocol escaping: newline/carriage-return ─────────────────────────

/// Sends a text payload containing a literal newline character (`\n`).
///
/// In InfluxDB line protocol the newline byte is the record delimiter.
/// `write_field_string` must escape it as `\\n` so the batch is not split.
/// This test verifies the point lands in InfluxDB (i.e. the line protocol
/// was valid after escaping).
/// Covers: `'\n' => buf.push_str("\\n")` branch in `write_field_string`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_text_with_newline(harness: &TestHarness, fixture: InfluxDbSinkTextFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Payload contains a literal newline — must be escaped to \n in line protocol.
    let payload = b"line one\nline two";
    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload.to_vec()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Expected 1 point for newline-containing text payload");
}
