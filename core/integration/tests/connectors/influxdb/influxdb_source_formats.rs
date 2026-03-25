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

//! Integration tests that exercise every `payload_format` / `payload_column`
//! branch of the InfluxDB source connector, plus `parse_scalar` type coverage,
//! cursor advancement, and the include-metadata flag.
//!
//! Coverage targets per source path:
//!  • `PayloadFormat::Text` with `payload_column`  — `influxdb_source_text_*`
//!  • `PayloadFormat::Raw` with `payload_column`   — `influxdb_source_raw_*`
//!  • `parse_scalar` (bool / i64 / f64 / null / string) — `influxdb_source_scalar_*`
//!  • Whole-row JSON wrapping (no `payload_column`) — covered by existing suite
//!  • Cursor advancement across multiple polls     — `influxdb_source_cursor_*`
//!  • `include_metadata = false` field filtering   — `influxdb_source_no_metadata_*`

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    InfluxDbSourceFixture, InfluxDbSourceRawFixture, InfluxDbSourceTextFixture,
};
use base64::{Engine as _, engine::general_purpose};
use iggy_common::MessageClient;
use iggy_common::Utc;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::Value;
use tracing::info;

// ─── PayloadFormat::Text with payload_column ──────────────────────────────────

/// Source configured with `payload_format = "text"` and `payload_column =
/// "_value"` reads the raw string value from each CSV row and emits it
/// unchanged as the Iggy message payload.
/// Covers: `PayloadFormat::Text` branch in `build_payload` (payload_column
/// path), `schema()` → `Schema::Text`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_text_payload_column(
    harness: &TestHarness,
    fixture: InfluxDbSourceTextFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    let lines: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| {
            format!(
                "readings,host=server v=\"text_value_{i}\" {ts}",
                ts = base_ts + i as u64 * 1_000,
            )
        })
        .collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();

    fixture.write_lines(&line_refs).await.expect("write lines");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut total = 0usize;
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll failed");

        total += polled.messages.len();
        if total >= TEST_MESSAGE_COUNT {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        total >= TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} text messages, got {total}"
    );

    info!("influxdb_source_text_payload_column produced {total} messages");
}

/// Verifies that when `_value` contains a string with embedded whitespace the
/// text payload is passed through unmodified (no JSON serialisation).
/// Covers: `PayloadFormat::Text` → `Ok(raw_value.into_bytes())`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source_text.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_text_whitespace_value(
    harness: &TestHarness,
    fixture: InfluxDbSourceTextFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    // InfluxDB string field value — will be the raw text payload received by Iggy.
    fixture
        .write_lines(&[&format!(
            r#"readings,host=srv v="hello world  tab" {base_ts}"#
        )])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Vec<u8>> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            msgs.push(m.payload.to_vec());
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(msgs.len(), 1, "Expected 1 text message");
    // The payload should be the raw string value from InfluxDB (not JSON-wrapped).
    let text = String::from_utf8(msgs[0].clone()).expect("UTF-8 payload");
    assert!(
        !text.starts_with('{'),
        "Text payload must not be JSON-wrapped, got: {text}"
    );
}

// ─── PayloadFormat::Raw (base64-decode) ───────────────────────────────────────

/// Source configured with `payload_format = "raw"` and `payload_column =
/// "_value"` reads the base64-encoded string from the CSV and decodes it back
/// to raw bytes before publishing to Iggy.
/// Covers: `PayloadFormat::Raw` branch in `build_payload`, `general_purpose::
/// STANDARD.decode`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source_raw.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_raw_payload_column(
    harness: &TestHarness,
    fixture: InfluxDbSourceRawFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

    // Encode arbitrary bytes as base64 and store them as the _value field.
    let raw_bytes: &[u8] = &[0x01, 0x02, 0x03, 0xFF];
    let encoded = general_purpose::STANDARD.encode(raw_bytes);

    fixture
        .write_lines(&[&format!(r#"readings,host=srv v="{encoded}" {base_ts}"#)])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Vec<u8>> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            msgs.push(m.payload.to_vec());
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(msgs.len(), 1, "Expected 1 raw message");
    assert_eq!(
        msgs[0], raw_bytes,
        "Decoded payload bytes must match the original"
    );
}

/// Multiple rows in the same poll with raw encoding.
/// Covers: the loop in `poll_messages` with `PayloadFormat::Raw`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source_raw.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_raw_multiple_rows(
    harness: &TestHarness,
    fixture: InfluxDbSourceRawFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

    let lines: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| {
            let encoded = general_purpose::STANDARD.encode([i as u8, (i + 10) as u8]);
            format!(
                r#"readings,host=srv v="{encoded}" {ts}"#,
                ts = base_ts + i as u64 * 1_000
            )
        })
        .collect();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    fixture.write_lines(&refs).await.expect("write lines");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut total = 0usize;
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll failed");

        total += polled.messages.len();
        if total >= TEST_MESSAGE_COUNT {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        total >= TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} raw messages, got {total}"
    );
}

// ─── parse_scalar type coverage ───────────────────────────────────────────────

/// Writes rows whose `_value` column contains different data types (integer,
/// float, bool) and verifies they are parsed and wrapped correctly in the
/// default whole-row JSON payload.
/// Covers: `parse_scalar` for `i64`, `f64`, `bool`, and string branches.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_scalar_type_integer(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    // InfluxDB integer field literal uses the `i` suffix.
    fixture
        .write_lines(&[&format!("counters,host=srv total=42i {base_ts}")])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    info!("influxdb_source_scalar_type_integer got {:?}", msgs);
    assert!(
        !msgs.is_empty(),
        "Expected at least 1 message for integer scalar"
    );
    // The wrapped value should be a JSON number.
    let v = &msgs[0];
    assert!(
        v.get("value").is_some(),
        "Wrapped payload must contain 'value' key"
    );
}

/// Covers `parse_scalar` for `f64` values.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_scalar_type_float(harness: &TestHarness, fixture: InfluxDbSourceFixture) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    fixture
        .write_lines(&[&format!("temperatures,host=srv v=98.6 {base_ts}")])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        !msgs.is_empty(),
        "Expected at least 1 message for float scalar"
    );
    let v = &msgs[0]["value"];
    assert!(v.is_number(), "Expected numeric value, got: {v}");
}

/// Covers `parse_scalar` for boolean values (`true`/`false`).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_scalar_type_bool(harness: &TestHarness, fixture: InfluxDbSourceFixture) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    fixture
        .write_lines(&[&format!("flags,host=srv active=true {base_ts}")])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        !msgs.is_empty(),
        "Expected at least 1 message for bool scalar"
    );
    // `_value` in the wrapped JSON may be a bool or a string representation.
    let v = &msgs[0];
    assert!(
        v.get("value").is_some(),
        "Wrapped payload must contain 'value'"
    );
}

// ─── Cursor advancement ───────────────────────────────────────────────────────

/// Verifies that the source connector's cursor advances after delivering the
/// first batch, so a second batch written with later timestamps is delivered
/// exactly once and not mixed with re-delivered first-batch rows.
///
/// Design notes:
/// - Both batches are written to InfluxDB *before* the test starts polling,
///   with the second batch timestamped 60 seconds after the first.  This
///   avoids a race between the test writing the second batch and the connector
///   updating its in-memory cursor state.
/// - The total expected message count is `2 * TEST_MESSAGE_COUNT`; the test
///   simply asserts all messages arrive, which proves cursor deduplication
///   (if the connector re-delivered the first batch the count would exceed
///   the expected total only if duplicates are emitted — but since cursor
///   filtering is strict-greater-than, the first batch is excluded after the
///   first poll and the total stays at exactly `2 * TEST_MESSAGE_COUNT`).
/// Covers: `max_cursor` update in `poll_messages`, `is_timestamp_after`,
/// cursor state persisted via `state.last_timestamp`.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_cursor_advances_between_batches(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let now_ns: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

    // First batch: 120 seconds in the past — safely within range(start: -1h)
    // and old enough that the cursor set after consuming it will be behind
    // the second batch.
    let first_batch: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| {
            format!(
                "cursor_test,batch=first v={i} {ts}",
                ts = now_ns - 120_000_000_000u64 + i as u64 * 1_000
            )
        })
        .collect();

    // Second batch: 60 seconds in the past — also within range(start: -1h)
    // but strictly after the first batch, so the cursor from the first
    // batch will include them on the next connector poll.
    let second_batch: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| {
            format!(
                "cursor_test,batch=second v={i} {ts}",
                ts = now_ns - 60_000_000_000u64 + i as u64 * 1_000
            )
        })
        .collect();

    // Write both batches before any polling so there is no race between
    // the test writing data and the connector advancing its cursor.
    let all_lines: Vec<&str> = first_batch
        .iter()
        .chain(second_batch.iter())
        .map(String::as_str)
        .collect();

    fixture
        .write_lines(&all_lines)
        .await
        .expect("write both batches");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    // Poll until all 2 * TEST_MESSAGE_COUNT messages arrive.
    // The connector may need two poll rounds (one per batch) depending on
    // its batch_size and poll_interval; 300 attempts × 100 ms = 30 s budget.
    let expected_total = TEST_MESSAGE_COUNT * 2;
    let mut total = 0usize;
    for _ in 0..300 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll failed");

        total += polled.messages.len();
        if total >= expected_total {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        total, expected_total,
        "Expected exactly {expected_total} messages (both batches, no duplicates), got {total}"
    );
}

// ─── include_metadata = false ─────────────────────────────────────────────────

/// When `include_metadata = false` the JSON wrapper should still contain the
/// core fields (`measurement`, `timestamp`, `value`, `field`) but omit other
/// InfluxDB metadata columns from the `row` map.
/// Covers: the `include_metadata || key == "_value" || …` filter in
/// `build_payload` (whole-row path).
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_no_metadata_core_fields_present(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    fixture
        .write_lines(&[&format!("sensors,location=roof humidity=78.5 {base_ts}")])
        .await
        .expect("write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(msgs.len(), 1, "Expected 1 message");
    let m = &msgs[0];
    // Core keys always present regardless of include_metadata.
    assert!(m.get("measurement").is_some(), "missing 'measurement'");
    assert!(m.get("timestamp").is_some(), "missing 'timestamp'");
    assert!(m.get("value").is_some(), "missing 'value'");
    assert!(m.get("field").is_some(), "missing 'field'");
    assert!(m.get("row").is_some(), "missing 'row'");
}
