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

use bytes::Bytes;
use iggy::prelude::{HeaderKey, HeaderKind, HeaderValue, IggyClient, IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    QuestDbSinkFixture, QuestDbSinkHeadersFixture, QuestDbSinkPayloadTimestampFixture,
    QuestDbSinkServerTimestampFixture, QuestDbSinkSmallBatchFixture,
    QuestDbSinkStoreAndForwardFixture, QuestDbSinkTextFixture, QuestDbSinkTypedFixture,
};

fn message(id: u128, payload: serde_json::Value) -> IggyMessage {
    IggyMessage::builder()
        .id(id)
        .payload(Bytes::from(
            serde_json::to_vec(&payload).expect("Failed to serialize"),
        ))
        .build()
        .expect("Failed to build message")
}

/// `TestHarness` is injected into the annotated test bodies by the macro, so
/// this shared helper takes the already-built client instead.
async fn send(client: &IggyClient, mut messages: Vec<IggyMessage>) {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_messages_when_consumed_should_write_rows(
    harness: &TestHarness,
    fixture: QuestDbSinkFixture,
) {
    let messages = (1..=TEST_MESSAGE_COUNT as u32)
        .map(|i| message(i as u128, json!({"sensor_id": i, "temp": 20.0 + i as f64})))
        .collect();
    send(&harness.root_client().await.unwrap(), messages).await;

    fixture
        .wait_for_rows(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to wait for QuestDB rows");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_typed_payload_when_consumed_should_map_questdb_column_types(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(
            1,
            json!({
                "side": "buy",
                "price": 2615.54,
                "amount": 7,
                "active": true,
                "note": "hello",
                "trade_id": "123e4567-e89b-12d3-a456-426614174000",
                "embedding": [1.5, 2.5, 3.5],
                "matrix": [[1.0, 2.0], [3.0, 4.0]],
                "meta": {"venue": "nyse"},
                "missing": null,
            }),
        )],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    assert_eq!(columns.get("side").map(String::as_str), Some("SYMBOL"));
    assert_eq!(columns.get("price").map(String::as_str), Some("DOUBLE"));
    assert_eq!(columns.get("amount").map(String::as_str), Some("LONG"));
    assert_eq!(columns.get("active").map(String::as_str), Some("BOOLEAN"));
    assert_eq!(columns.get("note").map(String::as_str), Some("VARCHAR"));
    assert_eq!(columns.get("trade_id").map(String::as_str), Some("UUID"));
    assert_eq!(
        columns.get("embedding").map(String::as_str),
        Some("DOUBLE[]")
    );
    assert_eq!(
        columns.get("matrix").map(String::as_str),
        Some("DOUBLE[][]")
    );
    // A nested object has no QuestDB type, so it is stored as JSON text.
    assert_eq!(columns.get("meta").map(String::as_str), Some("VARCHAR"));
    // A null field must not create a column at all.
    assert!(
        !columns.contains_key("missing"),
        "null field created a column: {columns:?}"
    );
    // Metadata columns requested by this fixture.
    assert_eq!(
        columns.get("partition_id").map(String::as_str),
        Some("LONG")
    );
    assert_eq!(columns.get("offset").map(String::as_str), Some("LONG"));
    assert_eq!(columns.get("stream").map(String::as_str), Some("SYMBOL"));
    assert_eq!(columns.get("topic").map(String::as_str), Some("SYMBOL"));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_uuid_column_when_consumed_should_round_trip_canonical_form(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    let fixture = &fixture.0;
    // Pins the lo/hi split. Getting it backwards yields a byte-reversed UUID
    // with no error anywhere, so only a round-trip catches it.
    let expected = "123e4567-e89b-12d3-a456-426614174000";
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"side": "buy", "trade_id": expected}))],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let values = fixture.column_values("trade_id").await.expect("no values");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].as_str(), Some(expected));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_nested_numeric_arrays_when_consumed_should_preserve_values(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(
            1,
            json!({"side": "buy", "embedding": [1.5, 2.5, 3.5]}),
        )],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let values = fixture.column_values("embedding").await.expect("no values");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], json!([1.5, 2.5, 3.5]));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_integer_array_when_consumed_should_widen_to_double_array(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    // QuestDB stores only DOUBLE arrays and rejects a LONG[] frame outright,
    // so the sink must widen rather than pass integers through.
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"side": "buy", "embedding": [1, 2, 3]}))],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    assert_eq!(
        columns.get("embedding").map(String::as_str),
        Some("DOUBLE[]")
    );
    let values = fixture.column_values("embedding").await.expect("no values");
    assert_eq!(values[0], json!([1.0, 2.0, 3.0]));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_invalid_record_in_batch_when_consumed_should_write_the_rest(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    // The middle record fails only after its table, symbol and a column are
    // already encoded, which is the case the marker/rewind path exists for.
    // Without it the malformed row would take out the whole batch.
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![
            message(1, json!({"side": "buy", "price": 1.0, "trade_id": "123e4567-e89b-12d3-a456-426614174000"})),
            message(2, json!({"side": "sell", "price": 2.0, "trade_id": "definitely-not-a-uuid"})),
            message(3, json!({"side": "buy", "price": 3.0, "trade_id": "00000000-0000-0000-0000-000000000002"})),
        ],
    )
    .await;

    let count = fixture.wait_for_rows(2).await.expect("no rows");
    assert_eq!(
        count, 2,
        "the malformed record must be the only one dropped"
    );

    let prices = fixture.column_values("price").await.expect("no values");
    let mut prices: Vec<f64> = prices
        .iter()
        .filter_map(serde_json::Value::as_f64)
        .collect();
    prices.sort_by(f64::total_cmp);
    assert_eq!(prices, vec![1.0, 3.0]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_payload_timestamp_source_when_consumed_should_use_the_field(
    harness: &TestHarness,
    fixture: QuestDbSinkPayloadTimestampFixture,
) {
    let fixture = &fixture.0;
    // 2026-09-04T12:00:00Z in microseconds.
    let event_time = 1_788_523_200_000_000i64;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"event_time": event_time, "price": 1.5}))],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    // An auto-created designated timestamp is always named `timestamp`, and
    // the source field must not also appear as a data column.
    assert!(columns.contains_key("timestamp"), "{columns:?}");
    assert!(
        !columns.contains_key("event_time"),
        "timestamp field must not double as a column: {columns:?}"
    );

    let values = fixture.column_values("timestamp").await.expect("no values");
    let rendered = values[0].as_str().expect("timestamp should be a string");
    assert!(
        rendered.starts_with("2026-09-04T12:00:00"),
        "unexpected timestamp: {rendered}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_messages_when_consumed_should_write_every_row(
    harness: &TestHarness,
    fixture: QuestDbSinkFixture,
) {
    let bulk_count = 500usize;
    let messages = (0..bulk_count)
        .map(|i| message((i + 1) as u128, json!({"seq": i, "value": i as f64 * 1.5})))
        .collect();
    send(&harness.root_client().await.unwrap(), messages).await;

    let count = fixture
        .wait_for_rows(bulk_count)
        .await
        .expect("Failed to wait for QuestDB rows");
    assert!(count >= bulk_count, "expected {bulk_count}, got {count}");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_store_and_forward_when_consumed_should_persist_a_slot_and_write_rows(
    harness: &TestHarness,
    fixture: QuestDbSinkStoreAndForwardFixture,
) {
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        (1..=5u32)
            .map(|i| message(i as u128, json!({"seq": i})))
            .collect(),
    )
    .await;

    fixture.wait_for_rows(5).await.expect("no rows");

    // The client persists frames before sending, so a slot directory must
    // exist on disk rather than the batch living only in memory.
    let slot = fixture.wait_for_sf_slot().await.expect("no sf slot");
    assert!(
        slot.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("iggy_test")),
        "unexpected slot name: {slot:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_questdb_outage_when_consumed_should_buffer_and_replay_on_recovery(
    harness: &TestHarness,
    fixture: QuestDbSinkStoreAndForwardFixture,
) {
    let fixture = &fixture.0;
    let client = harness.root_client().await.unwrap();

    // Land a first batch so the table exists and the slot is established.
    send(
        &client,
        (1..=5u32)
            .map(|i| message(i as u128, json!({"seq": i})))
            .collect(),
    )
    .await;
    fixture.wait_for_rows(5).await.expect("first batch missing");
    let slot = fixture.wait_for_sf_slot().await.expect("no sf slot");

    // Freeze QuestDB. The sink keeps accepting batches from the runtime but
    // cannot deliver them.
    fixture.simulate_outage().await.expect("failed to pause");

    // The runtime has already committed the Apache Iggy offset for these, so
    // disk is the only thing standing between an outage and data loss.
    send(
        &client,
        (6..=15u32)
            .map(|i| message(i as u128, json!({"seq": i})))
            .collect(),
    )
    .await;

    // Give the sink time to poll, attempt delivery and park the frames. Without
    // this the pause could be over before it ever tried, leaving the test
    // asserting nothing.
    sleep(Duration::from_secs(3)).await;
    let segments = QuestDbSinkFixture::sf_segments(&slot);
    assert!(
        !segments.is_empty(),
        "no store-and-forward segments on disk during the outage: {slot:?}"
    );

    fixture.resume().await.expect("failed to unpause");

    // Everything sent during the outage must arrive once the server is back.
    let count = fixture
        .wait_for_rows(15)
        .await
        .expect("rows buffered during the outage were not replayed");
    assert!(count >= 15, "expected 15 rows after replay, got {count}");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_batch_size_below_message_count_when_consumed_should_write_every_chunk(
    harness: &TestHarness,
    fixture: QuestDbSinkSmallBatchFixture,
) {
    // batch_size is 7, so `consume` must drain in several chunks and the
    // final partial chunk must not be dropped.
    let fixture = &fixture.0;
    let total = 100usize;
    send(
        &harness.root_client().await.unwrap(),
        (0..total)
            .map(|i| message((i + 1) as u128, json!({"seq": i})))
            .collect(),
    )
    .await;

    let count = fixture.wait_for_rows(total).await.expect("no rows");
    assert_eq!(count, total, "chunking lost or duplicated rows");

    // Offsets must be contiguous: an off-by-one in the drain loop would show
    // up as a gap rather than a count mismatch.
    let offsets = fixture.column_values("offset").await.expect("no offsets");
    let mut offsets: Vec<i64> = offsets
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect();
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), total, "offsets are not unique: {offsets:?}");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_server_timestamp_source_when_consumed_should_let_questdb_stamp_rows(
    harness: &TestHarness,
    fixture: QuestDbSinkServerTimestampFixture,
) {
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"seq": 1}))],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    assert!(columns.contains_key("timestamp"), "{columns:?}");
    let values = fixture.column_values("timestamp").await.expect("no values");
    assert!(
        values[0].as_str().is_some_and(|ts| ts.starts_with("20")),
        "server did not stamp the row: {values:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_text_schema_when_consumed_should_store_body_in_payload_column(
    harness: &TestHarness,
    fixture: QuestDbSinkTextFixture,
) {
    // A text payload has no field structure, so it lands whole in `payload`
    // rather than being dropped for not being a JSON object.
    let fixture = &fixture.0;
    let body = "plain text body";
    send(
        &harness.root_client().await.unwrap(),
        vec![
            IggyMessage::builder()
                .id(1)
                .payload(Bytes::from(body.as_bytes().to_vec()))
                .build()
                .expect("Failed to build message"),
        ],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    assert_eq!(columns.get("payload").map(String::as_str), Some("VARCHAR"));
    let values = fixture.column_values("payload").await.expect("no values");
    assert_eq!(values[0].as_str(), Some(body));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_headers_enabled_when_consumed_should_write_prefixed_columns(
    harness: &TestHarness,
    fixture: QuestDbSinkHeadersFixture,
) {
    let fixture = &fixture.0;
    let mut headers = std::collections::BTreeMap::new();
    headers.insert(
        HeaderKey::from_raw(HeaderKind::String, b"source").unwrap(),
        HeaderValue::from_raw(HeaderKind::String, b"gateway").unwrap(),
    );
    send(
        &harness.root_client().await.unwrap(),
        vec![
            IggyMessage::builder()
                .id(1)
                .payload(Bytes::from(serde_json::to_vec(&json!({"seq": 1})).unwrap()))
                .user_headers(headers)
                .build()
                .expect("Failed to build message"),
        ],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");

    let columns = fixture.column_types().await.expect("no columns");
    assert_eq!(
        columns.get("header_source").map(String::as_str),
        Some("VARCHAR"),
        "{columns:?}"
    );
    let values = fixture
        .column_values("header_source")
        .await
        .expect("no values");
    assert_eq!(values[0].as_str(), Some("gateway"));
}
