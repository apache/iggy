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
use iggy::prelude::{IggyClient, IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::json;

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    QuestDbSinkFixture, QuestDbSinkPayloadTimestampFixture, QuestDbSinkTypedFixture,
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
