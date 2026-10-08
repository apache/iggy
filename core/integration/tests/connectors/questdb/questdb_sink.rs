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
use iggy_common::{Identifier, IggyTimestamp, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::fixtures::{
    QuestDbOps, QuestDbSinkCoercionFixture, QuestDbSinkDedupFixture, QuestDbSinkFixture,
    QuestDbSinkHeadersFixture, QuestDbSinkPayloadTimestampFixture, QuestDbSinkRawFixture,
    QuestDbSinkServerTimestampFixture, QuestDbSinkSmallBatchFixture,
    QuestDbSinkStoreAndForwardFixture, QuestDbSinkTextFixture, QuestDbSinkTypedFixture,
    QuestDbSinkTypedNumbersFixture,
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
    // `amount` is in integer_columns, so it stays LONG while every other number
    // takes the default DOUBLE.
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
    // The middle record carries a malformed UUID, which validation refuses
    // before anything is written, so its siblings are unaffected.
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

    // The README calls the log line the only trace of a rejected record, so the
    // line has to carry what it takes to find that record again.
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available");
    let (stdout, stderr) = runtime.collect_logs();
    let logs = format!("{stdout}\n{stderr}");
    let rejection = logs
        .lines()
        .find(|line| line.contains("rejected message"))
        .expect("no rejection was logged");
    assert!(
        rejection.contains("offset: 1") && rejection.contains("message_id: 2"),
        "the rejection must name the record: {rejection}"
    );
    assert!(
        rejection.contains("not a valid UUID"),
        "the rejection must give the reason: {rejection}"
    );
    // The fixture turns the payload preview on, so the line carries it too.
    assert!(
        rejection.contains("payload:"),
        "log_rejected_payload is on, so the preview must appear: {rejection}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_the_default_when_consumed_should_write_both_mixed_records(
    harness: &TestHarness,
    fixture: QuestDbSinkFixture,
) {
    // JSON writes 20.0 as 20, so a field arriving whole and fractional is
    // ordinary data. The default has to take both against a real server, not
    // only in a unit test, and the column has to end up a DOUBLE.
    send(
        &harness.root_client().await.unwrap(),
        vec![
            message(1, json!({"sensor_id": 1, "temp": 20})),
            message(2, json!({"sensor_id": 2, "temp": 20.5})),
        ],
    )
    .await;

    let count = fixture.wait_for_rows(2).await.expect("no rows");
    assert_eq!(count, 2, "both records must survive the mixed number types");

    let columns = fixture.column_types().await.expect("no columns");
    assert_eq!(
        columns.get("temp").map(String::as_str),
        Some("DOUBLE"),
        "the whole-number record must not pin the column to LONG: {columns:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_a_declared_table_when_consumed_should_coerce_both_number_directions(
    harness: &TestHarness,
    fixture: QuestDbSinkCoercionFixture,
) {
    // The README's claim that `numbers_as_double` is safe below 2^53 rests on
    // QuestDB coercing an incoming value to the column's declared type in both
    // directions. The fixture declares the columns opposite to what the
    // connector sends, so one record tests both, and `big` pins the loss above
    // 2^53 that the README documents as the reason `integer_columns` exists.
    let fixture = &fixture.0;
    let past_precision = 9_007_199_254_740_993i64;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(
            1,
            json!({"measured": 7, "counted": 42, "big": past_precision}),
        )],
    )
    .await;

    let count = fixture.wait_for_rows(1).await.expect("no rows");
    assert_eq!(count, 1, "a declared table must accept the record");

    // `measured` is in integer_columns, so the connector sent a LONG into a
    // DOUBLE column: widening.
    let measured = fixture.column_values("measured").await.expect("no values");
    assert_eq!(
        measured
            .iter()
            .filter_map(serde_json::Value::as_f64)
            .collect::<Vec<_>>(),
        vec![7.0],
        "a LONG into a DOUBLE column must widen"
    );

    // `counted` took the default, so the connector sent a DOUBLE into a LONG
    // column: narrowing. This is the direction the README depends on and the
    // one that was never verified.
    let counted = fixture.column_values("counted").await.expect("no values");
    assert_eq!(
        counted
            .iter()
            .filter_map(serde_json::Value::as_i64)
            .collect::<Vec<_>>(),
        vec![42],
        "a whole DOUBLE into a LONG column must coerce back to the integer"
    );

    // `big` also took the default, so it was rounded in the connector before the
    // frame existed. The declared LONG column cannot recover the lost bit, which
    // is exactly why a column past 2^53 has to be declared in integer_columns.
    let big = fixture.column_values("big").await.expect("no values");
    let stored = big
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect::<Vec<_>>();
    assert_eq!(
        stored,
        vec![past_precision - 1],
        "a value past 2^53 is rounded by the f64 cast, and the column type cannot undo it"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_integer_columns_when_consumed_should_keep_the_declared_column_exact(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedFixture,
) {
    // A DOUBLE holds integers exactly only below 2^53, which is the cost of the
    // default, so a value past it has to come back byte for byte from the
    // declared column.
    let fixture = &fixture.0;
    let exact = 9_007_199_254_740_993i64;
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"amount": exact, "price": 1.5}))],
    )
    .await;

    let count = fixture.wait_for_rows(1).await.expect("no rows");
    assert_eq!(count, 1);

    let amounts = fixture.column_values("amount").await.expect("no values");
    assert_eq!(
        amounts
            .iter()
            .filter_map(serde_json::Value::as_i64)
            .collect::<Vec<_>>(),
        vec![exact],
        "the declared column must survive the round trip exactly"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_many_conflicting_records_when_consumed_should_write_every_other_one(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedNumbersFixture,
) {
    let fixture = &fixture.0;
    // JSON has one number type, so a producer alternating whole and fractional
    // values for one field makes every second record disagree with the column
    // type the first one pinned. The client refuses those records and rolls each
    // one back, so the sink keeps the buffer and loses only them. An earlier
    // version counted recoveries and failed the whole batch past a cap, which
    // dropped the rows already buffered and every later chunk.
    let total = 24u32;
    let mut messages = Vec::new();
    for i in 0..total {
        let temp = if i % 2 == 0 { json!(20) } else { json!(20.5) };
        messages.push(message(
            (i + 1) as u128,
            json!({"sensor_id": i, "temp": temp}),
        ));
    }
    send(&harness.root_client().await.unwrap(), messages).await;

    // The records sharing the pinned type are exactly the even ones, so the
    // count is an equality rather than a floor, and every one of the twelve has
    // to arrive. A cap that failed the batch would leave far fewer.
    let expected = (total / 2) as usize;
    let count = fixture.wait_for_rows(expected).await.expect("no rows");
    assert_eq!(
        count, expected,
        "every record matching the pinned column type must survive"
    );

    let ids = fixture.column_values("sensor_id").await.expect("no values");
    let mut ids: Vec<i64> = ids.iter().filter_map(serde_json::Value::as_i64).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..total as i64).filter(|i| i % 2 == 0).collect::<Vec<_>>(),
        "the surviving records must be exactly the ones that agree on the type"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_rejected_record_before_a_conflicting_one_when_consumed_should_write_the_rest(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedNumbersFixture,
) {
    // Two records in one batch are refused for different reasons: one by this
    // mapping before anything is written, one by the client after it rolled the
    // row back. Both must cost one record, and the records around them must
    // still be written, which is what this pins. `numbers_as_double` is off, so
    // the mixed numbers still conflict.
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![
            message(1, json!({"sensor_id": 1, "temp": 20.5})),
            message(2, json!({"bad.name": 1})),
            message(3, json!({"sensor_id": 3, "temp": 21})),
            message(4, json!({"sensor_id": 4, "temp": 22.5})),
            message(5, json!({"sensor_id": 5, "temp": 23.5})),
        ],
    )
    .await;

    let count = fixture.wait_for_rows(3).await.expect("no rows");
    assert_eq!(
        count, 3,
        "only the rejected and the conflicting record should be dropped"
    );

    let ids = fixture.column_values("sensor_id").await.expect("no values");
    let mut ids: Vec<i64> = ids.iter().filter_map(serde_json::Value::as_i64).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 4, 5],
        "the records around the two dropped ones must survive"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_record_whose_column_type_conflicts_when_consumed_should_write_the_rest(
    harness: &TestHarness,
    fixture: QuestDbSinkTypedNumbersFixture,
) {
    // QuestDB pins one type per column for the lifetime of a buffer, so a
    // record disagreeing with a column an earlier record defined is refused by
    // the client rather than by validation: nothing about the record alone says
    // it conflicts. The client rolls the half-written row back itself, so the
    // buffer stays usable and only the offending record is lost. This is the
    // `numbers_as_double = false` path, which is what makes mixed numbers reach
    // it.
    let fixture = &fixture.0;
    send(
        &harness.root_client().await.unwrap(),
        vec![
            message(1, json!({"sensor_id": 1, "temp": 20.5})),
            message(2, json!({"sensor_id": 2, "temp": 21})),
            message(3, json!({"sensor_id": 3, "temp": 22.5})),
            message(4, json!({"sensor_id": 4, "temp": 23.5})),
        ],
    )
    .await;

    let count = fixture.wait_for_rows(3).await.expect("no rows");
    assert_eq!(
        count, 3,
        "only the conflicting record should be dropped, not the batch"
    );

    let ids = fixture.column_values("sensor_id").await.expect("no values");
    let mut ids: Vec<i64> = ids.iter().filter_map(serde_json::Value::as_i64).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 3, 4],
        "the records either side of the conflicting one must survive"
    );
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
    assert_eq!(
        count, bulk_count,
        "expected exactly {bulk_count}, got {count}"
    );
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
    // The slot directory existing is the claim: the client persisted frames
    // before sending them. Its name is the `sender_id` the fixture itself wrote
    // into the connect string, so asserting that would prove nothing.
    fixture.wait_for_sf_slot().await.expect("no sf slot");
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

    // Wait for the frames to be parked rather than for a fixed duration: the
    // sink has to poll, attempt delivery and fail before anything reaches disk,
    // and a loaded machine can take longer than any constant worth hard-coding.
    let mut parked = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        parked = QuestDbSinkFixture::sf_segments(&slot);
        if !parked.is_empty() {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert!(
        !parked.is_empty(),
        "no store-and-forward segments on disk during the outage: {slot:?}"
    );

    fixture.resume().await.expect("failed to unpause");

    // Everything sent during the outage must arrive once the server is back.
    // `wait_for_rows` only returns once the count is reached, so the assertion
    // below is about the exact total rather than the wait succeeding.
    let count = fixture
        .wait_for_rows(15)
        .await
        .expect("rows buffered during the outage were not replayed");
    assert_eq!(
        count, 15,
        "expected exactly 15 rows after replay, got {count}"
    );
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
async fn given_many_rejections_across_chunks_when_consumed_should_write_every_valid_record(
    harness: &TestHarness,
    fixture: QuestDbSinkSmallBatchFixture,
) {
    // batch_size is 7, so the rejections are spread over several chunks. Every
    // odd record claims one column twice, which QuestDB resolves as one name, so
    // validation refuses it. The log cap is covered by a unit test, because how
    // many rejections share one runtime batch depends on poll timing.
    let fixture = &fixture.0;
    let total = 60usize;
    let rejected = total / 2;
    send(
        &harness.root_client().await.unwrap(),
        (0..total)
            .map(|i| {
                let payload = if i % 2 == 0 {
                    json!({"seq": i})
                } else {
                    json!({"seq": i, "SEQ": i})
                };
                message((i + 1) as u128, payload)
            })
            .collect(),
    )
    .await;

    let expected = total - rejected;
    let count = fixture.wait_for_rows(expected).await.expect("no rows");
    assert_eq!(count, expected, "every valid record must survive");

    // The surviving rows must be exactly the valid ones, so a rejection cannot
    // be quietly taking a neighbour with it. `seq` is a DOUBLE under the
    // default, so it comes back as a float.
    let seqs = fixture.column_values("seq").await.expect("no values");
    let mut seqs: Vec<f64> = seqs.iter().filter_map(serde_json::Value::as_f64).collect();
    seqs.sort_by(f64::total_cmp);
    assert_eq!(
        seqs,
        (0..total)
            .filter(|i| i % 2 == 0)
            .map(|i| i as f64)
            .collect::<Vec<_>>()
    );

    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available");
    let (stdout, stderr) = runtime.collect_logs();
    let logs = format!("{stdout}\n{stderr}");
    // However the batches fell, no rejection may go unreported, and the count
    // the connector reports has to match what actually went missing.
    let reported: usize = logs
        .lines()
        .filter_map(|line| {
            line.split_once("rejected ")
                .and_then(|(_, rest)| rest.split_once(" of "))
                .and_then(|(count, _)| count.parse::<usize>().ok())
        })
        .sum();
    assert_eq!(
        reported, rejected,
        "every rejected record must be reported once: {logs}"
    );
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
    // Bound the stored timestamp to the window around the send. This catches a
    // regression to the Unix epoch, to a wrong unit, or to a payload field, all
    // of which land far outside the window. It cannot tell the server clock
    // apart from the message or origin timestamp, because every one of those is
    // also "about now" for a message sent here.
    let before = IggyTimestamp::now().to_utc_string("%Y-%m-%dT%H:%M:%S");
    send(
        &harness.root_client().await.unwrap(),
        vec![message(1, json!({"seq": 1}))],
    )
    .await;

    fixture.wait_for_rows(1).await.expect("no rows");
    let after = IggyTimestamp::now().to_utc_string("%Y-%m-%dT%H:%M:%S");

    let columns = fixture.column_types().await.expect("no columns");
    assert!(columns.contains_key("timestamp"), "{columns:?}");
    let values = fixture.column_values("timestamp").await.expect("no values");
    let stored = values[0].as_str().expect("timestamp is not a string");
    // ISO-8601 in a fixed layout sorts lexicographically, so comparing the
    // second-precision prefix orders the three instants correctly.
    let stored_seconds: String = stored.chars().take(before.len()).collect();
    assert!(
        stored_seconds.as_str() >= before.as_str() && stored_seconds.as_str() <= after.as_str(),
        "stored {stored} is outside the send window {before}..{after}"
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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_raw_schema_when_consumed_should_store_bytes_in_payload_column(
    harness: &TestHarness,
    fixture: QuestDbSinkRawFixture,
) {
    // A raw payload has no field structure, so it lands whole in `payload`
    // rather than being rejected for not being a JSON object.
    let fixture = &fixture.0;
    let body = "raw binary-ish body";
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
async fn given_dedup_table_when_duplicates_delivered_should_collapse_to_one_row(
    harness: &TestHarness,
    fixture: QuestDbSinkDedupFixture,
) {
    // Delivery is at-least-once, and the README tells operators to declare
    // DEDUP UPSERT KEYS. This proves that recipe actually collapses a replay:
    // the same logical rows are delivered twice as distinct Apache Iggy
    // messages, and the table must still hold one row per key.
    let fixture = &fixture.0;
    let client = harness.root_client().await.unwrap();
    let rows = |revision: i64| -> Vec<serde_json::Value> {
        (0..10u32)
            .map(|i| {
                json!({
                    "event_time": 1_788_523_200_000_000i64 + i as i64,
                    "seq": i,
                    "revision": revision,
                })
            })
            .collect()
    };

    send(
        &client,
        rows(1)
            .iter()
            .enumerate()
            .map(|(i, row)| message((i + 1) as u128, row.clone()))
            .collect(),
    )
    .await;
    fixture.wait_for_rows(10).await.expect("first delivery");

    // Same keys with a higher revision, as fresh Apache Iggy messages: a replay
    // from the sink's perspective.
    send(
        &client,
        rows(2)
            .iter()
            .enumerate()
            .map(|(i, row)| message((i + 100) as u128, row.clone()))
            .collect(),
    )
    .await;

    // Wait for the replay to be observable rather than for a fixed duration. A
    // row count alone cannot tell "the duplicates collapsed" apart from "the
    // duplicates never arrived", so wait until every row carries the second
    // revision, which only the replay can produce.
    let mut upserted = false;
    for _ in 0..POLL_ATTEMPTS {
        let revisions = fixture.column_values("revision").await.expect("no values");
        if revisions.len() == 10 && revisions.iter().all(|value| value.as_i64() == Some(2)) {
            upserted = true;
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert!(upserted, "the replayed rows never reached the table");

    let count = fixture
        .count_rows(&fixture.table())
        .await
        .expect("count failed")
        .expect("table missing");
    assert_eq!(count, 10, "DEDUP did not collapse the replayed rows");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/questdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_large_payloads_when_consumed_should_write_every_row(
    harness: &TestHarness,
    fixture: QuestDbSinkFixture,
) {
    // Roughly 8 MiB across the batch, well past the client's per-frame target,
    // so the publication path has to span several frames without losing rows.
    let blob = "x".repeat(64 * 1024);
    let count = 128usize;
    send(
        &harness.root_client().await.unwrap(),
        (0..count)
            .map(|i| message((i + 1) as u128, json!({"seq": i, "blob": blob})))
            .collect(),
    )
    .await;

    let written = fixture.wait_for_rows(count).await.expect("no rows");
    assert_eq!(written, count, "large payloads lost rows");
}
