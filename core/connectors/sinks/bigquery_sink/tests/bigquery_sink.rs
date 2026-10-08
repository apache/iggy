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

//! End-to-end tests of the sink against the in-process fake BigQuery.

mod common;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use common::{AppendScript, FakeBigQuery};
use iggy_connector_bigquery_sink::{BigQuerySink, BigQuerySinkConfig};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Schema, Sink, TopicMetadata,
};
use tonic::Code;

const EVENTS_TABLE: &str = r#"
    {"name":"user_id","type":"INT64","mode":"REQUIRED"},
    {"name":"event","type":"STRING"},
    {"name":"created","type":"TIMESTAMP","mode":"REQUIRED","defaultValueExpression":"CURRENT_TIMESTAMP()"},
    {"name":"iggy_stream","type":"STRING"},
    {"name":"iggy_topic","type":"STRING"},
    {"name":"iggy_partition_id","type":"INT64"},
    {"name":"iggy_offset","type":"INT64"},
    {"name":"iggy_timestamp","type":"TIMESTAMP"},
    {"name":"iggy_id","type":"STRING"}"#;

fn config(fake: &FakeBigQuery, extra: &str) -> BigQuerySinkConfig {
    toml::from_str(&format!(
        r#"
        project_id = "proj"
        dataset = "ds"
        table = "events"
        emulator_endpoint = "{}"
        emulator_grpc_endpoint = "{}"
        retry_delay = "5ms"
        max_retry_delay = "20ms"
        timeout = "500ms"
        {extra}
        "#,
        fake.rest_url, fake.grpc_addr
    ))
    .expect("valid test config")
}

async fn open_sink(fake: &FakeBigQuery, extra: &str) -> BigQuerySink {
    let mut sink = BigQuerySink::new(1, config(fake, extra));
    sink.open().await.expect("sink should open");
    sink
}

fn topic() -> TopicMetadata {
    TopicMetadata {
        stream: "shop".into(),
        topic: "events".into(),
    }
}

fn metadata() -> MessagesMetadata {
    MessagesMetadata {
        partition_id: 2,
        current_offset: 99,
        schema: Schema::Json,
    }
}

fn json_message(offset: u64, text: &str) -> ConsumedMessage {
    let mut bytes = text.as_bytes().to_vec();
    ConsumedMessage {
        id: u128::from(offset),
        offset,
        checksum: 0,
        timestamp: 1_700_000_000_000_000,
        origin_timestamp: 0,
        headers: None,
        payload: Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
    }
}

fn events(offsets: std::ops::Range<u64>) -> Vec<ConsumedMessage> {
    offsets
        .map(|offset| {
            json_message(
                offset,
                &format!(r#"{{"user_id": {offset}, "event": "e{offset}"}}"#),
            )
        })
        .collect()
}

fn offsets(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("iggy_offset")
                .expect("iggy_offset column")
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn given_json_messages_should_append_rows_with_metadata() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), events(0..3))
        .await
        .expect("consume should succeed");

    let appends = fake.appends();
    assert_eq!(appends.len(), 1);
    let append = &appends[0];
    assert_eq!(
        append.write_stream,
        "projects/proj/datasets/ds/tables/events/streams/_default"
    );
    assert_eq!(append.default_missing_value_interpretation, 2);

    let batch = &append.batch;
    let names: Vec<&str> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(!names.contains(&"created"), "unset column must be omitted");
    assert_eq!(
        batch
            .column_by_name("user_id")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values(),
        &[0, 1, 2]
    );
    assert_eq!(
        batch
            .column_by_name("iggy_partition_id")
            .unwrap()
            .as_primitive::<Int64Type>()
            .value(0),
        2
    );
    assert_eq!(
        batch
            .column_by_name("iggy_stream")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "shop"
    );
    assert_eq!(offsets(&fake.stored()), vec![0, 1, 2]);
}

#[tokio::test]
async fn given_null_missing_value_should_send_null_interpretation() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, r#"missing_value = "null""#).await;
    sink.consume(&topic(), metadata(), events(0..1))
        .await
        .unwrap();
    assert_eq!(fake.appends()[0].default_missing_value_interpretation, 1);
}

#[tokio::test]
async fn given_batch_over_request_budget_should_split_into_several_appends() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "max_request_bytes = 65536").await;
    let messages = (0..60)
        .map(|offset| {
            json_message(
                offset,
                &format!(
                    r#"{{"user_id": {offset}, "event": "{}"}}"#,
                    "x".repeat(4096)
                ),
            )
        })
        .collect();

    sink.consume(&topic(), metadata(), messages).await.unwrap();

    assert!(fake.appends().len() > 1);
    assert_eq!(offsets(&fake.stored()), (0..60).collect::<Vec<_>>());
}

#[tokio::test]
async fn given_invalid_rows_should_drop_them_and_write_the_rest() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "").await;
    let messages = vec![
        json_message(0, r#"{"user_id": 0}"#),
        json_message(1, r#"{"event": "no user"}"#),
        json_message(2, r#"{"user_id": "abc"}"#),
        json_message(3, r#"{"user_id": 3}"#),
    ];

    sink.consume(&topic(), metadata(), messages)
        .await
        .expect("rejected rows do not fail the batch");

    assert_eq!(offsets(&fake.stored()), vec![0, 3]);
}

#[tokio::test]
async fn given_row_errors_should_reappend_without_rejected_rows() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::RowErrors(vec![1, 3])]);
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), events(10..15))
        .await
        .expect("row errors are handled by dropping rows");

    let appends = fake.appends();
    assert_eq!(appends.len(), 2);
    assert!(!appends[0].stored);
    assert_eq!(offsets(&fake.stored()), vec![10, 12, 14]);
}

#[tokio::test]
async fn given_duplicate_row_errors_should_drop_each_row_once() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::RowErrors(vec![1, 1])]);
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), events(10..13))
        .await
        .expect("duplicate indexes should be deduplicated");

    assert_eq!(offsets(&fake.stored()), vec![10, 12]);
}

#[tokio::test]
async fn given_out_of_range_row_error_should_fail_the_chunk() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::RowErrors(vec![10])]);
    let sink = open_sink(&fake, "").await;

    let result = sink.consume(&topic(), metadata(), events(0..2)).await;

    assert!(
        matches!(result, Err(Error::PermanentHttpError(_))),
        "{result:?}"
    );
    assert!(fake.stored().is_empty());
}

#[tokio::test]
async fn given_all_rows_rejected_by_bigquery_should_fail_the_batch() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::RowErrors(vec![0, 1])]);
    let sink = open_sink(&fake, "").await;

    let result = sink.consume(&topic(), metadata(), events(0..2)).await;

    assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    assert!(fake.stored().is_empty());
}

#[tokio::test]
async fn given_row_errors_twice_should_fail_the_chunk() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[
        AppendScript::RowErrors(vec![0]),
        AppendScript::RowErrors(vec![0]),
    ]);
    let sink = open_sink(&fake, "").await;

    let result = sink.consume(&topic(), metadata(), events(0..3)).await;

    assert!(
        matches!(result, Err(Error::PermanentHttpError(_))),
        "{result:?}"
    );
    assert!(fake.stored().is_empty());
}

#[tokio::test]
async fn given_transient_failure_should_retry_and_succeed() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[
        AppendScript::CallError(Code::Unavailable),
        AppendScript::ResponseError(Code::ResourceExhausted),
    ]);
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), events(0..2))
        .await
        .expect("should succeed on the third attempt");

    assert_eq!(offsets(&fake.stored()), vec![0, 1]);
}

#[tokio::test]
async fn given_cancelled_timeout_should_retry_and_succeed() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::CallError(Code::Cancelled), AppendScript::Ok]);
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), events(0..2))
        .await
        .expect("cancelled client timeout should be retried");

    assert_eq!(offsets(&fake.stored()), vec![0, 1]);
    assert_eq!(fake.pending_script(), 0);
}

#[tokio::test]
async fn given_unknown_and_unauthenticated_failures_should_retry() {
    for code in [Code::Unknown, Code::Unauthenticated] {
        let fake = FakeBigQuery::start(EVENTS_TABLE).await;
        fake.script(&[AppendScript::CallError(code), AppendScript::Ok]);
        let sink = open_sink(&fake, "").await;

        sink.consume(&topic(), metadata(), events(0..1))
            .await
            .expect("transient connection and token failures should retry");

        assert_eq!(offsets(&fake.stored()), vec![0], "{code:?}");
    }
}

#[tokio::test]
async fn given_append_response_hangs_should_fail_at_the_configured_deadline() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::Hang]);
    let sink = open_sink(&fake, "max_retries = 1").await;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sink.consume(&topic(), metadata(), events(0..1)),
    )
    .await
    .expect("connector deadline should finish before the test timeout");

    assert!(
        matches!(result, Err(Error::CannotStoreData(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn given_transient_failure_beyond_retry_budget_should_fail() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[
        AppendScript::CallError(Code::Unavailable),
        AppendScript::CallError(Code::Unavailable),
        AppendScript::CallError(Code::Unavailable),
    ]);
    let sink = open_sink(&fake, "max_retries = 2").await;

    let result = sink.consume(&topic(), metadata(), events(0..2)).await;

    assert!(
        matches!(result, Err(Error::CannotStoreData(_))),
        "{result:?}"
    );
    assert!(fake.stored().is_empty());
    assert_eq!(fake.pending_script(), 1, "only two attempts were made");
}

#[tokio::test]
async fn given_invalid_argument_should_fail_without_retry() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[AppendScript::ResponseError(Code::InvalidArgument)]);
    let sink = open_sink(&fake, "").await;

    let result = sink.consume(&topic(), metadata(), events(0..2)).await;

    assert!(
        matches!(result, Err(Error::PermanentHttpError(_))),
        "{result:?}"
    );
    assert_eq!(fake.appends().len(), 1);
}

#[tokio::test]
async fn given_permission_error_should_be_permanent() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.script(&[
        AppendScript::CallError(Code::PermissionDenied),
        AppendScript::Ok,
    ]);
    let sink = open_sink(&fake, "").await;

    let result = sink.consume(&topic(), metadata(), events(0..1)).await;

    assert!(
        matches!(result, Err(Error::PermanentHttpError(_))),
        "{result:?}"
    );
    assert!(fake.stored().is_empty());
    assert_eq!(fake.pending_script(), 1, "permission error must not retry");
}

#[tokio::test]
async fn given_unsupported_column_type_should_fail_open() {
    let fake = FakeBigQuery::start(r#"{"name":"span","type":"INTERVAL"}"#).await;
    let mut sink = BigQuerySink::new(1, config(&fake, "include_metadata = false"));

    let result = sink.open().await;

    let Err(Error::InitError(reason)) = result else {
        panic!("expected InitError, got {result:?}");
    };
    assert!(reason.contains("span"), "{reason}");
}

#[tokio::test]
async fn given_missing_metadata_column_should_fail_open() {
    let fake = FakeBigQuery::start(r#"{"name":"user_id","type":"INT64"}"#).await;
    let mut sink = BigQuerySink::new(1, config(&fake, ""));

    let result = sink.open().await;

    let Err(Error::InitError(reason)) = result else {
        panic!("expected InitError, got {result:?}");
    };
    assert!(reason.contains("iggy_stream STRING"), "{reason}");
}

#[tokio::test]
async fn given_table_not_found_should_fail_open_without_retry() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.table_responses(&[(404, r#"{"error":{"message":"Not found: Table"}}"#)]);
    let mut sink = BigQuerySink::new(1, config(&fake, ""));

    let result = sink.open().await;

    assert!(matches!(result, Err(Error::InitError(_))), "{result:?}");
    assert_eq!(fake.table_calls(), 1);
}

#[tokio::test]
async fn given_table_lookup_unavailable_once_should_retry_and_open() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.table_responses(&[(503, "busy")]);
    let mut sink = BigQuerySink::new(1, config(&fake, ""));

    sink.open().await.expect("second tables.get succeeds");

    assert_eq!(fake.table_calls(), 2);
}

#[tokio::test]
async fn given_write_stream_permission_denied_should_fail_open() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.fail_get_write_stream(Code::PermissionDenied);
    let mut sink = BigQuerySink::new(1, config(&fake, ""));

    let result = sink.open().await;

    assert!(matches!(result, Err(Error::InitError(_))), "{result:?}");
}

#[tokio::test]
async fn given_write_stream_setup_keeps_retrying_should_hit_overall_open_timeout() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    fake.fail_get_write_stream(Code::Unknown);
    let mut sink_config = config(&fake, "");
    sink_config.timeout = Some("100ms".into());
    let mut sink = BigQuerySink::new(1, sink_config);

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), sink.open())
        .await
        .expect("open deadline should finish before the test timeout");

    assert!(matches!(result, Err(Error::InitError(_))), "{result:?}");
}

#[tokio::test]
async fn given_raw_mode_should_store_payload_column() {
    let fake = FakeBigQuery::start(r#"{"name":"payload","type":"JSON"}"#).await;
    let sink = open_sink(&fake, "mode = \"raw\"\ninclude_metadata = false").await;
    let messages = vec![
        json_message(0, r#"{"a": 1}"#),
        ConsumedMessage {
            payload: Payload::Text("not json".into()),
            ..json_message(1, "{}")
        },
    ];

    sink.consume(&topic(), metadata(), messages).await.unwrap();

    let stored = fake.stored();
    assert_eq!(stored.len(), 1);
    let payload = stored[0].column(0).as_string::<i32>();
    assert_eq!(payload.len(), 1);
    assert_eq!(payload.value(0), r#"{"a":1}"#);
}

#[tokio::test]
async fn given_all_rows_rejected_locally_should_fail_the_batch() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "").await;
    let messages = vec![
        json_message(0, r#"{"event":"missing id"}"#),
        json_message(1, r#"{"user_id":1.5}"#),
    ];

    let result = sink.consume(&topic(), metadata(), messages).await;

    assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    assert!(fake.appends().is_empty());
}

#[tokio::test]
async fn given_rows_with_different_default_presence_should_use_separate_appends() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "").await;
    let messages = vec![
        json_message(0, r#"{"user_id":0,"created":"2024-01-02T03:04:05Z"}"#),
        json_message(1, r#"{"user_id":1}"#),
        json_message(2, r#"{"user_id":2,"created":null}"#),
    ];

    sink.consume(&topic(), metadata(), messages)
        .await
        .expect("each default-presence group should append");

    let appends = fake.appends();
    assert_eq!(appends.len(), 2);
    assert!(appends[0].batch.column_by_name("created").is_some());
    assert!(appends[1].batch.column_by_name("created").is_none());
    assert_eq!(offsets(&fake.stored()), vec![0, 1, 2]);
}

#[tokio::test]
async fn given_empty_batch_should_not_call_bigquery() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = open_sink(&fake, "").await;

    sink.consume(&topic(), metadata(), Vec::new())
        .await
        .unwrap();

    assert!(fake.appends().is_empty());
}

#[tokio::test]
async fn given_consume_before_open_should_fail() {
    let fake = FakeBigQuery::start(EVENTS_TABLE).await;
    let sink = BigQuerySink::new(1, config(&fake, ""));

    let result = sink.consume(&topic(), metadata(), events(0..1)).await;

    assert!(matches!(result, Err(Error::InitError(_))), "{result:?}");
}
