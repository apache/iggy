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

use std::collections::BTreeMap;
use std::str::FromStr;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_scopedb_sink::{ScopeDBSink, WriteMode};
use iggy_connector_sdk::{Error, Payload, Schema, Sink};
use serde_json::json;
use tokio::sync::watch;

mod common;
use common::{FakeScopeDB, Reply, json_message, message, metadata, table, topic};

#[tokio::test]
async fn given_partial_writes_when_recovering_should_only_resubmit_valid_rejected_rows() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([
        Reply::Commit,
        Reply::RowErrors {
            indexes: vec![1, 1],
            truncated: false,
        },
        Reply::Commit,
        Reply::Commit,
    ]);
    let mut sink = ScopeDBSink::new(1, backend.config());
    sink.open().await.expect("open sink");
    let messages = (0..6)
        .map(|index| json_message(1000 + index, json!({"id":index})))
        .collect();
    sink.consume(&topic(), metadata(Schema::Json), messages)
        .await
        .expect("recover valid rows");
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 4);
    assert_eq!(
        state.appends[0].rows,
        vec![json!({"id":0}), json!({"id":1})]
    );
    assert_eq!(
        state.appends[2].rows,
        vec![json!({"id":4}), json!({"id":5})]
    );
    assert_eq!(state.appends[3].rows, vec![json!({"id":2})]);
    assert_eq!(state.stored.len(), 5);
    assert_eq!(sink.stats().written, 5);
    assert_eq!(sink.stats().rejected, 1);
    assert_eq!(sink.stats().failed, 0);
    assert_eq!(sink.stats().unconfirmed, 0);
}

#[tokio::test]
async fn given_unknown_outcomes_when_retries_disabled_should_not_replay_rows() {
    for reply in [
        Reply::Ambiguous,
        Reply::InvalidCount,
        Reply::Reject(StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
        backend.script([Reply::Commit, reply]);
        let mut config = backend.config();
        config.batch_size = Some(1);
        let mut sink = ScopeDBSink::new(2, config);
        sink.open().await.expect("open sink");
        let result = sink
            .consume(
                &topic(),
                metadata(Schema::Json),
                vec![
                    json_message(10, json!({"id":1})),
                    json_message(11, json!({"id":2})),
                ],
            )
            .await;
        assert!(matches!(result, Err(Error::WriteFailure(_))));
        assert_eq!(sink.stats().written, 1);
        assert_eq!(sink.stats().unconfirmed, 1);
        assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 2);
    }
}

#[tokio::test]
async fn given_rate_limits_when_writing_should_bound_retries() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([Reply::Reject(StatusCode::TOO_MANY_REQUESTS), Reply::Commit]);
    let mut config = backend.config();
    config.api_key = Some("test-api-key".into());
    let mut sink = ScopeDBSink::new(3, config);
    sink.open().await.expect("open sink");
    sink.consume(
        &topic(),
        metadata(Schema::Json),
        vec![json_message(1, json!({"id":1}))],
    )
    .await
    .expect("retry rejection");
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 2);
    assert_eq!(state.appends[0].rows, state.appends[1].rows);
    assert_eq!(
        state.appends[0].headers["authorization"],
        "Bearer test-api-key"
    );
    assert_eq!(state.stored.len(), 1);
}

#[tokio::test]
async fn given_exhausted_rejections_when_writing_should_stop_at_three_attempts() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([
        Reply::Reject(StatusCode::TOO_MANY_REQUESTS),
        Reply::Reject(StatusCode::TOO_MANY_REQUESTS),
        Reply::Reject(StatusCode::TOO_MANY_REQUESTS),
        Reply::Commit,
    ]);
    let mut config = backend.config();
    config.max_retries = Some(99);
    let mut sink = ScopeDBSink::new(16, config);
    sink.open().await.expect("open sink");
    assert!(matches!(
        sink.consume(
            &topic(),
            metadata(Schema::Json),
            vec![json_message(1, json!({"id":1}))]
        )
        .await,
        Err(Error::PermanentHttpError(_))
    ));
    assert_eq!(sink.stats().failed, 1);
    assert_eq!(sink.stats().unconfirmed, 0);
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 3);
    assert!(state.stored.is_empty());
}

#[tokio::test]
async fn given_permanent_or_incomplete_errors_when_writing_should_not_replay_rows() {
    for reply in [
        Reply::Reject(StatusCode::FORBIDDEN),
        Reply::RowErrors {
            indexes: vec![1],
            truncated: true,
        },
        Reply::RowErrors {
            indexes: vec![99],
            truncated: false,
        },
    ] {
        let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
        backend.script([reply]);
        let mut sink = ScopeDBSink::new(4, backend.config());
        sink.open().await.expect("open sink");
        let result = sink
            .consume(
                &topic(),
                metadata(Schema::Json),
                vec![
                    json_message(10, json!({"id":1})),
                    json_message(11, json!({"id":2})),
                ],
            )
            .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
        assert_eq!(sink.stats().failed, 2);
        assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 1);
    }
}

#[tokio::test]
async fn given_repeated_row_errors_when_recovering_should_fail_without_another_replay() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([
        Reply::RowErrors {
            indexes: vec![0],
            truncated: false,
        },
        Reply::RowErrors {
            indexes: vec![0],
            truncated: false,
        },
    ]);
    let mut sink = ScopeDBSink::new(5, backend.config());
    sink.open().await.expect("open sink");
    assert!(
        sink.consume(
            &topic(),
            metadata(Schema::Json),
            vec![
                json_message(10, json!({"id":1})),
                json_message(11, json!({"id":2}))
            ]
        )
        .await
        .is_err()
    );
    assert_eq!(sink.stats().rejected, 1);
    assert_eq!(sink.stats().failed, 1);
    assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 2);
}

#[tokio::test]
async fn given_request_limits_when_writing_should_split_and_preserve_the_valid_tail() {
    let backend = FakeScopeDB::start(table(&[("text", "string")])).await;
    let mut config = backend.config();
    config.max_request_bytes = Some(40);
    config.batch_size = Some(100);
    let mut sink = ScopeDBSink::new(6, config);
    sink.open().await.expect("open sink");
    let mut messages: Vec<_> = (0..4)
        .map(|offset| json_message(offset, json!({"text":"abcdefghijklmnop"})))
        .collect();
    messages.insert(2, json_message(999, json!({"text":"x".repeat(80)})));
    sink.consume(&topic(), metadata(Schema::Json), messages)
        .await
        .expect("write valid tail");
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 4);
    assert!(state.appends.iter().all(|append| append.bytes <= 40));
    assert_eq!(state.stored.len(), 4);
    assert_eq!(sink.stats().written, 4);
    assert_eq!(sink.stats().rejected, 1);
}

#[tokio::test]
async fn given_message_metadata_when_writing_should_preserve_offsets_ids_and_headers() {
    let backend = FakeScopeDB::start(table(&[
        ("id", "int"),
        ("iggy_stream", "string"),
        ("iggy_topic", "string"),
        ("iggy_partition_id", "uint"),
        ("iggy_offset", "uint"),
        ("iggy_timestamp", "timestamp"),
        ("iggy_id", "string"),
        ("iggy_headers", "object"),
    ]))
    .await;
    let mut config = backend.config();
    config.include_metadata = Some(true);
    config.include_headers = Some(true);
    let mut sink = ScopeDBSink::new(7, config);
    sink.open().await.expect("open sink");
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::from_str("trace").expect("header key"),
        HeaderValue::from_str("abc").expect("text header"),
    );
    headers.insert(
        HeaderKey::from_str("blob").expect("header key"),
        HeaderValue::try_from(&[255_u8, 0][..]).expect("binary header"),
    );
    let mut input = json_message(
        42,
        json!({"id":1,"iggy_offset":999,"ignored":"not a column"}),
    );
    input.headers = Some(headers);
    sink.consume(&topic(), metadata(Schema::Json), vec![input])
        .await
        .expect("write metadata");
    let state = backend.state.lock().expect("backend lock");
    let row = &state.stored[0];
    assert_eq!(row["iggy_offset"], 42);
    assert_eq!(row["iggy_partition_id"], 3);
    assert_eq!(row["iggy_id"], u128::MAX.to_string());
    assert_eq!(row["iggy_timestamp"], "2023-11-14T22:13:20.000001Z");
    assert_eq!(row["iggy_headers"]["trace"], "abc");
    assert_eq!(row["iggy_headers"]["blob"]["data"], "/wA=");
    assert!(row.get("ignored").is_none());
}

#[tokio::test]
async fn given_mapped_payloads_when_writing_should_require_supported_json_objects() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    let mut sink = ScopeDBSink::new(8, backend.config());
    sink.open().await.expect("open sink");
    for payload in [
        Payload::Text("{\"id\":1}".into()),
        Payload::Proto("{\"id\":2}".into()),
    ] {
        let schema = payload.schema();
        sink.consume(&topic(), metadata(schema), vec![message(1, payload)])
            .await
            .expect("write JSON object");
    }
    for payload in [
        Payload::Raw(b"{\"id\":3}".to_vec()),
        Payload::Text("[]".into()),
        Payload::Proto("not JSON".into()),
    ] {
        let schema = payload.schema();
        assert!(matches!(
            sink.consume(&topic(), metadata(schema), vec![message(2, payload)])
                .await,
            Err(Error::InvalidRecordValue(_))
        ));
    }
    assert_eq!(sink.stats().written, 2);
    assert_eq!(sink.stats().rejected, 3);
}

#[tokio::test]
async fn given_raw_payloads_when_writing_should_preserve_binary_text_and_json_values() {
    for (column_type, payload, expected) in [
        ("binary", Payload::Raw(vec![255, 0, 1]), json!("/wAB")),
        ("binary", Payload::Avro(vec![0, 255]), json!("AP8=")),
        ("binary", Payload::FlatBuffer(vec![0, 1]), json!("AAE=")),
        ("string", Payload::Text("hello".into()), json!("hello")),
        (
            "object",
            Payload::Proto("{\"id\":1}".into()),
            json!({"id":1}),
        ),
        ("any", Payload::Raw(b"[1,true]".to_vec()), json!([1, true])),
    ] {
        let backend = FakeScopeDB::start(table(&[("payload", column_type)])).await;
        let mut config = backend.config();
        config.mode = Some(WriteMode::Raw);
        let mut sink = ScopeDBSink::new(9, config);
        sink.open().await.expect("open raw sink");
        let schema = payload.schema();
        sink.consume(&topic(), metadata(schema), vec![message(1, payload)])
            .await
            .expect("write raw payload");
        assert_eq!(
            backend.state.lock().expect("backend lock").stored[0]["payload"],
            expected
        );
    }
}

#[tokio::test]
async fn given_invalid_numbers_when_writing_should_reject_before_network_io() {
    let backend = FakeScopeDB::start(table(&[("id", "uint"), ("attrs", "object")])).await;
    let mut sink = ScopeDBSink::new(10, backend.config());
    sink.open().await.expect("open sink");
    for value in [json!({"id":u64::MAX}), json!({"attrs":{"nested":u64::MAX}})] {
        assert!(
            sink.consume(
                &topic(),
                metadata(Schema::Json),
                vec![json_message(1, value)]
            )
            .await
            .is_err()
        );
    }
    assert_eq!(sink.stats().rejected, 2);
    assert!(
        backend
            .state
            .lock()
            .expect("backend lock")
            .appends
            .is_empty()
    );
}

#[tokio::test]
async fn given_catalog_failures_when_opening_should_retry_and_validate_schema() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend
        .state
        .lock()
        .expect("backend lock")
        .catalog_replies
        .extend([StatusCode::SERVICE_UNAVAILABLE, StatusCode::OK]);
    let mut sink = ScopeDBSink::new(11, backend.config());
    sink.open().await.expect("retry catalog fetch");
    assert_eq!(backend.state.lock().expect("backend lock").catalog_calls, 2);
    let mut config = backend.config();
    config.include_metadata = Some(true);
    let mut sink = ScopeDBSink::new(12, config);
    assert!(matches!(sink.open().await, Err(Error::SchemaMismatch(_))));
}

#[tokio::test]
async fn given_timeout_when_unknown_retries_enabled_should_not_replay_rows() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([Reply::Hang]);
    let mut config = backend.config();
    config.retry_ambiguous = Some(true);
    config.timeout = Some("1s".into());
    let mut sink = ScopeDBSink::new(13, config);
    sink.open().await.expect("open sink");
    assert!(matches!(
        sink.consume(
            &topic(),
            metadata(Schema::Json),
            vec![json_message(1, json!({"id":1}))]
        )
        .await,
        Err(Error::WriteFailure(_))
    ));
    assert_eq!(sink.stats().unconfirmed, 1);
    assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 1);
}

#[tokio::test]
async fn given_unknown_outcome_when_later_rejection_occurs_should_preserve_unknown_error() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([Reply::Ambiguous, Reply::Reject(StatusCode::FORBIDDEN)]);
    let mut config = backend.config();
    config.batch_size = Some(1);
    let mut sink = ScopeDBSink::new(14, config);
    sink.open().await.expect("open sink");
    let result = sink
        .consume(
            &topic(),
            metadata(Schema::Json),
            vec![
                json_message(10, json!({"id":1})),
                json_message(11, json!({"id":2})),
            ],
        )
        .await;
    assert!(matches!(result, Err(Error::WriteFailure(_))));
    assert_eq!(sink.stats().unconfirmed, 1);
    assert_eq!(sink.stats().failed, 1);
    assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 2);
}

#[tokio::test]
async fn given_unknown_replay_when_later_row_errors_occur_should_not_filter_and_replay() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([
        Reply::Ambiguous,
        Reply::RowErrors {
            indexes: vec![0],
            truncated: false,
        },
    ]);
    let mut config = backend.config();
    config.retry_ambiguous = Some(true);
    let mut sink = ScopeDBSink::new(15, config);
    sink.open().await.expect("open sink");
    let result = sink
        .consume(
            &topic(),
            metadata(Schema::Json),
            vec![
                json_message(10, json!({"id":1})),
                json_message(11, json!({"id":2})),
            ],
        )
        .await;
    assert!(matches!(result, Err(Error::WriteFailure(_))));
    assert_eq!(sink.stats().unconfirmed, 2);
    assert_eq!(sink.stats().rejected, 0);
    assert_eq!(backend.state.lock().expect("backend lock").appends.len(), 2);
}

#[tokio::test]
async fn given_failed_batch_without_row_indexes_when_writing_should_preserve_successful_siblings() {
    let backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    backend.script([
        Reply::Commit,
        Reply::Reject(StatusCode::BAD_REQUEST),
        Reply::Commit,
    ]);
    let mut sink = ScopeDBSink::new(17, backend.config());
    sink.open().await.expect("open sink");
    let result = sink
        .consume(
            &topic(),
            metadata(Schema::Json),
            (0..6)
                .map(|id| json_message(id, json!({"id":id})))
                .collect(),
        )
        .await;
    assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    assert_eq!(sink.stats().written, 4);
    assert_eq!(sink.stats().failed, 2);
    assert_eq!(sink.stats().rejected, 0);
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 3);
    assert_eq!(
        state.stored,
        vec![
            json!({"id":0}),
            json!({"id":1}),
            json!({"id":4}),
            json!({"id":5})
        ]
    );
}

#[tokio::test]
async fn given_cancelled_http_wait_when_closing_should_not_retry_or_send_the_suffix() {
    let mut backend = FakeScopeDB::start(table(&[("id", "int")])).await;
    let (release, gate) = watch::channel(false);
    backend.script([Reply::Blocked(gate)]);
    let mut config = backend.config();
    config.batch_size = Some(1);
    let mut sink = ScopeDBSink::new(18, config);
    sink.open().await.expect("open sink");
    let sink = Arc::new(sink);
    let pending = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.consume(
                &topic(),
                metadata(Schema::Json),
                vec![
                    json_message(1, json!({"id":1})),
                    json_message(2, json!({"id":2})),
                ],
            )
            .await
        }
    });
    backend.wait_for_appends(1).await;
    pending.abort();
    assert!(pending.await.expect_err("consume cancelled").is_cancelled());
    let mut sink = Arc::try_unwrap(sink).expect("cancelled task releases the sink");
    sink.close().await.expect("close cancelled sink");
    release.send(true).expect("release HTTP response");
    sink.open().await.expect("reopen sink");
    sink.consume(
        &topic(),
        metadata(Schema::Json),
        vec![json_message(9, json!({"id":9}))],
    )
    .await
    .expect("new consume succeeds");
    sink.close().await.expect("close settled sink");
    assert_eq!(sink.stats().written, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), backend.wait_for_appends(3))
            .await
            .is_err()
    );
    let state = backend.state.lock().expect("backend lock");
    assert_eq!(state.appends.len(), 2);
    assert_eq!(state.appends[0].rows, vec![json!({"id":1})]);
    assert_eq!(state.appends[1].rows, vec![json!({"id":9})]);
}
