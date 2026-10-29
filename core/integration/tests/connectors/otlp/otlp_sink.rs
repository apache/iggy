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

use crate::connectors::fixtures::{
    OtelCollectorContainer, OtlpHttpGzipFixture, OtlpHttpMockFixture,
    OtlpHttpMockSmallRequestsFixture, OtlpHttpProtoLogsFixture, OtlpHttpSmallRequestsFixture,
    SMALL_MAX_REQUEST_BYTES, log_record_count,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::time::Duration;
use tokio::time::sleep;

const TEST_TOKEN_HEADER: &str = "Bearer iggy-test-token";
// Comfortably longer than every retry the sink config allows (3 attempts, 200 ms cap).
const NO_RETRY_GRACE: Duration = Duration::from_millis(1_500);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn otlp_proto_logs_are_exported_over_http(
    harness: &TestHarness,
    fixture: OtlpHttpProtoLogsFixture,
) {
    assert_logs_pass_through(harness, fixture.collector(), "http-proto").await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn otlp_proto_logs_are_exported_over_http_with_gzip(
    harness: &TestHarness,
    fixture: OtlpHttpGzipFixture,
) {
    assert_logs_pass_through(harness, fixture.collector(), "http-proto-gzip").await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn undecodable_message_is_dropped_without_losing_its_batch(
    harness: &TestHarness,
    fixture: OtlpHttpProtoLogsFixture,
) {
    send_payloads(
        harness,
        [
            logs_request("http-proto", &["before".to_owned()]).encode_to_vec(),
            vec![0xff; 4],
            logs_request("http-proto", &["after".to_owned()]).encode_to_vec(),
        ],
    )
    .await;

    let exports = fixture
        .collector()
        .wait_for_log_records(2)
        .await
        .expect("collector received the valid records");

    let mut bodies = log_bodies(&exports);
    bodies.sort();
    assert_eq!(bodies, vec!["after", "before"]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn small_max_request_bytes_keeps_every_request_within_limit(
    harness: &TestHarness,
    fixture: OtlpHttpSmallRequestsFixture,
) {
    let requests: Vec<ExportLogsServiceRequest> = (0..5)
        .map(|i| logs_request("http-proto", &[format!("{i}-{}", "x".repeat(120))]))
        .collect();
    let single_message = requests[0].encoded_len() as u64;
    assert!(
        single_message <= SMALL_MAX_REQUEST_BYTES && 2 * single_message > SMALL_MAX_REQUEST_BYTES,
        "the payload must fit the limit alone but not in pairs, got {single_message} bytes"
    );
    send_payloads(harness, requests.iter().map(Message::encode_to_vec)).await;

    let exports = fixture
        .collector()
        .wait_for_log_records(5)
        .await
        .expect("collector received every record");

    assert_eq!(exports.len(), 5, "one request per message");
    for export in &exports {
        assert_eq!(log_record_count(std::slice::from_ref(export)), 1);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn throttled_http_export_is_retried_with_the_same_request(
    harness: &TestHarness,
    fixture: OtlpHttpMockFixture,
) {
    fixture.respond_with(&[503, 200]).await;
    send_payloads(
        harness,
        [logs_request("http-mock", &["retried".to_owned()]).encode_to_vec()],
    )
    .await;

    let requests = fixture
        .wait_for_requests(2)
        .await
        .expect("the throttled export is retried");
    sleep(NO_RETRY_GRACE).await;
    let requests_after_success = fixture.received().await;

    assert_eq!(
        requests_after_success.len(),
        2,
        "an accepted export is not resent"
    );
    assert_eq!(requests[0].body, requests[1].body);
    for request in &requests {
        assert_eq!(
            request
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some(TEST_TOKEN_HEADER)
        );
        assert_eq!(
            request
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/x-protobuf")
        );
    }
    let exported =
        ExportLogsServiceRequest::decode(requests[1].body.as_slice()).expect("protobuf body");
    assert_eq!(log_bodies(&[exported]), vec!["retried"]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn rejected_http_export_is_not_retried_and_later_batches_are_exported(
    harness: &TestHarness,
    fixture: OtlpHttpMockFixture,
) {
    fixture.respond_with(&[400, 200]).await;
    send_payloads(
        harness,
        [logs_request("http-mock", &["rejected".to_owned()]).encode_to_vec()],
    )
    .await;

    fixture
        .wait_for_requests(1)
        .await
        .expect("the export reaches the receiver");
    sleep(NO_RETRY_GRACE).await;
    assert_eq!(
        fixture.received().await.len(),
        1,
        "a 400 is permanent under the OTLP/HTTP specification"
    );

    send_payloads_from(
        harness,
        100,
        [logs_request("http-mock", &["next".to_owned()]).encode_to_vec()],
    )
    .await;
    let requests = fixture
        .wait_for_requests(2)
        .await
        .expect("the batch after a rejected one is still exported");
    let next =
        ExportLogsServiceRequest::decode(requests[1].body.as_slice()).expect("protobuf body");
    assert_eq!(log_bodies(&[next]), vec!["next"]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn otlp_proto_messages_are_forwarded_as_the_bytes_produced(
    harness: &TestHarness,
    fixture: OtlpHttpMockFixture,
) {
    fixture.respond_with(&[200]).await;
    let mut with_unknown_field = logs_request("http-mock", &["first".to_owned()]).encode_to_vec();
    // Field 15 as a varint: no OTLP version defines it, so a decode and re-encode drops it.
    with_unknown_field.extend_from_slice(&[0x78, 0x01]);
    let payloads = vec![
        with_unknown_field,
        logs_request("http-mock", &["second".to_owned()]).encode_to_vec(),
    ];
    let produced = payloads.concat();
    send_payloads(harness, payloads).await;

    fixture
        .wait_for_requests(1)
        .await
        .expect("the export reaches the receiver");
    sleep(NO_RETRY_GRACE).await;

    let received: Vec<u8> = fixture
        .received()
        .await
        .into_iter()
        .flat_map(|request| request.body)
        .collect();
    assert_eq!(received, produced, "the unknown field reaches the receiver");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn message_above_max_request_bytes_is_retried_after_a_transient_failure(
    harness: &TestHarness,
    fixture: OtlpHttpMockSmallRequestsFixture,
) {
    fixture.respond_with(&[503, 200]).await;
    let body = "x".repeat(2 * SMALL_MAX_REQUEST_BYTES as usize);
    let payload = logs_request("http-mock", &[body]).encode_to_vec();
    send_payloads(harness, [payload.clone()]).await;

    let requests = fixture
        .wait_for_requests(2)
        .await
        .expect("a 503 says nothing about the size, so the oversized export is retried");
    sleep(NO_RETRY_GRACE).await;

    assert_eq!(
        fixture.received().await.len(),
        2,
        "an accepted export is not resent"
    );
    assert_eq!(requests[1].body, payload);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn redirected_http_export_is_not_followed(
    harness: &TestHarness,
    fixture: OtlpHttpMockFixture,
) {
    fixture.redirect_to("/moved").await;
    send_payloads(
        harness,
        [logs_request("http-mock", &["redirected".to_owned()]).encode_to_vec()],
    )
    .await;

    fixture
        .wait_for_requests(1)
        .await
        .expect("the export reaches the receiver");
    sleep(NO_RETRY_GRACE).await;

    let requests = fixture.received().await;
    let paths: Vec<&str> = requests.iter().map(|request| request.url.path()).collect();
    assert_eq!(
        paths,
        vec!["/v1/logs"],
        "a 3xx is permanent, and following it would resend the headers as a GET"
    );
}

/// Send three OTLP log requests produced by `service`, and check that the collector got
/// every record, under that service's own resource.
async fn assert_logs_pass_through(
    harness: &TestHarness,
    collector: &OtelCollectorContainer,
    service: &str,
) {
    let requests: Vec<ExportLogsServiceRequest> = (0..3)
        .map(|i| logs_request(service, &[format!("log-{i}-a"), format!("log-{i}-b")]))
        .collect();
    send_payloads(harness, requests.iter().map(Message::encode_to_vec)).await;

    let exports = collector
        .wait_for_log_records(6)
        .await
        .expect("collector received the log records");

    let mut bodies = log_bodies(&exports);
    bodies.sort();
    let mut expected = log_bodies(&requests);
    expected.sort();
    assert_eq!(bodies, expected);
    for export in &exports {
        for resource_logs in &export.resource_logs {
            assert_eq!(
                service_name(resource_logs.resource.as_ref()),
                Some(service),
                "a passthrough export keeps the producer's resource"
            );
        }
    }
}

async fn send_payloads(harness: &TestHarness, payloads: impl IntoIterator<Item = Vec<u8>>) {
    send_payloads_from(harness, 1, payloads).await;
}

/// Send `payloads` as messages with IDs counting up from `first_id`.
async fn send_payloads_from(
    harness: &TestHarness,
    first_id: u128,
    payloads: impl IntoIterator<Item = Vec<u8>>,
) {
    let client = harness.root_client().await.expect("root client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("stream id");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic id");
    let mut messages: Vec<IggyMessage> = payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id(first_id + i as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("build message")
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
        .expect("send messages");
}

fn logs_request(service: &str, bodies: &[String]) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(service)),
            scope_logs: vec![ScopeLogs {
                log_records: bodies
                    .iter()
                    .map(|body| LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9,
                        body: Some(string_value(body)),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_owned(),
            value: Some(string_value(service)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn string_value(text: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(text.to_owned())),
    }
}

fn attribute<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a any_value::Value> {
    attributes
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|value| value.value.as_ref())
}

fn service_name(resource: Option<&Resource>) -> Option<&str> {
    match attribute(&resource?.attributes, "service.name") {
        Some(any_value::Value::StringValue(name)) => Some(name),
        _ => None,
    }
}

fn log_bodies(exports: &[ExportLogsServiceRequest]) -> Vec<String> {
    exports
        .iter()
        .flat_map(|export| &export.resource_logs)
        .flat_map(|resource| &resource.scope_logs)
        .flat_map(|scope| &scope.log_records)
        .filter_map(|record| match record.body.as_ref()?.value.as_ref()? {
            any_value::Value::StringValue(body) => Some(body.clone()),
            _ => None,
        })
        .collect()
}
