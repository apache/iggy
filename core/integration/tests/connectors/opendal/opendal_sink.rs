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

use std::time::{Duration, Instant};

use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus, SinkInfoResponse};
use integration::{harness::seeds, iggy_harness};
use reqwest::Client;
use tokio::time::{sleep, timeout};
use wiremock::Request;

use crate::connectors::fixtures::{
    OpenDalControlledS3SinkFixture, OpenDalS3SinkFixture, OpenDalSinkFixture, S3SinkOps,
};

const EXPECTED_JSON_LINES: &[u8] = br#"{"payload":{"id":1}}
{"payload":{"id":2}}
"#;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_batch_when_consumed_should_write_single_json_lines_object_to_fs(
    harness: &TestHarness,
    fixture: OpenDalSinkFixture,
) {
    send_json_batch(harness).await;

    fixture
        .wait_for_object(0, 1, EXPECTED_JSON_LINES)
        .await
        .expect("OpenDAL batch object should contain the expected payload");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_batch_when_consumed_should_write_single_json_lines_object_to_s3(
    harness: &TestHarness,
    fixture: OpenDalS3SinkFixture,
) {
    assert!(
        fixture
            .list_objects("")
            .await
            .expect("bucket should be readable")
            .is_empty(),
        "startup probe should not leave objects in the bucket"
    );

    send_json_batch(harness).await;

    let keys = fixture
        .wait_for_objects("iggy/messages/", 1)
        .await
        .expect("OpenDAL batch object should reach S3");
    let key = "iggy/messages/test_stream/test_topic/\
               00000-00000000000000000000-00000000000000000001.jsonl";
    assert_eq!(keys, vec![key.to_string()]);
    let payload = fixture
        .get_object(key)
        .await
        .expect("OpenDAL batch object should be readable");
    assert_eq!(payload.as_slice(), EXPECTED_JSON_LINES);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_s3_bucket_removed_when_consumed_should_report_write_failure(
    harness: &TestHarness,
    fixture: OpenDalS3SinkFixture,
) {
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();
    let sinks: Vec<SinkInfoResponse> = http_client
        .get(format!("{api_url}/sinks"))
        .send()
        .await
        .expect("sink request should complete")
        .error_for_status()
        .expect("sink endpoint should succeed")
        .json()
        .await
        .expect("sink response should decode");
    let sink = sinks
        .iter()
        .find(|sink| sink.key == "opendal")
        .expect("OpenDAL sink should be reported");
    assert_eq!(sink.status, ConnectorStatus::Running);

    let status = fixture
        .bucket()
        .delete()
        .await
        .expect("empty bucket should be deleted after startup");
    assert_eq!(status, 204);

    send_json_batch(harness).await;

    wait_for_sink_stats(harness, 2, 0, 1).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_temporary_s3_errors_when_consumed_should_retry_identical_objects_until_success(
    harness: &TestHarness,
    fixture: OpenDalControlledS3SinkFixture,
) {
    fixture.fail_next_writes(1);
    send_json_batch(harness).await;
    wait_for_sink_stats(harness, 2, 2, 0).await;
    assert_object_requests(&fixture.object_requests(), 0, 1, 2);

    fixture.fail_next_writes(2);
    send_json_batch_with_first_id(harness, 3).await;
    wait_for_sink_stats(harness, 4, 4, 0).await;

    let requests = fixture.object_requests();
    assert_eq!(
        requests.len(),
        5,
        "both batches should use only their planned attempts"
    );
    assert_object_requests(&requests[2..], 2, 3, 3);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_temporary_s3_errors_when_attempts_are_exhausted_should_report_write_failure(
    harness: &TestHarness,
    fixture: OpenDalControlledS3SinkFixture,
) {
    fixture.fail_next_writes(3);
    send_json_batch(harness).await;

    wait_for_sink_stats(harness, 2, 0, 1).await;
    assert_object_requests(&fixture.object_requests(), 0, 1, 3);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_permanent_s3_error_when_consumed_should_report_failure_without_retrying(
    harness: &TestHarness,
    fixture: OpenDalControlledS3SinkFixture,
) {
    fixture.deny_writes();
    send_json_batch(harness).await;

    wait_for_sink_stats(harness, 2, 0, 1).await;
    assert_object_requests(&fixture.object_requests(), 0, 1, 1);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_delayed_s3_responses_when_writes_time_out_should_report_failure_before_responses(
    harness: &TestHarness,
    fixture: OpenDalControlledS3SinkFixture,
) {
    fixture.delay_writes(Duration::from_secs(60));
    send_json_batch(harness).await;

    wait_for_sink_stats(harness, 2, 0, 1).await;
    let failure_observed_at = Instant::now();
    let earliest_response_at = fixture
        .first_delayed_response_at()
        .expect("an object write should have reached the delayed endpoint");
    assert!(
        failure_observed_at < earliest_response_at,
        "the runtime should report exhausted write deadlines before any delayed response can arrive"
    );
    // A local timeout does not establish whether the remote write committed.
    assert_object_requests(&fixture.object_requests(), 0, 1, 3);
}

fn assert_object_requests(
    requests: &[Request],
    offset_start: u64,
    offset_end: u64,
    attempts: usize,
) {
    assert_eq!(
        requests.len(),
        attempts,
        "unexpected number of S3 object PUTs"
    );
    let path = format!(
        "/iggy-opendal-test/iggy/messages/test_stream/test_topic/\
         00000-{offset_start:020}-{offset_end:020}.jsonl"
    );
    for request in requests {
        assert_eq!(request.url.path(), path);
        assert_eq!(request.body.as_slice(), EXPECTED_JSON_LINES);
    }
}

async fn wait_for_sink_stats(
    harness: &integration::harness::TestHarness,
    expected_consumed: u64,
    expected_processed: u64,
    expected_errors: u64,
) {
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();
    let sink = timeout(Duration::from_secs(15), async {
        loop {
            let snapshot: ConnectorRuntimeStats = http_client
                .get(format!("{api_url}/stats"))
                .send()
                .await
                .expect("stats request should complete")
                .error_for_status()
                .expect("stats endpoint should succeed")
                .json()
                .await
                .expect("stats response should decode");
            let sink = snapshot
                .connectors
                .into_iter()
                .find(|sink| sink.key == "opendal")
                .expect("OpenDAL sink stats should be reported");
            if sink
                .messages_consumed
                .is_some_and(|count| count >= expected_consumed)
                && sink
                    .messages_processed
                    .is_some_and(|count| count >= expected_processed)
                && sink.errors >= expected_errors
            {
                break sink;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("OpenDAL batch outcome should reach the runtime");

    assert_eq!(sink.messages_consumed, Some(expected_consumed));
    assert_eq!(sink.messages_processed, Some(expected_processed));
    assert_eq!(sink.errors, expected_errors);
}

async fn send_json_batch(harness: &integration::harness::TestHarness) {
    send_json_batch_with_first_id(harness, 1).await;
}

async fn send_json_batch_with_first_id(
    harness: &integration::harness::TestHarness,
    first_id: u128,
) {
    let client = harness.root_client().await.expect("client should connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream identifier should be valid");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("topic identifier should be valid");
    let payloads = [
        Bytes::from_static(br#"{"id":1}"#),
        Bytes::from_static(br#"{"id":2}"#),
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id(first_id + index as u128)
                .payload(payload)
                .build()
                .expect("message should build")
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
        .expect("messages should be sent");
}
