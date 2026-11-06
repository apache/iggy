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

//! At-least-once delivery of the HTTP sink against a WireMock receiver: a rejected message is
//! delivered once the receiver accepts it again, before any later message.

use crate::connectors::fixtures::{
    HttpSinkAtLeastOnceFixture, HttpSinkAtLeastOnceStopFixture, HttpSinkWireMockContainer,
};
use bytes::Bytes;
use iggy::prelude::IggyClient;
use iggy_common::{Identifier, IggyMessage, MessageClient, Partitioning};
use iggy_connector_sdk::api::{ConnectorStatus, SinkInfoResponse};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const HTTP_SINK_KEY: &str = "http";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_at_least_once_when_the_receiver_recovers_should_deliver_the_rejected_message_first(
    harness: &TestHarness,
    fixture: HttpSinkAtLeastOnceFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    let receiver = fixture.container();
    receiver
        .set_ingest_status(StatusCode::SERVICE_UNAVAILABLE)
        .await
        .expect("ingest response should be configured");

    send(&client, "first").await;
    // Retries of the rejected message, so the sink is past its first attempt.
    wait_for_messages(receiver, |messages| messages.len() >= 2).await;
    receiver
        .set_ingest_status(StatusCode::OK)
        .await
        .expect("ingest response should be configured");
    send(&client, "second").await;

    let messages = wait_for_messages(receiver, |messages| {
        messages.last().is_some_and(|last| last == "second")
    })
    .await;
    let (last, retried) = messages.split_last().expect("requests were received");
    assert_eq!(last, "second");
    assert!(
        retried.len() >= 3 && retried.iter().all(|message| message == "first"),
        "the rejected message must be retried until accepted, before the next one: {messages:?}"
    );
    assert_eq!(sink_info(harness).await.status, ConnectorStatus::Running);
}

/// The reproduction from apache/iggy#4232: a rejected message, then a restart once the
/// receiver recovered. At most once, the restart skipped the rejected message.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_at_least_once_when_a_stopped_sink_restarts_should_deliver_the_rejected_message(
    harness: &TestHarness,
    fixture: HttpSinkAtLeastOnceStopFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    let receiver = fixture.container();
    receiver
        .set_ingest_status(StatusCode::SERVICE_UNAVAILABLE)
        .await
        .expect("ingest response should be configured");

    send(&client, "first").await;
    let stopped = wait_for_sink(harness, |sink| sink.status == ConnectorStatus::Error).await;
    let error = stopped
        .last_error
        .expect("a stopped sink reports its error")
        .message;
    assert!(
        error.contains("Delivery of stream:"),
        "the error should name the stopped delivery: {error}"
    );

    // The stopped sink sends nothing more, so these are all the rejected requests.
    let rejected = wait_for_messages(receiver, |_| true).await;
    assert!(
        !rejected.is_empty() && rejected.iter().all(|message| message == "first"),
        "only the first message was sent before the stop: {rejected:?}"
    );

    receiver
        .set_ingest_status(StatusCode::OK)
        .await
        .expect("ingest response should be configured");
    restart_sink(harness).await;
    send(&client, "second").await;

    let messages = wait_for_messages(receiver, |messages| {
        messages.last().is_some_and(|last| last == "second")
    })
    .await;
    assert_eq!(
        messages[rejected.len()..],
        ["first", "second"],
        "the restart must deliver the rejected message before the next one: {messages:?}"
    );
}

async fn send(client: &IggyClient, text: &str) {
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("valid stream name");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("valid topic name");
    let payload = serde_json::to_vec(&serde_json::json!({ "message": text }))
        .expect("payload should serialize");
    let mut messages = vec![
        IggyMessage::builder()
            .payload(Bytes::from(payload))
            .build()
            .expect("valid JSON message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("message should reach Iggy");
}

/// Waits until the `message` fields of the requests received so far, oldest first, satisfy
/// `condition`, and returns them.
async fn wait_for_messages(
    receiver: &HttpSinkWireMockContainer,
    condition: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    timeout(DELIVERY_TIMEOUT, async {
        loop {
            // WireMock lists the newest request first.
            let messages: Vec<String> = receiver
                .get_received_requests()
                .await
                .expect("received requests should be available")
                .iter()
                .rev()
                .map(|request| {
                    let body = request.body_as_json().expect("body should be JSON");
                    body["payload"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect();
            if condition(&messages) {
                return messages;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the receiver should get the expected requests")
}

async fn sink_info(harness: &TestHarness) -> SinkInfoResponse {
    reqwest::Client::new()
        .get(format!("{}/sinks/{HTTP_SINK_KEY}", runtime_url(harness)))
        .send()
        .await
        .expect("sink request should complete")
        .error_for_status()
        .expect("sink endpoint should succeed")
        .json()
        .await
        .expect("sink response should decode")
}

async fn wait_for_sink(
    harness: &TestHarness,
    condition: impl Fn(&SinkInfoResponse) -> bool,
) -> SinkInfoResponse {
    timeout(DELIVERY_TIMEOUT, async {
        loop {
            let sink = sink_info(harness).await;
            if condition(&sink) {
                return sink;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the sink should reach the expected state")
}

async fn restart_sink(harness: &TestHarness) {
    reqwest::Client::new()
        .post(format!(
            "{}/sinks/{HTTP_SINK_KEY}/restart",
            runtime_url(harness)
        ))
        .send()
        .await
        .expect("restart request should complete")
        .error_for_status()
        .expect("restart should succeed");
}

fn runtime_url(harness: &TestHarness) -> String {
    harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url()
}
