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

use crate::connectors::create_test_messages;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus, SinkInfoResponse};
use integration::harness::{TestBinaryError, TestFixture, seeds};
use integration::iggy_harness;
use reqwest::Client;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

const API_KEY: &str = "test-api-key";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/stdout/sink.toml")),
    seed = seeds::connector_stream
)]
async fn stdout_sink_consumes_messages(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 5;
    let test_messages = create_test_messages(message_count);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    sleep(Duration::from_millis(500)).await;

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].key, "stdout");
    assert_eq!(sinks[0].status, ConnectorStatus::Running);
    assert!(sinks[0].enabled);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/stdout/sink.toml")),
    seed = seeds::connector_stream
)]
async fn stdout_sink_reports_metrics(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 10;
    let test_messages = create_test_messages(message_count);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    sleep(Duration::from_millis(500)).await;

    let response = http_client
        .get(format!("{}/stats", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get stats");

    assert_eq!(response.status(), 200);
    let stats: ConnectorRuntimeStats = response.json().await.expect("Failed to parse stats");

    assert_eq!(stats.sinks_total, 1);
    assert_eq!(stats.sinks_running, 1);
    assert_eq!(stats.connectors.len(), 1);
    assert_eq!(stats.connectors[0].key, "stdout");
    assert_eq!(stats.connectors[0].connector_type, "sink");
    assert_eq!(stats.connectors[0].status, ConnectorStatus::Running);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/stdout/sink.toml")),
    seed = seeds::connector_stream
)]
async fn stdout_sink_handles_bulk_messages(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 100;
    let test_messages = create_test_messages(message_count);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    sleep(Duration::from_secs(1)).await;

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].status, ConnectorStatus::Running);
    assert!(sinks[0].last_error.is_none());
}

/// Asks for at-least-once delivery from the stdout sink, which does not support it.
pub struct StdoutAtLeastOnceFixture;

#[async_trait]
impl TestFixture for StdoutAtLeastOnceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "IGGY_CONNECTORS_SINK_STDOUT_DELIVERY_GUARANTEE".to_owned(),
                "at_least_once".to_owned(),
            ),
            (
                "IGGY_CONNECTORS_SINK_STDOUT_PATH".to_owned(),
                "../../target/debug/libiggy_connector_stdout_sink".to_owned(),
            ),
        ])
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/stdout/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_at_least_once_when_the_plugin_does_not_support_it_should_refuse_to_start(
    harness: &TestHarness,
    _fixture: StdoutAtLeastOnceFixture,
) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();

    let sink: SinkInfoResponse = Client::new()
        .get(format!("{api_address}/sinks/stdout"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("sink request should complete")
        .json()
        .await
        .expect("sink response should decode");

    assert_eq!(sink.status, ConnectorStatus::Error);
    let error = sink
        .last_error
        .expect("the refusal should be reported")
        .message;
    assert!(
        error.contains("cannot deliver at least once"),
        "the error should name the missing guarantee: {error}"
    );
}
