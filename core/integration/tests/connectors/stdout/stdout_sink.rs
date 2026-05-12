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

use crate::connectors::create_test_messages;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::{
    ConnectorRuntimeStats, ConnectorStatus, HealthResponse, SinkInfoResponse,
};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/stdout/invalid_sink.toml")),
    seed = seeds::connector_stream
)]
async fn stdout_sink_with_invalid_config_does_not_abort_runtime(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let health_response = http_client
        .get(format!("{}/health", api_address))
        .send()
        .await
        .expect("Failed to query health endpoint");
    assert_eq!(health_response.status(), 200);
    let health: HealthResponse = health_response
        .json()
        .await
        .expect("Failed to parse health response");
    assert_eq!(health.status, "healthy");

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(
        sinks.len(),
        2,
        "Both the invalid and the valid sink should be visible in the API"
    );

    let invalid_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_invalid")
        .expect("Invalid sink should be reported");
    assert_eq!(invalid_sink.status, ConnectorStatus::Error);
    let last_error = invalid_sink
        .last_error
        .as_ref()
        .expect("Invalid sink should expose a last_error");
    assert!(
        last_error.message.contains("poll interval"),
        "last_error should mention the misconfigured poll_interval, got: {}",
        last_error.message
    );

    let valid_sink = sinks
        .iter()
        .find(|sink| sink.key == "stdout_valid")
        .expect("Healthy sibling sink should be reported");
    assert_eq!(valid_sink.status, ConnectorStatus::Running);
    assert!(
        valid_sink.last_error.is_none(),
        "Healthy sibling sink should have no last_error"
    );
}
