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
use crate::connectors::fixtures::{DeltaFixture, DeltaS3Fixture};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::SinkInfoResponse;
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;

const API_KEY: &str = "test-api-key";
const DELTA_SINK_KEY: &str = "delta";
const VERSION_POLL_ATTEMPTS: usize = 30;
const VERSION_POLL_INTERVAL_MS: u64 = 500;
const BULK_VERSION_POLL_ATTEMPTS: usize = 60;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/delta/sink.toml")),
    seed = seeds::connector_stream
)]
async fn delta_sink_initializes_and_runs(harness: &TestHarness, _fixture: DeltaFixture) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].key, DELTA_SINK_KEY);
    assert!(sinks[0].enabled);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/delta/sink.toml")),
    seed = seeds::connector_stream
)]
async fn delta_sink_consumes_json_messages(harness: &TestHarness, fixture: DeltaFixture) {
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

    let row_count = fixture
        .wait_for_row_count(message_count, VERSION_POLL_ATTEMPTS, VERSION_POLL_INTERVAL_MS)
        .await
        .expect("Data should be written to Delta table");

    assert_eq!(row_count, message_count);

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].key, DELTA_SINK_KEY);
    assert!(sinks[0].last_error.is_none());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/delta/sink.toml")),
    seed = seeds::connector_stream
)]
async fn delta_sink_handles_bulk_messages(harness: &TestHarness, fixture: DeltaFixture) {
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

    let row_count = fixture
        .wait_for_row_count(message_count, BULK_VERSION_POLL_ATTEMPTS, VERSION_POLL_INTERVAL_MS)
        .await
        .expect("Data should be written to Delta table");

    assert_eq!(row_count, message_count);

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert!(sinks[0].last_error.is_none());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/delta/sink.toml")),
    seed = seeds::connector_stream
)]
async fn delta_sink_writes_to_s3(harness: &TestHarness, fixture: DeltaS3Fixture) {
    let client = harness.root_client().await.unwrap();

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

    let row_count = fixture
        .wait_for_row_count(message_count, VERSION_POLL_ATTEMPTS, VERSION_POLL_INTERVAL_MS)
        .await
        .expect("Data should be written to S3 Delta table");

    assert_eq!(row_count, message_count);
}
