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
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
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
async fn delta_sink_initializes_and_runs(harness: &TestHarness, fixture: DeltaFixture) {
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

    drop(fixture);
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

    let version_count = fixture
        .wait_for_delta_log(1, VERSION_POLL_ATTEMPTS, VERSION_POLL_INTERVAL_MS)
        .await
        .expect("Data should be written to Delta table");

    assert!(version_count >= 1);

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

    let version_count = fixture
        .wait_for_delta_log(1, BULK_VERSION_POLL_ATTEMPTS, VERSION_POLL_INTERVAL_MS)
        .await
        .expect("Data should be written to Delta table");

    assert!(version_count >= 1);

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

    // For S3, we can't check local delta log files.
    // Instead, poll the connector runtime API to verify successful consumption.
    let max_attempts = 30;
    let interval_ms = 500;
    let mut last_error = None;

    for _ in 0..max_attempts {
        let response = http_client
            .get(format!("{}/sinks", api_address))
            .header("api-key", API_KEY)
            .send()
            .await
            .expect("Failed to get sinks");

        let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");
        if !sinks.is_empty() && sinks[0].last_error.is_none() && sinks[0].enabled {
            last_error = None;
            // Give the sink time to process the messages
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            // Verify still healthy after processing
            let response = http_client
                .get(format!("{}/sinks", api_address))
                .header("api-key", API_KEY)
                .send()
                .await
                .expect("Failed to get sinks");
            let sinks: Vec<SinkInfoResponse> =
                response.json().await.expect("Failed to parse sinks");
            assert_eq!(sinks.len(), 1);
            assert_eq!(sinks[0].key, DELTA_SINK_KEY);
            assert!(
                sinks[0].last_error.is_none(),
                "Sink reported error: {:?}",
                sinks[0].last_error
            );
            break;
        }

        last_error = sinks.first().and_then(|s| s.last_error.clone());
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
    }

    assert!(
        last_error.is_none(),
        "Delta S3 sink had errors: {:?}",
        last_error
    );

    drop(fixture);
}
