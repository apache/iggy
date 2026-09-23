// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::connectors::fixtures::{
    Mqtt5InvalidCredentialsFixture, Mqtt5Qos0Fixture, Mqtt5Qos1Fixture, Mqtt5Qos2Fixture,
    Mqtt311InvalidCredentialsFixture, Mqtt311Qos0Fixture, Mqtt311Qos1Fixture, Mqtt311Qos2Fixture,
};
use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use serde_json::Value;
use std::future::Future;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const POLL_TIMEOUT: Duration = Duration::from_secs(15);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_invalid_credentials_should_fail_source_initialization(
    harness: &TestHarness,
    fixture: Mqtt5InvalidCredentialsFixture,
) {
    let source = wait_for_source_status(harness, "error").await;
    assert_eq!(
        source
            .get("last_error")
            .and_then(Value::as_object)
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("Invalid configuration: Plugin initialization failed (ID: 1)")
    );

    let payload = b"mqtt5-invalid-credentials";
    fixture
        .publish(payload)
        .await
        .expect("authenticated MQTT publisher should complete");
    assert_message_is_not_persisted(harness, payload).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_invalid_credentials_should_fail_source_initialization(
    harness: &TestHarness,
    fixture: Mqtt311InvalidCredentialsFixture,
) {
    let source = wait_for_source_status(harness, "error").await;
    assert_eq!(
        source
            .get("last_error")
            .and_then(Value::as_object)
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("Invalid configuration: Plugin initialization failed (ID: 1)")
    );

    let payload = b"mqtt311-invalid-credentials";
    fixture
        .publish(payload)
        .await
        .expect("authenticated MQTT publisher should complete");
    assert_message_is_not_persisted(harness, payload).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos0_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt311Qos0Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt311-qos0-integration"),
        b"mqtt311-qos0-integration",
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos1_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt311Qos1Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt311-qos1-integration"),
        b"mqtt311-qos1-integration",
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos2_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt311Qos2Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt311-qos2-integration"),
        b"mqtt311-qos2-integration",
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos0_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt5Qos0Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt5-qos0-integration"),
        b"mqtt5-qos0-integration",
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos1_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt5Qos1Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt5-qos1-integration"),
        b"mqtt5-qos1-integration",
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos2_messages_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: Mqtt5Qos2Fixture,
) {
    assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt5-qos2-integration"),
        b"mqtt5-qos2-integration",
    )
    .await;
}

async fn assert_message_is_persisted<F>(harness: &TestHarness, publish: F, payload: &[u8])
where
    F: Future<Output = Result<(), String>>,
{
    wait_for_source_running(harness).await;
    publish.await.expect("MQTT publish should complete");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "mqtt_source_test_consumer".try_into().unwrap();

    let received = timeout(POLL_TIMEOUT, async {
        loop {
            if let Ok(polled) = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    10,
                    true,
                )
                .await
                && let Some(message) = polled
                    .messages
                    .into_iter()
                    .find(|message| message.payload.as_ref() == payload)
            {
                return message;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT message should be persisted to Iggy");

    assert_eq!(received.payload.as_ref(), payload);
}

async fn assert_message_is_not_persisted(harness: &TestHarness, payload: &[u8]) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "mqtt_source_auth_failure_consumer".try_into().unwrap();

    let result = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(polled) = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    10,
                    true,
                )
                .await
                && polled
                    .messages
                    .iter()
                    .any(|message| message.payload.as_ref() == payload)
            {
                return true;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(
        result.is_err(),
        "invalid MQTT credentials must not persist messages"
    );
}

async fn wait_for_source_running(harness: &TestHarness) {
    wait_for_source_status(harness, "running").await;
}

async fn wait_for_source_status(harness: &TestHarness, expected_status: &str) -> Value {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime should be configured");
    let http = Client::new();
    let api_url = runtime.http_url();

    timeout(POLL_TIMEOUT, async {
        loop {
            if let Ok(response) = http.get(format!("{api_url}/sources/mqtt")).send().await
                && let Ok(source) = response.json::<Value>().await
                && source.get("status").and_then(Value::as_str) == Some(expected_status)
            {
                return source;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT source connector should reach the expected status")
}
