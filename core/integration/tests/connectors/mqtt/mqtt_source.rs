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
    Mqtt5InvalidCredentialsFixture, Mqtt5PendingBatchFixture, Mqtt5Qos0Fixture, Mqtt5Qos1Fixture,
    Mqtt5Qos2Fixture, Mqtt311InvalidCredentialsFixture, Mqtt311Qos0Fixture, Mqtt311Qos1Fixture,
    Mqtt311Qos2Fixture,
};
use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use integration::harness::{TestBinary, TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde_json::Value;
use std::time::Duration;
use std::{collections::HashSet, future::Future};
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
    let message = assert_message_is_persisted(
        harness,
        fixture.publish(b"mqtt311-qos1-integration"),
        b"mqtt311-qos1-integration",
    )
    .await;
    let headers = message
        .user_headers_map()
        .expect("Iggy headers should deserialize")
        .expect("MQTT headers should be present");

    assert_eq!(
        headers[&"mqtt.protocol".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "mqtt311"
    );
    assert_eq!(
        headers[&"mqtt.topic".try_into().unwrap()].as_str().unwrap(),
        "devices/test/telemetry"
    );
    assert_eq!(
        headers[&"mqtt.qos".try_into().unwrap()].as_uint8().unwrap(),
        1
    );
    assert!(
        headers[&"mqtt.packet_id".try_into().unwrap()]
            .as_uint16()
            .is_ok()
    );
    assert!(!headers[&"mqtt.dup".try_into().unwrap()].as_bool().unwrap());
    assert!(
        !headers[&"mqtt.retain".try_into().unwrap()]
            .as_bool()
            .unwrap()
    );
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
async fn mqtt5_messages_are_persisted_as_a_source_batch(
    harness: &TestHarness,
    fixture: Mqtt5Qos1Fixture,
) {
    wait_for_source_running(harness).await;
    let payloads = (0..10)
        .map(|index| format!("mqtt5-batch-{index}").into_bytes())
        .collect::<Vec<_>>();
    fixture
        .publish_batch(&payloads)
        .await
        .expect("MQTT batch publish should complete");

    assert_payloads_are_persisted(harness, payloads.into_iter().collect()).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_messages_are_persisted_as_a_source_batch(
    harness: &TestHarness,
    fixture: Mqtt311Qos1Fixture,
) {
    wait_for_source_running(harness).await;
    let payloads = (0..10)
        .map(|index| format!("mqtt311-batch-{index}").into_bytes())
        .collect::<Vec<_>>();
    fixture
        .publish_batch(&payloads)
        .await
        .expect("MQTT 3.1.1 batch publish should complete");

    assert_payloads_are_persisted(harness, payloads.into_iter().collect()).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_partial_source_batch_flushes_after_timeout(
    harness: &TestHarness,
    fixture: Mqtt5Qos1Fixture,
) {
    let payload = b"mqtt5-partial-batch-timeout";
    assert_message_is_persisted(harness, fixture.publish(payload), payload).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_connector_restart_redelivers_pending_qos1_batch(
    harness: &TestHarness,
    fixture: Mqtt5PendingBatchFixture,
) {
    wait_for_source_running(harness).await;
    let payload = b"mqtt5-connector-restart-redelivery";
    fixture
        .publish(payload)
        .await
        .expect("MQTT publish should complete");

    restart_source(harness).await;
    assert_message_is_persisted(harness, async { Ok(()) }, payload).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_iggy_failure_redelivers_unacknowledged_qos1_message(
    harness: &mut TestHarness,
    fixture: Mqtt5Qos1Fixture,
) {
    wait_for_source_running(harness).await;
    harness
        .server_mut()
        .stop()
        .expect("Iggy server should stop for failure injection");

    let payload = b"mqtt5-iggy-failure-redelivery";
    fixture
        .publish(payload)
        .await
        .expect("MQTT broker should accept the message while Iggy is down");

    harness
        .server_mut()
        .start()
        .expect("Iggy server should restart after failure injection");
    harness
        .root_client()
        .await
        .expect("Iggy should accept connections after restart");
    restart_source(harness).await;

    assert_message_is_persisted(harness, async { Ok(()) }, payload).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_emqx_outage_recovers_batch_accumulation(
    harness: &TestHarness,
    fixture: Mqtt5PendingBatchFixture,
) {
    wait_for_source_running(harness).await;
    let first_payload = b"mqtt5-emqx-restart-first".to_vec();
    let second_payload = b"mqtt5-emqx-restart-second".to_vec();
    fixture
        .publish(&first_payload)
        .await
        .expect("first MQTT publish should complete");
    fixture.restart_broker().await.expect("EMQX should restart");
    fixture
        .publish(&second_payload)
        .await
        .expect("second MQTT publish should complete after EMQX restart");

    assert_payloads_are_persisted(
        harness,
        [first_payload, second_payload].into_iter().collect(),
    )
    .await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_mixed_qos_messages_are_persisted_as_a_source_batch(
    harness: &TestHarness,
    fixture: Mqtt5Qos2Fixture,
) {
    wait_for_source_running(harness).await;
    let messages = vec![
        (0, b"mqtt5-mixed-qos0".to_vec()),
        (1, b"mqtt5-mixed-qos1".to_vec()),
        (2, b"mqtt5-mixed-qos2".to_vec()),
    ];
    fixture
        .publish_mixed_batch(&messages)
        .await
        .expect("mixed MQTT batch publish should complete");

    assert_payloads_are_persisted(
        harness,
        messages.into_iter().map(|(_, payload)| payload).collect(),
    )
    .await;
}

async fn assert_payloads_are_persisted(harness: &TestHarness, expected: HashSet<Vec<u8>>) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "mqtt_source_batch_consumer".try_into().unwrap();

    let received = timeout(POLL_TIMEOUT, async {
        let mut received = HashSet::new();
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
            {
                received.extend(polled.messages.into_iter().filter_map(|message| {
                    expected
                        .contains(message.payload.as_ref())
                        .then_some(message.payload.to_vec())
                }));
            }
            if received == expected {
                return received;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT batch messages should be persisted to Iggy");

    assert_eq!(received, expected);
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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_publish_properties_are_persisted_as_iggy_headers(
    harness: &TestHarness,
    fixture: Mqtt5Qos1Fixture,
) {
    let properties = PublishProperties {
        payload_format_indicator: Some(1),
        message_expiry_interval: Some(30),
        topic_alias: None,
        response_topic: Some("devices/response".to_string()),
        correlation_data: Some(b"correlation".as_slice().into()),
        user_properties: vec![("device_id".to_string(), "device-1".to_string())],
        subscription_identifiers: Vec::new(),
        content_type: Some("application/json".to_string()),
    };
    let message = assert_message_is_persisted(
        harness,
        fixture.publish_with_properties(b"mqtt5-properties-integration", properties),
        b"mqtt5-properties-integration",
    )
    .await;
    let headers = message
        .user_headers_map()
        .expect("Iggy headers should deserialize")
        .expect("MQTT headers should be present");

    assert_eq!(
        headers[&"mqtt.protocol".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "mqtt5"
    );
    assert_eq!(
        headers[&"mqtt.topic".try_into().unwrap()].as_str().unwrap(),
        "devices/test/telemetry"
    );
    assert_eq!(
        headers[&"mqtt.qos".try_into().unwrap()].as_uint8().unwrap(),
        1
    );
    assert_eq!(
        headers[&"mqtt.payload_format_indicator".try_into().unwrap()]
            .as_uint8()
            .unwrap(),
        1
    );
    assert_eq!(
        headers[&"mqtt.message_expiry_interval".try_into().unwrap()]
            .as_uint32()
            .unwrap(),
        30
    );
    assert_eq!(
        headers[&"mqtt.response_topic".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "devices/response"
    );
    assert_eq!(
        headers[&"mqtt.correlation_data".try_into().unwrap()]
            .as_raw()
            .unwrap(),
        b"correlation"
    );
    assert_eq!(
        headers[&"mqtt.user_property.0.key".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "device_id"
    );
    assert_eq!(
        headers[&"mqtt.user_property.0.value".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "device-1"
    );
    assert_eq!(
        headers[&"mqtt.content_type".try_into().unwrap()]
            .as_str()
            .unwrap(),
        "application/json"
    );
}

async fn assert_message_is_persisted<F>(
    harness: &TestHarness,
    publish: F,
    payload: &[u8],
) -> iggy_common::IggyMessage
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
    received
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

async fn restart_source(harness: &TestHarness) {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime should be configured");
    let response = Client::new()
        .post(format!("{}/sources/mqtt/restart", runtime.http_url()))
        .send()
        .await
        .expect("source restart request should be sent");
    assert!(
        response.status().is_success(),
        "source restart should succeed, got {}",
        response.status()
    );
    wait_for_source_running(harness).await;
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
