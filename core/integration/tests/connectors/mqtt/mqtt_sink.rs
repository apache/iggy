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
    Mqtt311SinkQosOneFixture, Mqtt311SinkQosTwoFixture, Mqtt311SinkQosZeroFixture,
    MqttSinkDynamicTopicFixture, MqttSinkEnvelopeFixture, MqttSinkFixture, MqttSinkIdleFixture,
    MqttSinkInvalidCredentialsFixture, MqttSinkPipelinedFixture, MqttSinkQosTwoFixture,
    MqttSinkQosZeroFixture, MqttSinkRawSchemaFixture, MqttSinkTemplateTopicFixture,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{HeaderKey, HeaderValue, Identifier, MessageClient};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tokio::time::{sleep, timeout};

const SINK_STATUS_TIMEOUT: Duration = Duration::from_secs(15);
const MQTT_SINK_KEY: &str = "mqtt";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_invalid_credentials_should_fail_sink_initialization(
    harness: &TestHarness,
    fixture: MqttSinkInvalidCredentialsFixture,
) {
    let sink = wait_for_sink_status(harness, "error").await;
    assert_eq!(
        sink.get("last_error")
            .and_then(Value::as_object)
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("Invalid configuration: Plugin initialization failed (ID: 1)")
    );

    send_json_messages(harness, vec![serde_json::json!({"message": "blocked"})]).await;
    assert!(
        timeout(Duration::from_secs(1), fixture.receive_messages(1))
            .await
            .is_err(),
        "an unauthenticated sink must not publish records"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_broker_rejection_should_not_publish_to_the_broker(
    harness: &TestHarness,
    fixture: MqttSinkFixture,
) {
    let errors_before_rejection = sink_errors(harness).await;
    fixture
        .deny_sink_publications()
        .await
        .expect("EMQX must deny the sink publication topic");
    send_json_messages(harness, vec![serde_json::json!({"rejected": true})]).await;

    assert!(
        timeout(Duration::from_secs(1), fixture.receive_messages(1))
            .await
            .is_err(),
        "a broker-rejected publication must not reach MQTT subscribers"
    );
    wait_for_sink_error_after(harness, errors_before_rejection).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos1_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: MqttSinkFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payloads = [
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
        serde_json::json!({"name": "Carol"}),
    ];
    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(payload).expect("test payload must serialize"),
                ))
                .build()
                .expect("test message must build")
        })
        .collect::<Vec<_>>();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("messages must send to Iggy");

    let received = fixture
        .receive_messages(payloads.len())
        .await
        .expect("messages must reach MQTT");
    let received = received
        .iter()
        .map(|payload| serde_json::from_slice::<serde_json::Value>(payload))
        .collect::<Result<Vec<_>, _>>()
        .expect("MQTT payloads must be valid JSON");

    assert_eq!(received, payloads);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_sink_keeps_idle_connection_alive(
    harness: &TestHarness,
    fixture: MqttSinkIdleFixture,
) {
    wait_for_sink_status(harness, "running").await;
    sleep(Duration::from_secs(8)).await;

    let payload = serde_json::json!({"after_idle": true});
    send_json_messages(harness, vec![payload.clone()]).await;

    let received = fixture
        .receive_messages(1)
        .await
        .expect("an idle MQTT connection must publish without a retry");
    let received: Value =
        serde_json::from_slice(&received[0]).expect("MQTT payload must be valid JSON");

    assert_eq!(received, payload);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn raw_schema_messages_including_payload_larger_than_ten_kib_preserve_payload_bytes(
    harness: &TestHarness,
    fixture: MqttSinkRawSchemaFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payloads = vec![
        b"plain text message".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD],
        vec![0xDE, 0xAD, 0xBE, 0xEF],
        vec![0xA5; 16 * 1024],
    ];
    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::copy_from_slice(payload))
                .build()
                .expect("test message must build")
        })
        .collect::<Vec<_>>();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("messages must send to Iggy");

    let received = fixture
        .receive_messages(payloads.len())
        .await
        .expect("messages must reach MQTT");

    assert_eq!(received, payloads);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos2_pipelined_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: MqttSinkQosTwoFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payloads = (0..10)
        .map(|sequence| serde_json::json!({"sequence": sequence}))
        .collect::<Vec<_>>();
    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(payload).expect("test payload must serialize"),
                ))
                .build()
                .expect("test message must build")
        })
        .collect::<Vec<_>>();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("messages must send to Iggy");

    let received = fixture
        .receive_messages(payloads.len())
        .await
        .expect("messages must reach MQTT");
    let received = received
        .iter()
        .map(|payload| serde_json::from_slice::<serde_json::Value>(payload))
        .collect::<Result<Vec<_>, _>>()
        .expect("MQTT payloads must be valid JSON");

    assert_eq!(received, payloads);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos0_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: MqttSinkQosZeroFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payload = serde_json::json!({"name": "Alice"});
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&payload).expect("test payload must serialize"),
            ))
            .build()
            .expect("test message must build"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("message must send to Iggy");

    let received = fixture
        .receive_messages(1)
        .await
        .expect("message must reach MQTT");
    let received: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("MQTT payload must be valid JSON");

    assert_eq!(received, payload);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos0_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: Mqtt311SinkQosZeroFixture,
) {
    publish_json_message_and_assert_delivery(harness, &fixture, "mqtt311-qos0").await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos1_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: Mqtt311SinkQosOneFixture,
) {
    publish_json_message_and_assert_delivery(harness, &fixture, "mqtt311-qos1").await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt311_qos2_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: Mqtt311SinkQosTwoFixture,
) {
    publish_json_message_and_assert_delivery(harness, &fixture, "mqtt311-qos2").await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_pipelined_messages_are_published_to_mqtt(
    harness: &TestHarness,
    fixture: MqttSinkPipelinedFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payloads = (0..10)
        .map(|index| serde_json::json!({"sequence": index}))
        .collect::<Vec<_>>();
    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(payload).expect("test payload must serialize"),
                ))
                .build()
                .expect("test message must build")
        })
        .collect::<Vec<_>>();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("messages must send to Iggy");

    let received = fixture
        .receive_messages(payloads.len())
        .await
        .expect("messages must reach MQTT");
    let received = received
        .iter()
        .map(|payload| serde_json::from_slice::<serde_json::Value>(payload))
        .collect::<Result<Vec<_>, _>>()
        .expect("MQTT payloads must be valid JSON");

    assert_eq!(received, payloads);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_sink_restart_publishes_pipelined_messages(
    harness: &TestHarness,
    fixture: MqttSinkPipelinedFixture,
) {
    let payloads = (0..10)
        .map(|sequence| serde_json::json!({"sequence": sequence}))
        .collect::<Vec<_>>();
    send_json_messages(harness, payloads.clone()).await;

    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime must be configured");
    let response = Client::new()
        .post(format!("{}/sinks/mqtt/restart", runtime.http_url()))
        .send()
        .await
        .expect("sink restart request must complete");
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    wait_for_sink_status(harness, "running").await;

    let received = fixture
        .receive_publications(payloads.len())
        .await
        .expect("all publications must survive sink restart");
    let received = received
        .iter()
        .map(|publication| {
            serde_json::from_slice::<serde_json::Value>(&publication.payload)
                .expect("MQTT payload must be valid JSON")
        })
        .collect::<Vec<_>>();
    for payload in payloads {
        assert!(
            received.contains(&payload),
            "publication was lost: {payload}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_sink_publishes_new_message_after_reconnect(
    harness: &TestHarness,
    fixture: MqttSinkPipelinedFixture,
) {
    fixture
        .wait_for_sink_reconnect()
        .await
        .expect("sink must connect before the test starts");

    let payloads = (0..10)
        .map(|sequence| serde_json::json!({"sequence": sequence}))
        .collect::<Vec<_>>();
    send_json_messages(harness, payloads.clone()).await;

    let first_publication = fixture
        .receive_publications(1)
        .await
        .expect("a publication must reach MQTT before disconnect")
        .pop()
        .expect("one requested MQTT publication must be returned");
    fixture
        .disconnect_sink()
        .await
        .expect("EMQX must disconnect the sink client");
    fixture
        .wait_for_sink_reconnect()
        .await
        .expect("the sink must reconnect after the blocked publications");

    let recovery_payload = serde_json::json!({"sequence": "after-reconnect"});
    let recovery_message = IggyMessage::builder()
        .id(11)
        .payload(Bytes::from(
            serde_json::to_vec(&recovery_payload).expect("recovery payload must serialize"),
        ))
        .build()
        .expect("recovery message must build");
    send_messages(harness, vec![recovery_message]).await;

    let original_payloads = payloads
        .iter()
        .map(|payload| serde_json::to_vec(payload).expect("test payload must serialize"))
        .collect::<Vec<_>>();
    assert!(
        original_payloads
            .iter()
            .any(|payload| payload == &first_publication.payload),
        "the publication received before disconnect must be part of the original batch"
    );
    fixture
        .receive_until_payloads(&[
            serde_json::to_vec(&recovery_payload).expect("recovery payload must serialize")
        ])
        .await
        .expect("a publication sent after reconnect must reach MQTT");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt_sink_static_topic_routes_publications_to_configured_topic(
    harness: &TestHarness,
    fixture: MqttSinkFixture,
) {
    let payload = serde_json::json!({"route": "static"});
    send_json_messages(harness, vec![payload.clone()]).await;

    let received = fixture
        .receive_publications(1)
        .await
        .expect("publication must reach MQTT");

    assert_eq!(received[0].topic, "iggy.messages");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&received[0].payload)
            .expect("MQTT payload must be valid JSON"),
        payload
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt_sink_template_topic_renders_stream_and_topic(
    harness: &TestHarness,
    fixture: MqttSinkTemplateTopicFixture,
) {
    let payload = serde_json::json!({"route": "template"});
    send_json_messages(harness, vec![payload.clone()]).await;

    let received = fixture
        .receive_publications(1)
        .await
        .expect("publication must reach MQTT");

    assert_eq!(received[0].topic, "integration/test_stream/test_topic");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&received[0].payload)
            .expect("MQTT payload must be valid JSON"),
        payload
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt_sink_dynamic_topic_uses_header_and_fallback(
    harness: &TestHarness,
    fixture: MqttSinkDynamicTopicFixture,
) {
    let topic_header = HeaderKey::try_from("mqtt.topic").expect("MQTT topic header key must parse");
    let topic_value =
        HeaderValue::try_from("devices/living-room").expect("MQTT topic header value must parse");
    let messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(b"header-routed"))
            .user_headers(BTreeMap::from([(topic_header, topic_value)]))
            .build()
            .expect("test message must build"),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from_static(b"fallback-routed"))
            .build()
            .expect("test message must build"),
    ];
    send_messages(harness, messages).await;

    let received = fixture
        .receive_publications(2)
        .await
        .expect("publications must reach MQTT");

    assert_eq!(received[0].topic, "devices/living-room");
    assert_eq!(received[0].payload, b"header-routed");
    assert_eq!(received[1].topic, "fallback/test_stream/test_topic");
    assert_eq!(received[1].payload, b"fallback-routed");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt_sink_mqtt5_envelope_preserves_supported_properties(
    harness: &TestHarness,
    fixture: MqttSinkEnvelopeFixture,
) {
    let payload = b"envelope payload";
    let envelope = serde_json::json!({
        "version": 1,
        "topic": "devices/roof/telemetry",
        "payload_base64": STANDARD.encode(payload),
        "properties": {
            "payload_format_indicator": 1,
            "message_expiry_interval": 30,
            "response_topic": "devices/roof/replies",
            "correlation_data_base64": STANDARD.encode(b"correlation-id"),
            "user_properties": [{"key": "source", "value": "integration-test"}],
            "content_type": "application/json"
        }
    });
    send_json_messages(harness, vec![envelope]).await;

    let received = fixture
        .receive_publications(1)
        .await
        .expect("publication must reach MQTT");
    let properties = received[0]
        .properties
        .as_ref()
        .expect("MQTT 5 publication must carry properties");

    assert_eq!(received[0].topic, "devices/roof/telemetry");
    assert_eq!(received[0].payload, payload);
    assert_eq!(received[0].qos, 1);
    assert!(!received[0].retain);
    assert_eq!(properties.payload_format_indicator, Some(1));
    assert!(
        properties
            .message_expiry_interval
            .is_some_and(|expiry| (1..=30).contains(&expiry))
    );
    assert_eq!(
        properties.response_topic.as_deref(),
        Some("devices/roof/replies")
    );
    assert_eq!(
        properties
            .correlation_data
            .as_ref()
            .map(|value| value.as_ref()),
        Some(b"correlation-id".as_slice())
    );
    assert_eq!(
        properties.user_properties,
        vec![("source".to_string(), "integration-test".to_string())]
    );
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/sink.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt_sink_skips_malformed_envelope_and_publishes_valid_records(
    harness: &TestHarness,
    fixture: MqttSinkEnvelopeFixture,
) {
    let messages = vec![
        serde_json::json!({
            "version": 1,
            "topic": "devices/valid/one",
            "payload_base64": STANDARD.encode(b"first")
        }),
        serde_json::json!({
            "version": "bad",
            "payload_base64": STANDARD.encode(b"discarded")
        }),
        serde_json::json!({
            "version": 1,
            "topic": "devices/valid/two",
            "payload_base64": STANDARD.encode(b"second")
        }),
    ];
    send_json_messages(harness, messages).await;

    let received = fixture
        .receive_publications(2)
        .await
        .expect("valid records must reach MQTT");

    assert_eq!(received[0].topic, "devices/valid/one");
    assert_eq!(received[0].payload, b"first");
    assert_eq!(received[1].topic, "devices/valid/two");
    assert_eq!(received[1].payload, b"second");
}

async fn send_json_messages(harness: &TestHarness, payloads: Vec<serde_json::Value>) {
    let messages = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(&payload).expect("test payload must serialize"),
                ))
                .build()
                .expect("test message must build")
        })
        .collect();
    send_messages(harness, messages).await;
}

async fn send_messages(harness: &TestHarness, mut messages: Vec<IggyMessage>) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("messages must send to Iggy");
}

async fn publish_json_message_and_assert_delivery(
    harness: &TestHarness,
    fixture: &MqttSinkFixture,
    protocol: &str,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client must connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream ID must parse");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID must parse");
    let payload = serde_json::json!({"protocol": protocol});
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&payload).expect("test payload must serialize"),
            ))
            .build()
            .expect("test message must build"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("message must send to Iggy");

    let received = fixture
        .receive_messages(1)
        .await
        .expect("message must reach MQTT");
    let received: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("MQTT payload must be valid JSON");

    assert_eq!(received, payload);
}

async fn wait_for_sink_status(harness: &TestHarness, expected_status: &str) -> Value {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime must be configured");
    let http = Client::new();
    let api_url = runtime.http_url();

    timeout(SINK_STATUS_TIMEOUT, async {
        loop {
            if let Ok(response) = http.get(format!("{api_url}/sinks/mqtt")).send().await
                && let Ok(sink) = response.json::<Value>().await
                && sink.get("status").and_then(Value::as_str) == Some(expected_status)
            {
                return sink;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT sink connector must reach the expected status")
}

async fn sink_errors(harness: &TestHarness) -> u64 {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime must be configured");
    let stats = Client::new()
        .get(format!("{}/stats", runtime.http_url()))
        .send()
        .await
        .expect("runtime statistics request must complete")
        .json::<ConnectorRuntimeStats>()
        .await
        .expect("runtime statistics must be valid JSON");

    stats
        .connectors
        .iter()
        .find(|connector| connector.key == MQTT_SINK_KEY)
        .expect("MQTT sink statistics must be present")
        .errors
}

async fn wait_for_sink_error_after(harness: &TestHarness, previous_errors: u64) {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime must be configured");
    let http = Client::new();

    timeout(SINK_STATUS_TIMEOUT, async {
        loop {
            if let Ok(response) = http
                .get(format!("{}/stats", runtime.http_url()))
                .send()
                .await
                && let Ok(stats) = response.json::<ConnectorRuntimeStats>().await
                && let Some(sink) = stats
                    .connectors
                    .iter()
                    .find(|connector| connector.key == MQTT_SINK_KEY)
                && sink.status == ConnectorStatus::Running
                && sink.errors > previous_errors
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("sink failure must increase the MQTT sink error counter");
}
