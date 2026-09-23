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

use crate::connectors::fixtures::MqttFixture;
use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const MQTT_PAYLOAD: &[u8] = b"mqtt5-qos1-integration";
const POLL_TIMEOUT: Duration = Duration::from_secs(15);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mqtt/source.toml")),
    seed = seeds::connector_stream
)]
async fn mqtt5_qos1_messages_are_persisted_to_iggy(harness: &TestHarness, fixture: MqttFixture) {
    wait_for_source_running(harness).await;
    fixture
        .publish_qos_one(MQTT_PAYLOAD)
        .await
        .expect("MQTT publish should complete");

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
                    .find(|message| message.payload.as_ref() == MQTT_PAYLOAD)
            {
                return message;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT message should be persisted to Iggy");

    assert_eq!(received.payload.as_ref(), MQTT_PAYLOAD);
}

async fn wait_for_source_running(harness: &TestHarness) {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime should be configured");
    let http = Client::new();
    let api_url = runtime.http_url();

    timeout(POLL_TIMEOUT, async {
        loop {
            if let Ok(response) = http.get(format!("{api_url}/sources/mqtt")).send().await
                && let Ok(source) = response.json::<Value>().await
                && source.get("status").and_then(Value::as_str) == Some("running")
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("MQTT source connector should become running");
}
