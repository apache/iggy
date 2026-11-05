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

use std::time::Duration;

use crate::connectors::fixtures::{
    RabbitMqSourceBatchFixture, RabbitMqSourceFixture, RabbitMqSourceMetadataFixture,
};
use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::{
    CommitPolicy, Consumer, Identifier, IggyMessage, MessageClient, PollOptions, PollingStrategy,
};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable},
};
use reqwest::Client;
use serde_json::Value;
use tokio::time::{sleep, timeout};

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn rabbitmq_source_preserves_raw_payload(
    harness: &TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    wait_for_running(harness).await;
    fixture
        .publish(&[0, 255, 1], BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed");
    let message = next_message(harness).await;
    assert_eq!(message.payload.as_ref(), &[0, 255, 1]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_supported_delivery_when_iggy_accepts_batch_should_acknowledge_rabbitmq(
    harness: &mut TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    wait_for_running(harness).await;
    fixture
        .publish(b"ack-after-iggy-accepts", BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed");

    let message = next_message(harness).await;
    assert_eq!(message.payload.as_ref(), b"ack-after-iggy-accepts");

    timeout(Duration::from_secs(30), async {
        loop {
            if fixture
                .acknowledged_messages()
                .await
                .expect("RabbitMQ queue inspection must succeed")
                .is_some_and(|count| count >= 1)
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ must acknowledge the accepted delivery");

    harness
        .server_mut()
        .stop_dependents()
        .expect("connectors runtime must stop");
    timeout(Duration::from_secs(10), async {
        loop {
            if fixture
                .consumer_count()
                .await
                .expect("RabbitMQ consumer inspection must succeed")
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ source consumer must disconnect");

    assert!(
        !fixture
            .get_ready_message()
            .await
            .expect("RabbitMQ queue inspection must succeed"),
        "RabbitMQ must not redeliver the acknowledged message"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_three_deliveries_when_iggy_accepts_batch_should_acknowledge_all(
    harness: &mut TestHarness,
    fixture: RabbitMqSourceBatchFixture,
) {
    let messages = next_messages(harness, 3).await;
    let payloads: Vec<_> = messages
        .iter()
        .map(|message| message.payload.as_ref())
        .collect();
    assert_eq!(
        payloads,
        vec![b"batch-1".as_slice(), b"batch-2", b"batch-3"]
    );

    timeout(Duration::from_secs(30), async {
        loop {
            if fixture
                .acknowledged_messages()
                .await
                .expect("RabbitMQ queue inspection must succeed")
                == Some(3)
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ must acknowledge all three deliveries");

    harness
        .server_mut()
        .stop_dependents()
        .expect("connectors runtime must stop");
    timeout(Duration::from_secs(10), async {
        loop {
            if fixture
                .consumer_count()
                .await
                .expect("RabbitMQ consumer inspection must succeed")
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ source consumer must disconnect");
    assert!(
        !fixture
            .get_ready_message()
            .await
            .expect("RabbitMQ queue inspection must succeed")
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn rabbitmq_source_reconnects_after_connection_is_closed(
    harness: &TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    wait_for_running(harness).await;
    let original_connection = timeout(Duration::from_secs(30), async {
        loop {
            let connections = fixture
                .consumer_connections()
                .await
                .expect("RabbitMQ consumer inspection must succeed");
            if let [connection] = connections.as_slice() {
                break connection.clone();
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("RabbitMQ source consumer must start");
    fixture
        .close_connection(&original_connection)
        .await
        .expect("RabbitMQ source connection must close");

    timeout(Duration::from_secs(30), async {
        loop {
            let connections = fixture
                .consumer_connections()
                .await
                .expect("RabbitMQ consumer inspection must succeed");
            if let [connection] = connections.as_slice()
                && connection != &original_connection
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("RabbitMQ source must create a consumer on a new connection");

    fixture
        .publish(b"rabbitmq-broker-reconnect", BasicProperties::default())
        .await
        .expect("RabbitMQ should accept a publish after reconnecting");
    let message = next_message(harness).await;
    assert_eq!(message.payload.as_ref(), b"rabbitmq-broker-reconnect");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_broker_outage_when_reconnect_fails_should_retry_after_recovery(
    harness: &TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    wait_for_running(harness).await;
    timeout(Duration::from_secs(30), async {
        loop {
            if fixture
                .consumer_count()
                .await
                .expect("RabbitMQ consumer inspection must succeed")
                == 1
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("RabbitMQ source consumer must start");

    fixture
        .stop_broker()
        .await
        .expect("RabbitMQ broker must stop");
    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime must exist");
    timeout(Duration::from_secs(30), async {
        loop {
            let (stdout, stderr) = runtime.collect_logs();
            if stdout.contains("RabbitMQ connect failed")
                || stderr.contains("RabbitMQ connect failed")
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("RabbitMQ source must report a failed reconnect while the broker is stopped");

    fixture
        .start_broker()
        .await
        .expect("RabbitMQ broker must start");
    let mut last_observation = String::new();
    timeout(Duration::from_secs(60), async {
        loop {
            match fixture.consumer_count().await {
                Ok(1) => break,
                Ok(count) => last_observation = format!("consumer count: {count}"),
                Err(error) => last_observation = format!("management request failed: {error}"),
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("RabbitMQ source must reconnect after the broker starts: {last_observation}")
    });

    fixture
        .publish(b"rabbitmq-retry-after-outage", BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed after recovery");
    let message = next_message(harness).await;
    assert_eq!(message.payload.as_ref(), b"rabbitmq-retry-after-outage");
}

#[iggy_harness(
    cluster_nodes = 1,
    server(
        // The test must stop the server process, not a Cargo runner.
        executable_path = "../../target/debug/iggy-server",
        connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")
    ),
    seed = seeds::connector_stream
)]
async fn given_iggy_is_unavailable_when_delivery_arrives_should_redeliver_after_runtime_restart(
    harness: &mut TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    harness
        .server_mut()
        .stop_dependents()
        .expect("connectors runtime should stop");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("connectors runtime should restart");

    wait_for_running(harness).await;
    let server_address = harness
        .server()
        .tcp_addr()
        .expect("Iggy server must have a TCP address");
    harness
        .kill_node(0)
        .expect("Iggy server should stop for failure injection");
    timeout(Duration::from_secs(10), async {
        loop {
            if matches!(
                timeout(
                    Duration::from_secs(1),
                    tokio::net::TcpStream::connect(server_address),
                )
                .await,
                Ok(Err(_))
            ) {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Iggy server must stop accepting connections");

    fixture
        .publish(
            b"rabbitmq-iggy-failure-redelivery",
            BasicProperties::default(),
        )
        .await
        .expect("RabbitMQ should accept the message while Iggy is down");

    timeout(Duration::from_secs(15), async {
        loop {
            if fixture
                .unacknowledged_messages()
                .await
                .expect("RabbitMQ queue inspection should succeed")
                == 1
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ should retain the failed delivery as unacknowledged");

    wait_for_source_error(harness).await;

    harness
        .restart_node(0)
        .expect("Iggy server should restart after failure injection");
    harness
        .root_client()
        .await
        .expect("Iggy should accept connections after restart");
    harness
        .server_mut()
        .stop_dependents()
        .expect("connectors runtime should stop after source error");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("connectors runtime should restart the RabbitMQ source");
    let message = next_message(harness).await;
    assert_eq!(
        message.payload.as_ref(),
        b"rabbitmq-iggy-failure-redelivery"
    );
}

async fn wait_for_source_error(harness: &TestHarness) {
    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime must exist");
    let http = Client::new();
    timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = http
                .get(format!("{}/sources/rabbitmq", runtime.http_url()))
                .send()
                .await
                && let Ok(source) = response.json::<Value>().await
                && source.get("status").and_then(Value::as_str) == Some("error")
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ source should report an error while Iggy is unavailable");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn rabbitmq_source_preserves_metadata_in_json_envelope(
    harness: &TestHarness,
    fixture: RabbitMqSourceMetadataFixture,
) {
    wait_for_running(harness).await;
    let mut headers = FieldTable::default();
    headers.insert(
        "amqp.exchange".into(),
        AMQPValue::LongString(vec![255].into()),
    );
    let properties = BasicProperties::default()
        .with_message_id("example-id".into())
        .with_headers(headers);
    fixture
        .publish(&[0, 255], properties)
        .await
        .expect("RabbitMQ publish must succeed");
    let message = next_message(harness).await;
    let envelope: Value =
        serde_json::from_slice(message.payload.as_ref()).expect("payload must be JSON");
    assert_eq!(envelope["body_base64"], "AP8=");
    assert_eq!(envelope["properties"]["message_id"], "example-id");
    assert_eq!(envelope["headers"]["amqp.exchange"]["type"], "long_string");
    assert_eq!(envelope["headers"]["amqp.exchange"]["value_base64"], "/w==");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_empty_body_in_metadata_mode_when_delivered_should_preserve_empty_body(
    harness: &TestHarness,
    fixture: RabbitMqSourceMetadataFixture,
) {
    wait_for_running(harness).await;
    fixture
        .publish(&[], BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed");
    let message = next_message(harness).await;
    let envelope: Value =
        serde_json::from_slice(message.payload.as_ref()).expect("payload must be JSON");
    assert_eq!(envelope["body_base64"], "");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_empty_body_in_raw_mode_when_followed_by_valid_message_should_skip_empty_body(
    harness: &TestHarness,
    fixture: RabbitMqSourceFixture,
) {
    wait_for_running(harness).await;
    fixture
        .publish(&[], BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed");
    fixture
        .publish(b"after-empty", BasicProperties::default())
        .await
        .expect("RabbitMQ publish must succeed");
    let message = next_message(harness).await;
    assert_eq!(message.payload.as_ref(), b"after-empty");
}

async fn wait_for_running(harness: &TestHarness) {
    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime must exist");
    let client = reqwest::Client::new();
    timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = client
                .get(format!("{}/sources/rabbitmq", runtime.http_url()))
                .send()
                .await
                && let Ok(source) = response.json::<Value>().await
                && source.get("status").and_then(Value::as_str) == Some("running")
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ source must start");
}

async fn next_message(harness: &TestHarness) -> IggyMessage {
    next_messages(harness, 1).await.remove(0)
}

async fn next_messages(harness: &TestHarness, count: u32) -> Vec<IggyMessage> {
    let client = harness
        .root_client()
        .await
        .expect("Iggy client must connect");
    let stream: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream identifier must parse");
    let topic: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("topic identifier must parse");
    let consumer: Identifier = "rabbitmq_source_test"
        .try_into()
        .expect("consumer identifier must parse");
    timeout(Duration::from_secs(30), async {
        let mut messages = Vec::with_capacity(count as usize);
        loop {
            if let Ok(polled) = client
                .poll_messages(
                    &stream,
                    &topic,
                    DEFAULT_PARTITION_ID,
                    &Consumer::new(consumer.clone()),
                    &PollingStrategy::next(),
                    PollOptions {
                        count: count - messages.len() as u32,
                        commit: CommitPolicy::OnRead,
                        ..PollOptions::default()
                    },
                )
                .await
            {
                messages.extend(polled.messages);
                if messages.len() == count as usize {
                    return messages;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RabbitMQ deliveries must reach Iggy")
}
