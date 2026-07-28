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
    RabbitMqOps, RabbitMqSinkDirectFixture, RabbitMqSinkFanoutFixture, RabbitMqSinkFixture,
    RabbitMqSinkRawSchemaFixture, RabbitMqSinkWithoutMetadataFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use lapin::types::{AMQPValue, ShortString};

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/sink.toml")),
    seed = seeds::connector_stream
)]

async fn json_messages_are_published_to_rabbitmq_exchange(
    harness: &TestHarness,
    fixture: RabbitMqSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = [
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
        serde_json::json!({"name": "Carol"}),
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).unwrap()))
                .build()
                .unwrap()
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
        .unwrap();

    let delivered = fixture.consume_messages(3).await.unwrap();
    assert_eq!(delivered.len(), 3);
    for (idx, delivery) in delivered.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_slice(&delivery.data).unwrap();
        assert_eq!(value, payloads[idx]);
        assert_eq!(
            header_str(&delivery.headers, "iggy_stream").as_deref(),
            Some("test_stream")
        );
        assert_eq!(
            header_str(&delivery.headers, "iggy_topic").as_deref(),
            Some("test_topic")
        );
    }
}

fn header_str(headers: &lapin::types::FieldTable, key: &str) -> Option<String> {
    headers
        .inner()
        .get(&ShortString::from(key))
        .and_then(|v| match v {
            AMQPValue::LongString(s) => Some(s.to_string()),
            _ => None,
        })
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/sink.toml")),
    seed = seeds::connector_stream
)]

async fn given_direct_exchange_when_published_should_deliver_to_bound_queue(
    harness: &TestHarness,
    fixture: RabbitMqSinkDirectFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = [
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
        serde_json::json!({"name": "Carol"}),
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).unwrap()))
                .build()
                .unwrap()
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
        .unwrap();

    let delivered = fixture.consume_messages(3).await.unwrap();
    assert_eq!(delivered.len(), 3);
    for (idx, delivery) in delivered.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_slice(&delivery.data).unwrap();
        assert_eq!(value, payloads[idx]);
        assert_eq!(
            header_str(&delivery.headers, "iggy_stream").as_deref(),
            Some("test_stream")
        );
        assert_eq!(
            header_str(&delivery.headers, "iggy_topic").as_deref(),
            Some("test_topic")
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_include_metadata_false_when_published_should_not_include_iggy_headers(
    harness: &TestHarness,
    fixture: RabbitMqSinkWithoutMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = serde_json::json!({"name": "Alice"});
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(serde_json::to_vec(&payload).unwrap()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let delivered = fixture.consume_messages(1).await.unwrap();
    assert_eq!(delivered.len(), 1);
    let value: serde_json::Value = serde_json::from_slice(&delivered[0].data).unwrap();
    assert_eq!(value, payload);
    assert!(delivered[0].headers.inner().is_empty());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_raw_schema_when_published_should_preserve_raw_payload_bytes(
    harness: &TestHarness,
    fixture: RabbitMqSinkRawSchemaFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let raw_payloads: Vec<Vec<u8>> = vec![
        b"plain text message".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD],
        vec![0xDE, 0xAD, 0xBE, 0xEF],
    ];

    let mut messages: Vec<IggyMessage> = raw_payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(payload.clone()))
                .build()
                .unwrap()
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
        .unwrap();

    let delivered = fixture.consume_messages(3).await.unwrap();
    assert_eq!(delivered.len(), 3);
    for (idx, delivery) in delivered.iter().enumerate() {
        assert_eq!(delivery.data, raw_payloads[idx]);
        assert_eq!(
            header_str(&delivery.headers, "iggy_stream").as_deref(),
            Some("test_stream")
        );
        assert_eq!(
            header_str(&delivery.headers, "iggy_topic").as_deref(),
            Some("test_topic")
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/rabbitmq/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_fanout_exchange_when_published_should_deliver_to_all_bound_queues(
    harness: &TestHarness,
    fixture: RabbitMqSinkFanoutFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = serde_json::json!({"name": "Alice"});
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(serde_json::to_vec(&payload).unwrap()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    assert_eq!(
        fixture.queue_names().len(),
        2,
        "fanout fixture should bind two queues"
    );
    for queue_name in fixture.queue_names() {
        let delivered = fixture.consume_messages_from(queue_name, 1).await.unwrap();
        assert_eq!(
            delivered.len(),
            1,
            "fanout exchange must deliver to every bound queue"
        );
        let value: serde_json::Value = serde_json::from_slice(&delivered[0].data).unwrap();
        assert_eq!(value, payload);
        assert_eq!(
            header_str(&delivered[0].headers, "iggy_stream").as_deref(),
            Some("test_stream")
        );
        assert_eq!(
            header_str(&delivered[0].headers, "iggy_topic").as_deref(),
            Some("test_topic")
        );
    }
}
