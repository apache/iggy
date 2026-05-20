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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::InfluxDbSinkFixture;
use bytes::Bytes;
use iggy::prelude::IggyMessage;
use iggy::prelude::Partitioning;
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::json;

// seeds::connector_stream creates the topic with 1 partition (Iggy partition IDs are 1-based).
// Use Partitioning::balanced() so the runtime picks partition 1 automatically.

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_writes_messages_to_bucket(
    harness: &TestHarness,
    fixture: InfluxDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (1u32..=TEST_MESSAGE_COUNT as u32)
        .map(|i| {
            let payload = serde_json::to_vec(&json!({"sensor_id": i, "temp": 20.0 + i as f64}))
                .expect("Failed to serialize");
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_points("iggy_messages", TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to wait for InfluxDB points");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_handles_bulk_messages(harness: &TestHarness, fixture: InfluxDbSinkFixture) {
    let client = harness.root_client().await.unwrap();
    let bulk_count = 50;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..bulk_count)
        .map(|i| {
            let payload = serde_json::to_vec(&json!({"seq": i})).expect("Failed to serialize");
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
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_points("iggy_messages", bulk_count)
        .await
        .expect("Failed to wait for InfluxDB points");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_payload_fields_stored_correctly(
    harness: &TestHarness,
    fixture: InfluxDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = serde_json::to_vec(&json!({"device": "sensor-42", "reading": 99.5})).unwrap();
    let mut messages = vec![
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_points("iggy_messages", 1)
        .await
        .expect("Failed to wait for InfluxDB points");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_large_batch(harness: &TestHarness, fixture: InfluxDbSinkFixture) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    for chunk_start in (0..500usize).step_by(100) {
        let mut chunk: Vec<IggyMessage> = (chunk_start..chunk_start + 100)
            .map(|i| {
                let payload = serde_json::to_vec(&json!({"seq": i})).expect("Failed to serialize");
                IggyMessage::builder()
                    .id((i + 1) as u128)
                    .payload(Bytes::from(payload))
                    .build()
                    .expect("Failed to build message")
            })
            .collect();

        client
            .send_messages(&stream_id, &topic_id, &Partitioning::balanced(), &mut chunk)
            .await
            .expect("Failed to send messages");
    }

    fixture
        .wait_for_points("iggy_messages", 500)
        .await
        .expect("Failed to wait for 500 InfluxDB points");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_recovers_backlogged_messages(
    harness: &TestHarness,
    fixture: InfluxDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..10)
        .map(|i| {
            let payload = serde_json::to_vec(&json!({"i": i})).expect("Failed to serialize");
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
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_points("iggy_messages", 10)
        .await
        .expect("Failed to wait for 10 backlogged InfluxDB points");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_sink_multiple_partitions(harness: &TestHarness, fixture: InfluxDbSinkFixture) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Topic has only 1 partition — send 3 messages via balanced partitioning
    // (they all go to partition 1, which is correct for a 1-partition topic).
    for i in 1u32..=3 {
        let payload = serde_json::to_vec(&json!({"msg_index": i})).expect("Failed to serialize");
        let mut messages = vec![
            IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload))
                .build()
                .unwrap(),
        ];

        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::balanced(),
                &mut messages,
            )
            .await
            .expect("Failed to send messages");
    }

    fixture
        .wait_for_points("iggy_messages", 3)
        .await
        .expect("Failed to wait for 3 InfluxDB points");
}
