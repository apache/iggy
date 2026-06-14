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

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{S3SinkFixture, S3SinkOps, S3SinkRotationFixture};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::SinkInfoResponse;
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;

const API_KEY: &str = "test-api-key";
const S3_SINK_KEY: &str = "s3";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_initializes_and_runs(harness: &TestHarness, fixture: S3SinkFixture) {
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
    assert_eq!(sinks[0].key, S3_SINK_KEY);
    assert!(sinks[0].enabled);

    drop(fixture);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_writes_jsonl_with_correct_layout(harness: &TestHarness, fixture: S3SinkFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 5;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
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
        .expect("send messages");

    let prefix = format!("{}/", seeds::names::STREAM);
    let keys = fixture
        .wait_for_objects(&prefix, 1)
        .await
        .expect("wait for S3 objects");

    assert!(!keys.is_empty(), "Expected at least one S3 object");

    let key = &keys[0];
    assert!(
        key.contains(seeds::names::STREAM),
        "Key must contain stream name: {key}"
    );
    assert!(
        key.contains(seeds::names::TOPIC),
        "Key must contain topic name: {key}"
    );
    assert!(key.ends_with(".jsonl"), "Key must end with .jsonl: {key}");
    assert!(
        key.contains("/00000-"),
        "Key must contain partition_id (00000): {key}"
    );

    let data = fixture.get_object(key).await.expect("get S3 object");
    let content = String::from_utf8(data).expect("valid utf8");
    let lines: Vec<&str> = content.trim().lines().collect();
    assert_eq!(
        lines.len(),
        message_count,
        "Expected {message_count} lines in JSONL output"
    );

    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert!(value.get("offset").is_some(), "Line must have offset");
        assert!(value.get("timestamp").is_some(), "Line must have timestamp");
        assert!(value.get("stream").is_some(), "Line must have stream");
        assert!(value.get("payload").is_some(), "Line must have payload");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/sink_rotation.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_rotates_on_message_count(harness: &TestHarness, fixture: S3SinkRotationFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 25;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
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
        .expect("send messages");

    let prefix = format!("{}/", seeds::names::STREAM);
    let keys = fixture
        .wait_for_objects(&prefix, 2)
        .await
        .expect("wait for rotated S3 objects");

    assert!(
        keys.len() >= 2,
        "Expected at least 2 S3 objects from rotation (max_messages_per_file=10, sent 25), got {}",
        keys.len()
    );

    for key in &keys {
        assert!(key.ends_with(".jsonl"), "All keys must end with .jsonl");
        assert!(
            key.contains("/00000-"),
            "All keys must contain partition_id"
        );
    }
}
