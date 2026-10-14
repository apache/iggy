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

use std::collections::BTreeMap;
use std::str::FromStr;

use bytes::Bytes;
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

use crate::connectors::fixtures::{
    RedisOps, RedisSinkFixture, RedisSinkNoMetadataFixture, RedisSinkRawFixture,
};

const TARGET_KEY: &str = "iggy:events:test_stream:test_topic:0";

fn given_json_messages(payloads: &[serde_json::Value]) -> Vec<IggyMessage> {
    payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).unwrap()))
                .build()
                .unwrap()
        })
        .collect()
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redis/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_append_entries_with_offset_ids(
    harness: &TestHarness,
    fixture: RedisSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = [
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
        serde_json::json!({"name": "Carol"}),
    ];
    let mut messages = given_json_messages(&payloads);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let entries = fixture.wait_for_entries(TARGET_KEY, 3).await.unwrap();
    assert_eq!(entries.len(), 3);
    for (idx, (id, fields)) in entries.iter().enumerate() {
        assert_eq!(id, &format!("{}-0", idx + 1));
        let value: serde_json::Value = serde_json::from_slice(&fields["payload"]).unwrap();
        assert_eq!(value, payloads[idx]);
        assert_eq!(fields["offset"], idx.to_string().into_bytes());
        assert_eq!(fields["stream"], b"test_stream");
        assert_eq!(fields["topic"], b"test_topic");
        assert_eq!(fields["partition_id"], b"0");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redis/sink.toml")),
    seed = seeds::connector_stream
)]
async fn metadata_disabled_stores_only_payload(
    harness: &TestHarness,
    fixture: RedisSinkNoMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages = given_json_messages(&[serde_json::json!({"name": "Alice"})]);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let entries = fixture.wait_for_entries(TARGET_KEY, 1).await.unwrap();
    let (_, fields) = &entries[0];
    assert_eq!(fields.len(), 1);
    assert!(fields.contains_key("payload"));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redis/sink.toml")),
    seed = seeds::connector_stream
)]
async fn raw_messages_preserve_binary_payload(harness: &TestHarness, fixture: RedisSinkRawFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let payload = vec![0x00, 0xff, 0x7f, 0x80];
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(payload.clone()))
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

    let entries = fixture.wait_for_entries(TARGET_KEY, 1).await.unwrap();
    assert_eq!(entries[0].1["payload"], payload);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redis/sink.toml")),
    seed = seeds::connector_stream
)]
async fn user_headers_preserve_text_and_binary_values(
    harness: &TestHarness,
    fixture: RedisSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let headers = BTreeMap::from([
        (
            HeaderKey::from_str("content-type").unwrap(),
            HeaderValue::from_str("application/json").unwrap(),
        ),
        (
            HeaderKey::from_str("trace-id").unwrap(),
            HeaderValue::try_from(vec![0xde, 0xad, 0xbe, 0xef]).unwrap(),
        ),
    ]);
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(br#"{"name":"Alice"}"#))
            .user_headers(headers)
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

    let entries = fixture.wait_for_entries(TARGET_KEY, 1).await.unwrap();
    let headers: serde_json::Value =
        serde_json::from_slice(&entries[0].1["headers"]).expect("headers should be JSON");
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(headers["trace-id"]["data"], "3q2+7w==");
    assert_eq!(headers["trace-id"]["iggy_header_encoding"], "base64");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/redis/sink.toml")),
    seed = seeds::connector_stream
)]
async fn existing_entry_id_is_skipped_during_replay(
    harness: &TestHarness,
    fixture: RedisSinkFixture,
) {
    fixture
        .append_entry(TARGET_KEY, "1-0", br#"{"preseeded":true}"#)
        .await
        .expect("entry should be preseeded");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let payloads = [
        serde_json::json!({"preseeded": false}),
        serde_json::json!({"sequence": 2}),
        serde_json::json!({"sequence": 3}),
    ];
    let mut messages = given_json_messages(&payloads);

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let entries = fixture.wait_for_entries(TARGET_KEY, 3).await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, "1-0");
    assert_eq!(entries[0].1["payload"], br#"{"preseeded":true}"#);
    assert_eq!(entries[1].0, "2-0");
    assert_eq!(entries[2].0, "3-0");
}
