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

use crate::connectors::fixtures::{DynamoDbOps, DynamoDbSinkFixture, DynamoDbSinkSortKeyFixture};
use aws_sdk_dynamodb::types::AttributeValue;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/dynamodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_messages_when_sink_consumes_should_write_items(
    harness: &TestHarness,
    fixture: DynamoDbSinkFixture,
) {
    let payloads = [
        serde_json::json!({"name": "first", "count": 1}),
        serde_json::json!({"name": "second", "count": 2}),
    ];
    send_messages(harness, &payloads).await;

    let items = fixture
        .wait_for_items(payloads.len())
        .await
        .expect("wait for DynamoDB items");

    assert_eq!(items.len(), payloads.len());
    assert_eq!(
        string_attribute(&items[0], "iggy_stream"),
        seeds::names::STREAM
    );
    assert_eq!(
        string_attribute(&items[0], "iggy_topic"),
        seeds::names::TOPIC
    );
    assert!(
        items
            .iter()
            .any(|item| string_attribute(item, "name") == "first")
    );
    assert!(
        items
            .iter()
            .any(|item| string_attribute(item, "name") == "second")
    );
    assert!(
        items
            .iter()
            .all(|item| !string_attribute(item, "iggy_id").is_empty())
    );
    assert!(
        items
            .iter()
            .any(|item| number_attribute(item, "count") == "1")
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/dynamodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_table_with_sort_key_when_sink_consumes_should_write_offset_as_sort_key(
    harness: &TestHarness,
    fixture: DynamoDbSinkSortKeyFixture,
) {
    let payloads = [
        serde_json::json!({"name": "first"}),
        serde_json::json!({"name": "second"}),
    ];
    send_messages(harness, &payloads).await;

    let items = fixture
        .wait_for_items(payloads.len())
        .await
        .expect("wait for DynamoDB items");

    let mut offsets = items
        .iter()
        .map(|item| number_attribute(item, "iggy_offset").to_owned())
        .collect::<Vec<_>>();
    offsets.sort();
    assert_eq!(offsets, vec!["0".to_owned(), "1".to_owned()]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/dynamodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_more_messages_than_a_batch_write_when_sink_consumes_should_write_every_item(
    harness: &TestHarness,
    fixture: DynamoDbSinkFixture,
) {
    let payloads = (0..30)
        .map(|index| serde_json::json!({"name": format!("message-{index}"), "count": index}))
        .collect::<Vec<_>>();
    send_messages(harness, &payloads).await;

    let items = fixture
        .wait_for_items(payloads.len())
        .await
        .expect("wait for DynamoDB items");

    assert_eq!(items.len(), payloads.len());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/dynamodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_the_same_message_ids_when_sink_consumes_twice_should_overwrite_the_items(
    harness: &TestHarness,
    fixture: DynamoDbSinkFixture,
) {
    let first = [
        serde_json::json!({"name": "first"}),
        serde_json::json!({"name": "second"}),
    ];
    send_messages(harness, &first).await;
    fixture
        .wait_for_items(first.len())
        .await
        .expect("wait for DynamoDB items");

    let second = [
        serde_json::json!({"name": "third"}),
        serde_json::json!({"name": "fourth"}),
    ];
    send_messages(harness, &second).await;

    let items = wait_for_names(&fixture, &["third", "fourth"]).await;
    assert_eq!(items.len(), first.len());
}

/// The second batch reuses the message ids of the first one, so the item count
/// stays the same and only the payload tells the two rounds apart.
async fn wait_for_names(
    fixture: &DynamoDbSinkFixture,
    names: &[&str],
) -> Vec<HashMap<String, AttributeValue>> {
    for _ in 0..100 {
        let items = fixture.scan_items().await.expect("scan DynamoDB items");
        if names.iter().all(|name| {
            items
                .iter()
                .any(|item| string_attribute(item, "name") == *name)
        }) {
            return items;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("DynamoDB items never carried the names: {names:?}");
}

async fn send_messages(harness: &TestHarness, payloads: &[serde_json::Value]) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).expect("serialize")))
                .build()
                .expect("build message")
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
        .expect("send messages");
}

fn string_attribute<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    match item.get(field) {
        Some(AttributeValue::S(value)) => value,
        _ => "",
    }
}

fn number_attribute<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    match item.get(field) {
        Some(AttributeValue::N(value)) => value,
        _ => "",
    }
}
