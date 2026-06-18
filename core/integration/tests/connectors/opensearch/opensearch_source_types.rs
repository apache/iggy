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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::fixtures::{
    OpenSearchSourcePreCreatedFixture, OpenSearchSourceTypedFieldsFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

async fn poll_json_messages(
    client: &impl MessageClient,
    consumer_id: &Identifier,
    limit: u32,
) -> Vec<Value> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let mut received = Vec::new();

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                limit,
                true,
            )
            .await
        {
            for message in polled.messages {
                if let Ok(json) = serde_json::from_slice::<Value>(&message.payload) {
                    received.push(json);
                }
            }
            if !received.is_empty() {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    received
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_document_in_index_when_connector_polls_should_expose_payload_structure(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_document(1, "structure_doc", 42)
        .await
        .expect("insert document");
    fixture.refresh_index().await.expect("refresh index");

    let client = harness.root_client().await.unwrap();
    let consumer_id: Identifier = "payload_structure_consumer".try_into().unwrap();
    let messages = poll_json_messages(&client, &consumer_id, 10).await;

    assert_eq!(
        messages.len(),
        1,
        "expected one message, got {}",
        messages.len()
    );
    let record = &messages[0];
    assert_eq!(record["id"], 1);
    assert_eq!(record["name"], "structure_doc");
    assert_eq!(record["value"], 42);
    assert!(record.get("timestamp").is_some(), "missing timestamp field");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_typed_fields_document_when_connector_polls_should_round_trip_payload(
    harness: &TestHarness,
    fixture: OpenSearchSourceTypedFieldsFixture,
) {
    fixture
        .insert_typed_sample_document()
        .await
        .expect("insert typed document");

    let client = harness.root_client().await.unwrap();
    let consumer_id: Identifier = "typed_fields_consumer".try_into().unwrap();
    let messages = poll_json_messages(&client, &consumer_id, 10).await;

    assert_eq!(messages.len(), 1, "expected one typed message");
    let record = &messages[0];
    assert_eq!(record["title"], "OpenSearch typed field coverage");
    assert_eq!(record["status"], "active");
    assert_eq!(record["count"].as_i64(), Some(9_223_372_036_854_775_807));
    assert!((record["score"].as_f64().unwrap() - 98.6).abs() < 0.01);
    assert!((record["ratio"].as_f64().unwrap() - 0.125).abs() < f64::EPSILON);
    assert_eq!(record["active"], true);
    assert_eq!(record["client_ip"], "192.168.1.42");
    assert_eq!(record["location"]["lat"], 40.12);
    assert_eq!(record["location"]["lon"], -71.34);
    assert!(record["tags"].is_array());
    assert!(record["optional_note"].is_null());
    assert!(record.get("timestamp").is_some());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_first_batch_polled_when_second_batch_inserted_should_not_duplicate(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("insert first batch");

    let client = harness.root_client().await.unwrap();
    let consumer_id: Identifier = "cursor_consumer".try_into().unwrap();

    let first_batch = poll_json_messages(&client, &consumer_id, 10).await;
    assert_eq!(first_batch.len(), TEST_MESSAGE_COUNT);
    let first_ids: HashSet<i64> = first_batch
        .iter()
        .filter_map(|record| record.get("id").and_then(Value::as_i64))
        .collect();

    let second_batch_start_id = (TEST_MESSAGE_COUNT + 1) as i32;
    for offset in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_document(
                second_batch_start_id + offset as i32,
                &format!("batch_two_{offset}"),
                (100 + offset) as i32,
            )
            .await
            .expect("insert second batch document");
    }
    fixture.refresh_index().await.expect("refresh index");

    let mut second_batch = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        let polled = poll_json_messages(&client, &consumer_id, 10).await;
        for record in polled {
            let id = record.get("id").and_then(Value::as_i64).unwrap_or(0);
            if !first_ids.contains(&id) {
                second_batch.push(record);
            }
        }
        if second_batch.len() >= TEST_MESSAGE_COUNT {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(
        second_batch.len(),
        TEST_MESSAGE_COUNT,
        "expected second batch only"
    );
    for record in &second_batch {
        let id = record.get("id").and_then(Value::as_i64).unwrap_or(0);
        assert!(
            id > TEST_MESSAGE_COUNT as i64,
            "duplicate or first-batch id in second batch: {id}"
        );
    }
}
