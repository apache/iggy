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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::fixtures::{
    OpenSearchSourceMissingIndexFixture, OpenSearchSourcePreCreatedFixture,
    OpenSearchSourceSmallBatchFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

fn document_ids(messages: &[serde_json::Value]) -> HashSet<i64> {
    messages
        .iter()
        .filter_map(|record| record.get("id").and_then(|value| value.as_i64()))
        .collect()
}

fn assert_contains_document_ids(messages: &[serde_json::Value], expected_ids: &[i64]) {
    let ids = document_ids(messages);
    for expected_id in expected_ids {
        assert!(
            ids.contains(expected_id),
            "expected document id {expected_id}, got ids {ids:?}"
        );
    }
}

async fn poll_all_messages_from_offset_zero(
    client: &impl MessageClient,
    consumer_id: &Identifier,
    min_messages: usize,
) -> Vec<serde_json::Value> {
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
                &PollingStrategy::offset(0),
                100,
                false,
            )
            .await
        {
            received.clear();
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= min_messages {
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
async fn given_documents_in_index_when_connector_polls_should_produce_messages(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();

    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to insert documents");

    let doc_count = fixture
        .get_document_count()
        .await
        .expect("Failed to get document count");
    assert_eq!(
        doc_count, TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} documents in OpenSearch"
    );

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
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
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let expected_ids: Vec<i64> = (1..=TEST_MESSAGE_COUNT as i64).collect();
    assert_contains_document_ids(&received, &expected_ids);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_more_documents_than_batch_size_when_connector_polls_should_fetch_all(
    harness: &TestHarness,
    fixture: OpenSearchSourceSmallBatchFixture,
) {
    const DOC_COUNT: usize = 6;

    let client = harness.root_client().await.unwrap();

    fixture
        .insert_documents(DOC_COUNT)
        .await
        .expect("Failed to insert documents");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "pagination_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
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
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= DOC_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= DOC_COUNT,
        "Expected at least {DOC_COUNT} messages across search_after pages, got {}",
        received.len()
    );

    let expected_ids: Vec<i64> = (1..=DOC_COUNT as i64).collect();
    assert_contains_document_ids(&received, &expected_ids);

    // Wait for at least one empty poll to fire (connector catches up to end of index).
    // A cursor-reset bug would cause the connector to re-fetch all docs on the next empty poll.
    sleep(Duration::from_millis(POLL_INTERVAL_MS * 5)).await;

    let audit_consumer: Identifier = "pagination_audit".try_into().unwrap();
    let all_on_stream =
        poll_all_messages_from_offset_zero(&client, &audit_consumer, DOC_COUNT).await;

    let all_ids: Vec<i64> = all_on_stream
        .iter()
        .filter_map(|record| record.get("id").and_then(|v| v.as_i64()))
        .collect();
    let unique_count = document_ids(&all_on_stream).len();
    assert_eq!(
        all_ids.len(),
        unique_count,
        "stream contains duplicate document IDs after empty poll; cursor was reset"
    );
    assert_eq!(
        unique_count, DOC_COUNT,
        "expected exactly {DOC_COUNT} unique documents on stream, got {unique_count}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_empty_index_when_connector_polls_should_not_fail(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();

    let doc_count = fixture
        .get_document_count()
        .await
        .expect("Failed to get document count");
    assert_eq!(doc_count, 0, "Expected empty index");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    sleep(Duration::from_millis(100)).await;

    let polled = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &Consumer::new(consumer_id),
            &PollingStrategy::next(),
            10,
            false,
        )
        .await;

    assert!(
        polled.is_ok(),
        "Should be able to poll from topic even with empty source"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_documents_when_connector_polls_should_produce_all_messages(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();
    let bulk_count = 10;

    fixture
        .insert_documents(bulk_count)
        .await
        .expect("Failed to insert documents");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= bulk_count {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= bulk_count,
        "Expected at least {bulk_count} messages, got {}",
        received.len()
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_runtime_state_when_connector_restarts_should_resume_after_cursor(
    harness: &mut TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to insert first batch");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "state_test_consumer".try_into().unwrap();

    let client = harness.root_client().await.unwrap();
    let received_before = {
        let mut received: Vec<serde_json::Value> = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
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
                for msg in polled.messages {
                    if let Ok(json) = serde_json::from_slice(&msg.payload) {
                        received.push(json);
                    }
                }
                if received.len() >= TEST_MESSAGE_COUNT {
                    break;
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        received
    };
    assert_eq!(received_before.len(), TEST_MESSAGE_COUNT);

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");

    let second_batch_start_id = (TEST_MESSAGE_COUNT + 1) as i32;
    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_document(
                second_batch_start_id + i as i32,
                &format!("doc_batch2_{i}"),
                (TEST_MESSAGE_COUNT + i) as i32 * 10,
            )
            .await
            .expect("Failed to insert document");
    }
    fixture
        .refresh_index()
        .await
        .expect("Failed to refresh index");

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");
    sleep(Duration::from_millis(100)).await;

    let audit_consumer: Identifier = "state_audit_consumer".try_into().unwrap();
    let all_messages =
        poll_all_messages_from_offset_zero(&client, &audit_consumer, TEST_MESSAGE_COUNT * 2).await;

    let batch1_ids: HashSet<i64> = (1..=TEST_MESSAGE_COUNT as i64).collect();
    let batch1_occurrences = all_messages
        .iter()
        .filter_map(|record| record.get("id").and_then(|value| value.as_i64()))
        .filter(|id| batch1_ids.contains(id))
        .count();
    assert_eq!(
        batch1_occurrences, TEST_MESSAGE_COUNT,
        "batch 1 IDs must appear exactly once on the stream; duplicates mean cursor reset"
    );

    let batch2_ids: HashSet<i64> =
        ((TEST_MESSAGE_COUNT + 1) as i64..=(TEST_MESSAGE_COUNT * 2) as i64).collect();
    let batch2_seen: HashSet<i64> = all_messages
        .iter()
        .filter_map(|record| record.get("id").and_then(|value| value.as_i64()))
        .filter(|id| batch2_ids.contains(id))
        .collect();
    assert_eq!(
        batch2_seen.len(),
        TEST_MESSAGE_COUNT,
        "batch 2 IDs must be present after restart, got {batch2_seen:?}"
    );
}

async fn fetch_sources(http_client: &Client, api_address: &str) -> Vec<SourceInfoResponse> {
    let response = http_client
        .get(format!("{api_address}/sources"))
        .send()
        .await
        .expect("Failed to query /sources");
    assert_eq!(response.status(), 200);
    response.json().await.expect("Failed to parse sources")
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_missing_index_when_connector_opens_should_report_error(
    harness: &TestHarness,
    _fixture: OpenSearchSourceMissingIndexFixture,
) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let mut sources = fetch_sources(&http_client, &api_address).await;
    for _ in 0..POLL_ATTEMPTS {
        if sources
            .iter()
            .any(|source| source.status == ConnectorStatus::Error)
        {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        sources = fetch_sources(&http_client, &api_address).await;
    }

    assert_eq!(sources.len(), 1, "Expected a single configured source");
    let source = &sources[0];
    assert_eq!(source.status, ConnectorStatus::Error);
    let last_error = source
        .last_error
        .as_ref()
        .expect("Source with missing index should expose a last_error");
    assert!(
        last_error.message.contains("Plugin initialization failed"),
        "missing index should fail during plugin open, got: {}",
        last_error.message
    );
}
