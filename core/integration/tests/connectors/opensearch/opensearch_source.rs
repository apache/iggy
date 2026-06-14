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
    OpenSearchSourceMissingIndexFixture, OpenSearchSourcePreCreatedFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn opensearch_source_produces_messages_to_iggy(
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

    for (i, record) in received.iter().enumerate() {
        let expected_id = (i + 1) as i64;
        let expected_name = format!("doc_{}", i + 1);

        assert_eq!(
            record.get("id").and_then(|v| v.as_i64()),
            Some(expected_id),
            "ID mismatch at record {i}"
        );
        assert_eq!(
            record.get("name").and_then(|v| v.as_str()),
            Some(expected_name.as_str()),
            "Name mismatch at record {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn opensearch_source_handles_empty_index(
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
async fn opensearch_source_produces_bulk_messages(
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
async fn state_persists_across_connector_restart(
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

    let mut received_after: Vec<serde_json::Value> = Vec::new();
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
                    received_after.push(json);
                }
            }
            if received_after.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(received_after.len(), TEST_MESSAGE_COUNT);

    for record in &received_after {
        let id = record.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        assert!(
            id > TEST_MESSAGE_COUNT as i64,
            "After restart, got ID {id} from first batch"
        );
    }
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

/// Negative path: the configured index does not exist, so `open()` must
/// fail with a `Storage` error and the runtime reports the source as
/// `ConnectorStatus::Error` without aborting.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn opensearch_source_with_missing_index_reports_error(
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
        last_error.message.contains("does not exist"),
        "last_error should mention the missing index, got: {}",
        last_error.message
    );
}
