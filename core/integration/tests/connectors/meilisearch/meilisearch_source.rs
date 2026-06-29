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

use crate::connectors::fixtures::{MeilisearchOps, MeilisearchSourceFixture};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::sleep;

const TEST_MESSAGE_COUNT: usize = 2;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const SOURCE_INDEX: &str = "iggy_messages";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/meilisearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_index_documents_when_source_polls_should_produce_messages(
    harness: &TestHarness,
    fixture: MeilisearchSourceFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let documents = vec![
        serde_json::json!({"id": 1, "name": "first", "category": "alpha"}),
        serde_json::json!({"id": 2, "name": "second", "category": "beta"}),
    ];

    fixture
        .index_documents(SOURCE_INDEX, documents)
        .await
        .expect("index Meilisearch documents");
    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("wait for Meilisearch documents");

    let received = poll_documents(
        &client,
        &stream_id,
        &topic_id,
        &consumer_id,
        TEST_MESSAGE_COUNT,
    )
    .await;

    assert_eq!(received.len(), TEST_MESSAGE_COUNT);
    assert!(received.iter().any(|document| document["name"] == "first"));
    assert!(received.iter().any(|document| document["name"] == "second"));

    sleep(Duration::from_millis(POLL_INTERVAL_MS * 4)).await;
    let duplicate_poll = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &Consumer::new(consumer_id),
            &PollingStrategy::next(),
            10,
            true,
        )
        .await
        .expect("poll Meilisearch source messages after cursor advance");
    assert!(
        duplicate_poll.messages.is_empty(),
        "second poll should not receive duplicate Meilisearch documents"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/meilisearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_persisted_state_when_connector_restarts_should_resume_after_last_primary_key(
    harness: &mut TestHarness,
    fixture: MeilisearchSourceFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "state_test_consumer".try_into().unwrap();

    fixture
        .index_documents(
            SOURCE_INDEX,
            vec![
                serde_json::json!({"id": 1, "name": "first", "category": "alpha"}),
                serde_json::json!({"id": 2, "name": "second", "category": "beta"}),
            ],
        )
        .await
        .expect("index first Meilisearch document batch");
    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("wait for first Meilisearch document batch");

    let received_before = poll_documents(
        &client,
        &stream_id,
        &topic_id,
        &consumer_id,
        TEST_MESSAGE_COUNT,
    )
    .await;
    assert_eq!(received_before.len(), TEST_MESSAGE_COUNT);

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop Meilisearch source connector");

    fixture
        .index_documents(
            SOURCE_INDEX,
            vec![
                serde_json::json!({"id": 3, "name": "third", "category": "gamma"}),
                serde_json::json!({"id": 4, "name": "fourth", "category": "delta"}),
            ],
        )
        .await
        .expect("index second Meilisearch document batch");
    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT * 2)
        .await
        .expect("wait for second Meilisearch document batch");

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("restart Meilisearch source connector");
    sleep(Duration::from_millis(100)).await;

    let received_after = poll_documents(
        &client,
        &stream_id,
        &topic_id,
        &consumer_id,
        TEST_MESSAGE_COUNT,
    )
    .await;

    assert_eq!(received_after.len(), TEST_MESSAGE_COUNT);
    for document in received_after {
        let id = document.get("id").and_then(|value| value.as_i64());
        assert!(
            id.is_some_and(|id| id > TEST_MESSAGE_COUNT as i64),
            "after restart, received first-batch document: {document}"
        );
    }
}

async fn poll_documents<C>(
    client: &C,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer_id: &Identifier,
    expected_count: usize,
) -> Vec<serde_json::Value>
where
    C: MessageClient + Sync,
{
    let mut received = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                stream_id,
                topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                let json = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                    .expect("Meilisearch source payload should be valid JSON");
                received.push(json);
            }
            if received.len() >= expected_count {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    received
}
