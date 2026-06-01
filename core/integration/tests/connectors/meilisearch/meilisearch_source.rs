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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/meilisearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn meilisearch_source_produces_index_documents(
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
        .index_documents("iggy_messages", documents)
        .await
        .expect("index Meilisearch documents");
    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("wait for Meilisearch documents");

    let mut received = Vec::new();
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
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(received.len(), TEST_MESSAGE_COUNT);
    assert!(received.iter().any(|document| document["name"] == "first"));
    assert!(received.iter().any(|document| document["name"] == "second"));
}
