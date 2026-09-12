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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::ElasticsearchSinkFixture;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const LOG_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn elasticsearch_sink_stores_json_messages(
    harness: &TestHarness,
    fixture: ElasticsearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
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
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to wait for documents");

    fixture
        .refresh_index()
        .await
        .expect("Failed to refresh index");

    let search_result = fixture
        .search_documents()
        .await
        .expect("Failed to search documents");

    assert_eq!(
        search_result.hits.total.value, TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} documents in Elasticsearch"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn elasticsearch_sink_handles_bulk_messages(
    harness: &TestHarness,
    fixture: ElasticsearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let bulk_count = 50;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(bulk_count);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
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
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_documents(bulk_count)
        .await
        .expect("Failed to wait for documents");

    let doc_count = fixture
        .get_document_count()
        .await
        .expect("Failed to get document count");

    assert!(
        doc_count >= bulk_count,
        "Expected at least {bulk_count} documents, got {doc_count}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn elasticsearch_sink_preserves_json_structure(
    harness: &TestHarness,
    fixture: ElasticsearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let complex_messages: Vec<serde_json::Value> = vec![
        serde_json::json!({
            "user": {"name": "Alice", "age": 30},
            "tags": ["rust", "iggy"],
            "active": true
        }),
        serde_json::json!({
            "nested": {"deep": {"value": 42}},
            "array": [1, 2, 3],
            "nullable": null
        }),
        serde_json::json!({
            "float": 1.2345,
            "negative": -100,
            "empty_object": {},
            "empty_array": []
        }),
    ];

    let mut messages: Vec<IggyMessage> = complex_messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
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
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_documents(complex_messages.len())
        .await
        .expect("Failed to wait for documents");

    fixture
        .refresh_index()
        .await
        .expect("Failed to refresh index");

    let search_result = fixture
        .search_documents()
        .await
        .expect("Failed to search documents");

    assert_eq!(
        search_result.hits.total.value,
        complex_messages.len(),
        "Expected {} documents",
        complex_messages.len()
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_rejected_document_when_indexing_should_report_zero_indexed(
    harness: &TestHarness,
    fixture: ElasticsearchSinkFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("stream ID");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("topic ID");
    let mut message = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from_static(b"[1,2]"))
        .build()
        .expect("array payload message");
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            std::slice::from_mut(&mut message),
        )
        .await
        .expect("send rejected document");

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let logs = timeout(LOG_WAIT_TIMEOUT, async {
        loop {
            let (stdout, stderr) = runtime.collect_logs();
            let logs = format!("{stdout}\n{stderr}");
            if logs.contains("Successfully indexed") {
                break logs;
            }
            sleep(LOG_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("bulk completion should be logged");

    assert!(logs.contains("Document indexing error:"), "{logs}");
    assert_eq!(
        fixture.get_document_count().await.expect("document count"),
        0,
        "Elasticsearch must reject the non-object document"
    );
    assert!(
        logs.contains("Successfully indexed 0 documents"),
        "the indexed count must exclude the rejected document: {logs}"
    );
}
