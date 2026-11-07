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

use super::{
    CHECKPOINT_FILE, POLL_ATTEMPTS, POLL_INTERVAL_MS, SHARED_TIMESTAMP, TEST_MESSAGE_COUNT,
    expected_ids, json_payloads, poll_next, read_from_start, sorted_document_ids,
    wait_for_failed_checkpoint,
};
use crate::connectors::fixtures::{
    OpenSearchSourceMissingIndexFixture, OpenSearchSourcePreCreatedFixture,
    OpenSearchSourceSmallBatchFixture, OpenSearchSourceWildcardFixture,
};
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

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
    assert_eq!(
        fixture.get_document_count().await.unwrap(),
        TEST_MESSAGE_COUNT
    );

    let messages = poll_next(&client, "test_consumer", TEST_MESSAGE_COUNT).await;

    assert_eq!(
        sorted_document_ids(&json_payloads(&messages)),
        expected_ids(1, TEST_MESSAGE_COUNT)
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_more_documents_than_batch_size_when_connector_polls_should_fetch_each_once(
    harness: &TestHarness,
    fixture: OpenSearchSourceSmallBatchFixture,
) {
    const DOC_COUNT: usize = 6;

    let client = harness.root_client().await.unwrap();

    fixture
        .insert_documents(DOC_COUNT)
        .await
        .expect("Failed to insert documents");

    let received = poll_next(&client, "pagination_consumer", DOC_COUNT).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&received)),
        expected_ids(1, DOC_COUNT)
    );

    // Give the connector time to run an empty poll: a cursor reset would re-fetch everything.
    sleep(Duration::from_millis(POLL_INTERVAL_MS * 5)).await;

    let on_stream = read_from_start(&client, "pagination_audit", DOC_COUNT).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&on_stream)),
        expected_ids(1, DOC_COUNT),
        "every document must be on the stream exactly once"
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

    let client = harness.root_client().await.unwrap();
    let before_restart = read_from_start(&client, "state_before", TEST_MESSAGE_COUNT).await;
    assert_eq!(before_restart.len(), TEST_MESSAGE_COUNT);

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");
    fixture
        .insert_documents_from(TEST_MESSAGE_COUNT + 1, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to insert second batch");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");

    let all_messages = read_from_start(&client, "state_audit", TEST_MESSAGE_COUNT * 2).await;

    assert_eq!(
        sorted_document_ids(&json_payloads(&all_messages)),
        expected_ids(1, TEST_MESSAGE_COUNT * 2),
        "first batch must not be re-read and second batch must arrive after restart"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn checkpoint_failure_source_replays_documents_until_acknowledged(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    // A directory where the runtime renames its checkpoint makes every state save fail.
    let checkpoint = harness
        .connectors_runtime()
        .expect("connector runtime")
        .state_path()
        .join(CHECKPOINT_FILE);
    fs::create_dir(&checkpoint).expect("force checkpoint rename failure");

    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to insert documents");

    let checkpoint_failed = wait_for_failed_checkpoint(harness).await;
    fs::remove_dir(&checkpoint).expect("restore checkpoint destination");
    assert!(checkpoint_failed, "the produced batch must receive a Nack");

    for _ in 0..POLL_ATTEMPTS {
        if checkpoint.is_file() {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert!(
        checkpoint.is_file(),
        "checkpoint must recover after the failure"
    );

    let client = harness.root_client().await.unwrap();
    let replayed = read_from_start(&client, "checkpoint_replay", TEST_MESSAGE_COUNT * 2).await;
    let ids = sorted_document_ids(&json_payloads(&replayed));
    for id in expected_ids(1, TEST_MESSAGE_COUNT) {
        let copies = ids.iter().filter(|published| **published == id).count();
        assert!(
            copies >= 2,
            "a Nacked document must be replayed; document {id} was published {copies} time(s)"
        );
    }

    sleep(Duration::from_millis(POLL_INTERVAL_MS * 5)).await;
    let settled = read_from_start(&client, "checkpoint_settled", replayed.len()).await;
    assert_eq!(
        settled.len(),
        replayed.len(),
        "once the checkpoint is saved the cursor must stop replaying"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_late_visible_document_with_smaller_id_when_polling_should_still_publish_it(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();

    fixture
        .insert_document_at(20, SHARED_TIMESTAMP)
        .await
        .expect("Failed to insert the first document");
    let first = poll_next(&client, "late_visible", 1).await;
    assert_eq!(sorted_document_ids(&json_payloads(&first)), vec![20]);

    fixture
        .insert_document_at(10, SHARED_TIMESTAMP)
        .await
        .expect("Failed to insert the late document");
    let late = poll_next(&client, "late_visible", 1).await;

    assert_eq!(
        sorted_document_ids(&json_payloads(&late)),
        vec![10],
        "a document that sorts before the saved position must not be skipped"
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

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_wildcard_index_matching_nothing_when_index_appears_should_produce_messages(
    harness: &TestHarness,
    fixture: OpenSearchSourceWildcardFixture,
) {
    let client = harness.root_client().await.unwrap();

    fixture.setup_index().await.expect("Failed to create index");
    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to insert documents");

    let messages = poll_next(&client, "wildcard_consumer", TEST_MESSAGE_COUNT).await;

    assert_eq!(
        sorted_document_ids(&json_payloads(&messages)),
        expected_ids(1, TEST_MESSAGE_COUNT)
    );
}
