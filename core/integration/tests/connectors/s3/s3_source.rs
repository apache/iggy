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

use std::{collections::HashSet, path::PathBuf, time::Duration};

use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use iggy_connector_sdk::{
    ConnectorState,
    api::{ConnectorRuntimeStats, ConnectorStatus, SourceInfoResponse},
};
use integration::{
    harness::{TestHarness, seeds},
    iggy_harness,
};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{sleep, timeout};

use crate::connectors::fixtures::{
    S3_SOURCE_DATA_KEY, S3_SOURCE_RECORD_COUNT, S3SourceFixture, S3SourceRestartFixture,
};

const API_KEY: &str = "test-api-key";
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Deserialize)]
struct Checkpoint {
    active_object: Option<ActiveObject>,
}

#[derive(Debug, Deserialize)]
struct ActiveObject {
    key: String,
    etag: String,
    size: u64,
    next_byte_offset: u64,
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_preserves_raw_bytes_and_replays_completed_walk_after_restart(
    harness: &TestHarness,
    fixture: S3SourceFixture,
) {
    let first = wait_for_messages(harness, fixture.expected.len()).await;
    assert_eq!(
        first
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<Vec<_>>(),
        fixture.expected
    );
    wait_for_checkpoint(harness, false).await;

    restart(harness).await;
    let replayed = wait_for_messages(harness, fixture.expected.len() * 2).await;
    assert_eq!(&replayed[..first.len()], first.as_slice());
    assert_eq!(&replayed[first.len()..first.len() * 2], first.as_slice());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_resumes_active_object_from_saved_boundary(
    harness: &TestHarness,
    fixture: S3SourceRestartFixture,
) {
    let checkpoint = wait_for_checkpoint(harness, true).await;
    let active = checkpoint.active_object.expect("active object");
    assert_eq!(active.key, S3_SOURCE_DATA_KEY);
    assert!(!active.etag.is_empty());
    assert!(active.next_byte_offset > 0 && active.next_byte_offset < active.size);
    restart(harness).await;
    let messages = wait_for_unique_messages(harness, S3_SOURCE_RECORD_COUNT).await;
    let payloads: HashSet<_> = messages
        .iter()
        .map(|(_, payload)| payload.clone())
        .collect();
    assert_eq!(payloads, fixture.inner.expected.iter().cloned().collect());
    assert_eq!(
        messages
            .iter()
            .filter(|(_, payload)| payload == &fixture.inner.expected[0])
            .count(),
        1,
        "restart must not rewind an already committed first record"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_recovers_restored_missing_object_without_another_restart(
    harness: &mut TestHarness,
    fixture: S3SourceRestartFixture,
) {
    wait_for_checkpoint(harness, true).await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop runtime");
    let checkpoint = tokio::fs::read(state_path(harness))
        .await
        .expect("saved checkpoint");
    let restored = ConnectorState(checkpoint.clone())
        .deserialize::<Checkpoint>("S3 source", 0)
        .expect("checkpoint");
    assert!(restored.active_object.is_some());
    let original = fixture
        .inner
        .bucket
        .get_object(S3_SOURCE_DATA_KEY)
        .await
        .expect("read original object");
    assert_eq!(original.status_code(), 200);
    let deleted = fixture
        .inner
        .bucket
        .delete_object(S3_SOURCE_DATA_KEY)
        .await
        .expect("delete active object");
    assert!((200..300).contains(&deleted.status_code()));

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("restart runtime with missing active object");
    let api = harness.connectors_runtime().expect("runtime").http_url();
    let sources: Vec<SourceInfoResponse> = http_client()
        .get(format!("{api}/sources"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("sources request")
        .json()
        .await
        .expect("sources JSON");
    let source = sources
        .iter()
        .find(|source| source.key == "s3")
        .expect("S3 source");
    assert_eq!(source.status, ConnectorStatus::Running);
    assert_eq!(
        tokio::fs::read(state_path(harness))
            .await
            .expect("checkpoint remains readable"),
        checkpoint
    );
    fixture
        .inner
        .upload(S3_SOURCE_DATA_KEY, original.as_slice())
        .await
        .expect("restore active object");
    let messages = wait_for_unique_messages(harness, S3_SOURCE_RECORD_COUNT).await;
    assert_eq!(
        messages
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<HashSet<_>>(),
        fixture.inner.expected.iter().cloned().collect()
    );
    assert_eq!(
        messages
            .iter()
            .filter(|(_, payload)| payload == &fixture.inner.expected[0])
            .count(),
        1,
        "recovery must not rewind an already committed first record"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_preserves_checkpoint_during_state_save_failure(
    harness: &TestHarness,
    fixture: S3SourceRestartFixture,
) {
    wait_for_checkpoint(harness, true).await;
    let path = state_path(harness);
    let directory = path.parent().expect("state directory");
    let unavailable = directory.with_extension("s3-unavailable");
    let http = http_client();
    let api = harness.connectors_runtime().expect("runtime").http_url();
    let errors_before = source_errors(&http, &api).await;
    tokio::fs::rename(directory, &unavailable)
        .await
        .expect("make state unavailable");
    let preserved_path = unavailable.join("source_s3.state");
    let before = tokio::fs::read(&preserved_path)
        .await
        .expect("saved checkpoint");
    let failure = timeout(WAIT_TIMEOUT, async {
        while source_errors(&http, &api).await <= errors_before {
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await;
    let after = tokio::fs::read(&preserved_path)
        .await
        .expect("checkpoint remains readable");
    tokio::fs::rename(&unavailable, directory)
        .await
        .expect("restore state directory");
    failure.expect("state failure should be reported");
    assert_eq!(
        before, after,
        "failed save must not advance persisted progress"
    );
    // The SDK can stop after five NACKs; an explicit restart is valid recovery.
    restart(harness).await;
    let messages = wait_for_unique_messages(harness, S3_SOURCE_RECORD_COUNT).await;
    assert_eq!(
        messages
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<HashSet<_>>(),
        fixture.inner.expected.iter().cloned().collect()
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_restarts_replacement_without_splicing_old_offsets(
    harness: &TestHarness,
    fixture: S3SourceRestartFixture,
) {
    wait_for_checkpoint(harness, true).await;
    fixture
        .inner
        .upload(S3_SOURCE_DATA_KEY, b"replacement-first||replacement-last")
        .await
        .expect("replace object");
    timeout(WAIT_TIMEOUT, async {
        loop {
            let messages = read_messages(harness).await;
            if messages
                .iter()
                .any(|(_, payload)| payload == b"replacement-last")
            {
                assert!(
                    messages
                        .iter()
                        .any(|(_, payload)| payload == b"replacement-first"),
                    "replacement must start at byte zero"
                );
                break;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("replacement should be read");
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/s3/source.toml")),
    seed = seeds::connector_stream
)]
async fn s3_source_replays_unacknowledged_records_after_iggy_send_failure(
    harness: &mut TestHarness,
    fixture: S3SourceRestartFixture,
) {
    wait_for_checkpoint(harness, true).await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start runtime");
    let api = harness.connectors_runtime().expect("runtime").http_url();
    let http = http_client();
    let errors_before = source_errors(&http, &api).await;
    harness.kill_node(0).expect("stop Iggy");
    timeout(WAIT_TIMEOUT, async {
        while source_errors(&http, &api).await <= errors_before {
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("failed send should be reported");
    let checkpoint = tokio::fs::read(state_path(harness))
        .await
        .expect("checkpoint");
    let errors_after = source_errors(&http, &api).await;
    timeout(WAIT_TIMEOUT, async {
        while source_errors(&http, &api).await <= errors_after {
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("another failed send should be reported");
    assert_eq!(
        tokio::fs::read(state_path(harness))
            .await
            .expect("checkpoint"),
        checkpoint
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop runtime");
    harness.restart_node(0).expect("restart Iggy");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("runtime")
        .clear_iggy_connection_options();
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start runtime");
    let messages = wait_for_unique_messages(harness, S3_SOURCE_RECORD_COUNT).await;
    assert_eq!(
        messages
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<HashSet<_>>(),
        fixture.inner.expected.iter().cloned().collect()
    );
}

async fn read_messages(harness: &TestHarness) -> Vec<(u128, Vec<u8>)> {
    let client = harness.root_client().await.expect("root client");
    let stream: Identifier = seeds::names::STREAM.try_into().expect("stream");
    let topic: Identifier = seeds::names::TOPIC.try_into().expect("topic");
    let consumer = Consumer::new("s3-test-reader".try_into().expect("consumer"));
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            1000,
            false,
        )
        .await
        .expect("poll Iggy");
    polled
        .messages
        .into_iter()
        .map(|message| (message.header.id, message.payload.to_vec()))
        .collect()
}

async fn wait_for_messages(harness: &TestHarness, count: usize) -> Vec<(u128, Vec<u8>)> {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let messages = read_messages(harness).await;
            if messages.len() >= count {
                return messages;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("source messages should arrive")
}

async fn wait_for_unique_messages(harness: &TestHarness, count: usize) -> Vec<(u128, Vec<u8>)> {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let messages = read_messages(harness).await;
            if messages
                .iter()
                .map(|(id, _)| id)
                .collect::<HashSet<_>>()
                .len()
                >= count
            {
                return messages;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("all source records should arrive")
}

fn state_path(harness: &TestHarness) -> PathBuf {
    harness
        .connectors_runtime()
        .expect("runtime")
        .state_path()
        .join("source_s3.state")
}

async fn wait_for_checkpoint(harness: &TestHarness, active: bool) -> Checkpoint {
    timeout(WAIT_TIMEOUT, async {
        loop {
            if let Ok(bytes) = tokio::fs::read(state_path(harness)).await
                && let Some(checkpoint) =
                    ConnectorState(bytes).deserialize::<Checkpoint>("S3 source", 0)
                && if active {
                    checkpoint.active_object.as_ref().is_some_and(|object| {
                        object.next_byte_offset > 20 && object.next_byte_offset < object.size
                    })
                } else {
                    checkpoint.active_object.is_none()
                }
            {
                return checkpoint;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("expected checkpoint should be persisted")
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client")
}

async fn restart(harness: &TestHarness) {
    let api = harness.connectors_runtime().expect("runtime").http_url();
    let response = http_client()
        .post(format!("{api}/sources/s3/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("restart request");
    assert_eq!(response.status().as_u16(), 204);
}

async fn source_errors(http: &Client, api: &str) -> u64 {
    let stats: ConnectorRuntimeStats = http
        .get(format!("{api}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("stats")
        .json()
        .await
        .expect("stats JSON");
    stats
        .connectors
        .iter()
        .find(|connector| connector.key == "s3")
        .expect("S3 stats")
        .errors
}
