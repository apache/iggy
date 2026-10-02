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

use crate::connectors::fixtures::{
    JdbcBulkFixture, JdbcBulkOverflowFixture, JdbcBulkRowsFixture, JdbcIncrementalFixture,
    JdbcLargeResultFixture, JdbcMetadataFixture, JdbcQueryTimeoutFixture, JdbcRecoveryFixture,
    JdbcTextCursorFixture, JdbcTieBoundaryFixture,
};
use iggy::prelude::IggyClient;
use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const POLL_BATCH: u32 = 500;

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn bulk_query_produces_message_to_iggy(harness: &TestHarness, _fixture: JdbcBulkFixture) {
    let client = harness.root_client().await.expect("root client");
    let messages = poll_json_messages(&client, "jdbc_bulk", 1).await;
    assert!(!messages.is_empty(), "expected a JDBC message");

    let first = &messages[0];
    assert_eq!(
        first.get("operation_type").and_then(|value| value.as_str()),
        Some("SELECT")
    );
    let data = first.get("data").expect("metadata should contain data");
    assert_eq!(data.get("id").and_then(|value| value.as_i64()), Some(1));
    assert_eq!(
        data.get("name").and_then(|value| value.as_str()),
        Some("test")
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn bulk_query_produces_multiple_rows_to_iggy(
    harness: &TestHarness,
    _fixture: JdbcBulkRowsFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let messages = poll_json_messages(&client, "jdbc_bulk_rows", 3).await;
    assert!(messages.len() >= 3, "expected three JDBC messages");

    for message in &messages[..3] {
        let data = message.get("data").expect("metadata should contain data");
        assert!(data.get("id").is_some());
        assert!(data.get("name").is_some());
        assert!(data.get("active").is_some());
    }
    let first = messages[0].get("data").unwrap();
    assert_eq!(first.get("id").and_then(|value| value.as_i64()), Some(1));
    assert_eq!(
        first.get("name").and_then(|value| value.as_str()),
        Some("alice")
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn source_includes_metadata_fields_when_enabled(
    harness: &TestHarness,
    _fixture: JdbcMetadataFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let messages = poll_json_messages(&client, "jdbc_metadata", 1).await;
    let message = messages.first().expect("expected a JDBC message");
    assert!(message.get("timestamp").is_some());
    assert!(message.get("operation_type").is_some());
    assert!(message.get("data").is_some());
    assert!(message.get("table_name").is_some());
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn incremental_mode_advances_offset_across_polls(
    harness: &TestHarness,
    fixture: JdbcIncrementalFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let consumer = consumer("jdbc_incremental");
    let (first_ids, first_received) =
        poll_until_ids_seen(&client, &consumer, &[1, 2, 3], POLL_TIMEOUT).await;
    assert_eq!(
        first_ids,
        vec![1, 2, 3],
        "expected ids 1,2,3, got {first_ids:?} from {first_received} messages"
    );

    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    sqlx::query("INSERT INTO inc_test (id, name) VALUES (4, 'd'), (5, 'e')")
        .execute(&pool)
        .await
        .expect("insert additional rows");
    pool.close().await;

    let (second_ids, second_received) =
        poll_until_ids_seen(&client, &consumer, &[4, 5], POLL_TIMEOUT).await;
    assert_eq!(
        second_ids,
        vec![4, 5],
        "expected only ids 4,5, got {second_ids:?} from {second_received} messages"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn incremental_text_offset_with_backslash_preserves_cursor_boundary(
    harness: &TestHarness,
    _fixture: JdbcTextCursorFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let (ids, received) =
        poll_until_ids_seen(&client, &consumer("jdbc_text_cursor"), &[2], POLL_TIMEOUT).await;
    assert_eq!(
        ids,
        vec![2],
        "expected only the row after the exact cursor, got {ids:?} from {received} messages"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn large_result_set_streams_without_crashing(
    harness: &TestHarness,
    _fixture: JdbcLargeResultFixture,
) {
    let client = harness.root_client().await.expect("root client");
    let messages = poll_json_messages(&client, "jdbc_large_result", 150).await;
    assert!(messages.len() >= 150, "expected at least 150 messages");
    for message in &messages[..150] {
        let data = message.get("data").expect("metadata should contain data");
        assert!(data.get("id").and_then(|value| value.as_i64()).is_some());
        assert!(data.get("name").and_then(|value| value.as_str()).is_some());
    }
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn source_recovers_after_repeated_query_errors(
    harness: &TestHarness,
    fixture: JdbcRecoveryFixture,
) {
    assert_source_running(harness).await;
    sleep(Duration::from_millis(400)).await;

    let pool = fixture.create_pool().await.expect("PostgreSQL pool");
    sqlx::query("CREATE TABLE recover_test (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create recovery table");
    sqlx::query("INSERT INTO recover_test (id, name) VALUES (1, 'a'), (2, 'b')")
        .execute(&pool)
        .await
        .expect("insert recovery rows");
    pool.close().await;

    let client = harness.root_client().await.expect("root client");
    let (ids, received) =
        poll_until_ids_seen(&client, &consumer("jdbc_recovery"), &[1, 2], POLL_TIMEOUT).await;
    assert_eq!(
        ids,
        vec![1, 2],
        "source did not recover: got {ids:?} from {received} messages"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn bulk_result_larger_than_batch_size_fails_closed(
    harness: &TestHarness,
    _fixture: JdbcBulkOverflowFixture,
) {
    assert_source_running(harness).await;
    assert_no_messages_for(harness, "jdbc_bulk_overflow", Duration::from_millis(600)).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn incremental_tie_at_batch_boundary_fails_closed(
    harness: &TestHarness,
    _fixture: JdbcTieBoundaryFixture,
) {
    assert_source_running(harness).await;
    assert_no_messages_for(harness, "jdbc_tie_boundary", Duration::from_millis(600)).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/jdbc/config_postgres.toml")),
    seed = seeds::connector_stream
)]
async fn query_timeout_cancels_slow_statement(
    harness: &TestHarness,
    _fixture: JdbcQueryTimeoutFixture,
) {
    assert_source_running(harness).await;
    // Without Statement.setQueryTimeout the two-second query emits a row in
    // this window. A one-second timeout must cancel every retry before emission.
    assert_no_messages_for(harness, "jdbc_query_timeout", Duration::from_secs(3)).await;
}

async fn poll_json_messages(
    client: &IggyClient,
    consumer_name: &str,
    expected_count: usize,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let consumer = consumer(consumer_name);
    let mut received = Vec::new();
    loop {
        let polled = poll(client, &consumer).await;
        received.extend(
            polled
                .messages
                .iter()
                .filter_map(|message| serde_json::from_slice(&message.payload).ok()),
        );
        if received.len() >= expected_count || Instant::now() >= deadline {
            return received;
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn poll_until_ids_seen(
    client: &IggyClient,
    consumer: &Consumer,
    expected: &[i64],
    timeout: Duration,
) -> (Vec<i64>, usize) {
    let deadline = Instant::now() + timeout;
    let mut seen = BTreeSet::new();
    let mut received = 0;
    loop {
        let polled = poll(client, consumer).await;
        for message in &polled.messages {
            received += 1;
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&message.payload)
                && let Some(id) = value
                    .get("data")
                    .and_then(|data| data.get("id"))
                    .and_then(|id| id.as_i64())
            {
                seen.insert(id);
            }
        }
        if expected.iter().all(|id| seen.contains(id)) || Instant::now() >= deadline {
            return (seen.into_iter().collect(), received);
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn assert_no_messages_for(harness: &TestHarness, consumer_name: &str, duration: Duration) {
    let client = harness.root_client().await.expect("root client");
    let consumer = consumer(consumer_name);
    let deadline = Instant::now() + duration;
    loop {
        let polled = poll(&client, &consumer).await;
        assert!(
            polled.messages.is_empty(),
            "expected the source to fail closed, got {} messages",
            polled.messages.len()
        );
        if Instant::now() >= deadline {
            return;
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn poll(client: &IggyClient, consumer: &Consumer) -> iggy_common::PolledMessages {
    let stream: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic: Identifier = seeds::names::TOPIC.try_into().unwrap();
    client
        .poll_messages(
            &stream,
            &topic,
            None,
            consumer,
            &PollingStrategy::next(),
            POLL_BATCH,
            true,
        )
        .await
        .expect("poll JDBC messages")
}

fn consumer(name: &str) -> Consumer {
    Consumer::new(name.try_into().expect("valid consumer name"))
}

async fn assert_source_running(harness: &TestHarness) {
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let sources: serde_json::Value = reqwest::get(format!("{api_url}/sources"))
        .await
        .expect("query source status")
        .error_for_status()
        .expect("source status response")
        .json()
        .await
        .expect("deserialize source status");
    assert_eq!(sources[0]["status"], "running", "source status: {sources}");
}
