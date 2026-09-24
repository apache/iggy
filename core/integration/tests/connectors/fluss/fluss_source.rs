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

use super::{API_KEY, POLL_ATTEMPTS, POLL_INTERVAL_MS, SOURCE_KEY, STATE_FILE, TEST_ROW_COUNT};
use crate::connectors::fixtures::{
    FlussSourceAllTypesFixture, FlussSourceFixture, FlussSourceLatestFixture,
    FlussSourceSlowPollFixture,
};
use iggy::prelude::IggyClient;
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats, ConnectorStatus};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Redelivery waits out the slow poll interval, the rejected batch and the Apache Iggy restart.
const REDELIVERY_ATTEMPTS: usize = POLL_ATTEMPTS * 3;
const SEND_FAILURE_TIMEOUT: Duration = Duration::from_secs(30);
const STATE_FILE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Deserialize)]
struct FlussRecord {
    id: i32,
    payload: String,
    #[serde(rename = "_fluss_bucket")]
    bucket: i32,
    #[serde(rename = "_fluss_offset")]
    offset: i64,
    #[serde(rename = "_fluss_timestamp")]
    timestamp: i64,
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn log_table_rows_are_produced_to_iggy(harness: &TestHarness, fixture: FlussSourceFixture) {
    let client = harness.root_client().await.unwrap();

    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let received = poll_records(&client, TEST_ROW_COUNT, POLL_ATTEMPTS).await;

    assert!(
        received.len() >= TEST_ROW_COUNT,
        "Expected at least {TEST_ROW_COUNT} messages, got {}",
        received.len()
    );

    for (index, record) in received.iter().take(TEST_ROW_COUNT).enumerate() {
        assert_eq!(record.id, index as i32, "Column `id` mismatch at {index}");
        assert_eq!(
            record.payload, payloads[index],
            "Column `payload` mismatch at {index}"
        );
        assert_eq!(record.bucket, 0, "Bucket mismatch at {index}");
        assert_eq!(
            record.offset, index as i64,
            "Fluss offset should be preserved and sequential at {index}"
        );
        assert!(
            record.timestamp > 0,
            "Fluss timestamp should be populated at {index}"
        );
    }
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_rejected_batch_when_iggy_restarts_should_replay_rows_from_fluss(
    harness: &mut TestHarness,
    fixture: FlussSourceSlowPollFixture,
) {
    // Without reconnection retries a send to a stopped server fails at once, so the runtime
    // rejects the batch instead of blocking on it.
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    let errors_before_failure = source_stats(&http, &api_url)
        .await
        .expect("Apache Fluss source stats should be present")
        .errors;

    harness.kill_node(0).expect("Failed to kill Iggy server");
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    wait_for_source_errors(&http, &api_url, errors_before_failure + 1).await;

    harness
        .restart_node(0)
        .expect("Failed to restart the Iggy server");

    // The scanner handed these rows over before the batch was rejected, so they only arrive if
    // the source rewound it instead of moving on.
    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, payloads.len(), REDELIVERY_ATTEMPTS).await;
    assert_rows_delivered(&received, &payloads, 0);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_latest_start_when_runtime_restarts_before_any_row_should_deliver_rows_written_meanwhile(
    harness: &mut TestHarness,
    fixture: FlussSourceLatestFixture,
) {
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE);
    // No row has arrived yet, so only the tail offsets resolved for `latest` can have written it.
    wait_for_file(&state_path).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, payloads.len(), POLL_ATTEMPTS).await;
    assert!(
        received
            .iter()
            .all(|record| !fixture.existing_payloads().contains(&record.payload)),
        "Rows written before the first start should be skipped by `latest`"
    );
    assert_rows_delivered(
        &received,
        &payloads,
        fixture.existing_payloads().len() as i64,
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_every_supported_column_type_should_map_each_to_json(
    harness: &TestHarness,
    fixture: FlussSourceAllTypesFixture,
) {
    fixture
        .append_sample_rows()
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = poll_payloads(&client, 2, POLL_ATTEMPTS).await;
    let row_at = |offset: i64| {
        received
            .iter()
            .find(|payload| payload["_fluss_offset"] == offset)
            .unwrap_or_else(|| panic!("Row at Fluss offset {offset} was never delivered"))
    };
    let (filled, empty) = (row_at(0), row_at(1));

    let expected = json!({
        "bool_col": true,
        "tinyint_col": 7,
        "smallint_col": 300,
        "int_col": 70_000,
        "bigint_col": 9_000_000_000i64,
        "float_col": 1.5,
        "double_col": 2.25,
        "char_col": "abcde",
        "string_col": "hello",
        "decimal_col": "123.45",
        "date_col": "2024-02-29",
        "time_col": "12:34:56.789",
        "timestamp_col": "2023-11-14 22:13:20.123456",
        "timestamp_nanos_col": "2023-11-14 22:13:20.123456789",
        "timestamp_ltz_col": "2023-11-14T22:13:20.123456+00:00",
        "bytes_col": "AQID",
        "binary_col": "BAUG",
    });
    for (column, value) in expected
        .as_object()
        .expect("Expected row should be an object")
    {
        assert_eq!(
            &filled[column], value,
            "Column `{column}` did not map as expected"
        );
        assert_eq!(
            empty[column],
            Value::Null,
            "Column `{column}` should be null in the empty row"
        );
    }
}

fn test_payloads() -> Vec<String> {
    (0..TEST_ROW_COUNT)
        .map(|index| format!("fluss-payload-{index}"))
        .collect()
}

/// At-least-once delivery may repeat a row, so this checks that every row arrived, not how
/// many times.
fn assert_rows_delivered(received: &[FlussRecord], payloads: &[String], first_offset: i64) {
    for (index, payload) in payloads.iter().enumerate() {
        let offset = first_offset + index as i64;
        assert!(
            received
                .iter()
                .any(|record| record.offset == offset && &record.payload == payload),
            "Row at Fluss offset {offset} ({payload}) was never delivered, got {} messages",
            received.len()
        );
    }
}

async fn poll_records(
    client: &IggyClient,
    expected_rows: usize,
    attempts: usize,
) -> Vec<FlussRecord> {
    poll_payloads(client, expected_rows, attempts)
        .await
        .into_iter()
        .filter_map(|payload| serde_json::from_value(payload).ok())
        .collect()
}

/// Polls until messages for `expected_rows` distinct Fluss offsets have arrived, keeping
/// any repeats an at-least-once redelivery produced.
async fn poll_payloads(client: &IggyClient, expected_rows: usize, attempts: usize) -> Vec<Value> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "fluss_test_consumer".try_into().unwrap();

    let mut received: Vec<Value> = Vec::new();
    for _ in 0..attempts {
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
            for message in polled.messages {
                if let Ok(payload) = serde_json::from_slice(&message.payload) {
                    received.push(payload);
                }
            }
            let distinct_rows: HashSet<i64> = received
                .iter()
                .filter_map(|payload| payload["_fluss_offset"].as_i64())
                .collect();
            if distinct_rows.len() >= expected_rows {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}

async fn source_stats(http: &Client, api_url: &str) -> Option<ConnectorStats> {
    let response = http
        .get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .ok()?;
    let stats = response.json::<ConnectorRuntimeStats>().await.ok()?;
    stats
        .connectors
        .into_iter()
        .find(|connector| connector.key == SOURCE_KEY)
}

async fn wait_for_source_errors(http: &Client, api_url: &str, minimum_errors: u64) {
    timeout(SEND_FAILURE_TIMEOUT, async {
        loop {
            if let Some(source) = source_stats(http, api_url).await
                && source.status == ConnectorStatus::Error
                && source.errors >= minimum_errors
            {
                return;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("Apache Fluss source did not report the rejected batch");
}

async fn wait_for_file(path: &Path) {
    timeout(STATE_FILE_TIMEOUT, async {
        while !path.exists() {
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("Apache Fluss source did not persist its start offsets");
}
