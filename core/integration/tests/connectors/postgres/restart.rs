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
use crate::connectors::fixtures::{PostgresOps, PostgresSinkFixture};
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use iggy_connector_sdk::api::{ConnectorStatus, SinkInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

const API_KEY: &str = "test-api-key";
const SINK_TABLE: &str = "iggy_messages";
const SINK_KEY: &str = "postgres";

type SinkRow = (i64, String, String, Vec<u8>);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn restart_sink_connector_continues_processing(
    harness: &TestHarness,
    fixture: PostgresSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http = Client::new();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    fixture.wait_for_table(&pool, SINK_TABLE).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    wait_for_sink_status(&http, &api_url, ConnectorStatus::Running).await;

    let first_batch = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages = build_messages(&first_batch, 0);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send first batch");

    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {SINK_TABLE} ORDER BY iggy_offset"
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch first batch rows");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows before restart"
    );

    let resp = http
        .post(format!("{api_url}/sinks/{SINK_KEY}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to call restart endpoint");

    assert_eq!(
        resp.status().as_u16(),
        204,
        "Restart endpoint should return 204 No Content"
    );

    wait_for_sink_status(&http, &api_url, ConnectorStatus::Running).await;

    let second_batch = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages = build_messages(&second_batch, TEST_MESSAGE_COUNT);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send second batch");

    let total_expected = TEST_MESSAGE_COUNT * 2;
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, total_expected)
        .await
        .expect("Failed to fetch rows after restart");

    assert!(
        rows.len() >= total_expected,
        "Expected at least {total_expected} rows after restart, got {}",
        rows.len()
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn parallel_restart_requests_should_not_break_connector(
    harness: &TestHarness,
    fixture: PostgresSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http = Client::new();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    fixture.wait_for_table(&pool, SINK_TABLE).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    wait_for_sink_status(&http, &api_url, ConnectorStatus::Running).await;

    let mut tasks = Vec::new();
    for _ in 0..5 {
        let http = http.clone();
        let url = format!("{api_url}/sinks/{SINK_KEY}/restart");
        tasks.push(tokio::spawn(async move {
            http.post(&url)
                .header("api-key", API_KEY)
                .send()
                .await
                .expect("Failed to call restart endpoint")
        }));
    }

    let responses = futures::future::join_all(tasks).await;
    for resp in responses {
        let resp = resp.expect("Task panicked");
        assert_eq!(
            resp.status().as_u16(),
            204,
            "All restart requests should return 204"
        );
    }

    wait_for_sink_status(&http, &api_url, ConnectorStatus::Running).await;

    let batch = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages = build_messages(&batch, 0);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages after parallel restarts");

    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {SINK_TABLE} ORDER BY iggy_offset"
    );
    let rows: Vec<SinkRow> = fixture
        .fetch_rows_as(&pool, &query, TEST_MESSAGE_COUNT)
        .await
        .expect("Failed to fetch rows after parallel restarts");

    assert!(
        rows.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} rows after parallel restarts, got {}",
        rows.len()
    );
}

async fn wait_for_sink_status(
    http: &Client,
    api_url: &str,
    expected: ConnectorStatus,
) -> SinkInfoResponse {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(resp) = http
            .get(format!("{api_url}/sinks/{SINK_KEY}"))
            .header("api-key", API_KEY)
            .send()
            .await
            && let Ok(info) = resp.json::<SinkInfoResponse>().await
            && info.status == expected
        {
            return info;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!("Sink connector did not reach {expected:?} status in time");
}

fn build_messages(messages_data: &[TestMessage], id_offset: usize) -> Vec<IggyMessage> {
    messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((id_offset + i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
        })
        .collect()
}
