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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS};
use crate::connectors::fixtures::{
    OpenSearchSourceCircuitBreakerFixture, OpenSearchSourceTransientErrorFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::ConnectorStatus;
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

async fn poll_json_messages(
    client: &impl MessageClient,
    consumer_id: &str,
    min_messages: usize,
) -> Vec<serde_json::Value> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer: Identifier = consumer_id.try_into().unwrap();
    let mut received = Vec::new();

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer.clone()),
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
            if received.len() >= min_messages {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    received
}

async fn wait_for_source_status(
    http_client: &Client,
    api_address: &str,
    expected: ConnectorStatus,
) -> ConnectorStatus {
    let mut status = ConnectorStatus::Starting;
    for _ in 0..POLL_ATTEMPTS {
        let response = http_client
            .get(format!("{api_address}/sources"))
            .send()
            .await
            .expect("Failed to query /sources");
        assert_eq!(response.status(), 200);
        let sources: Vec<iggy_connector_sdk::api::SourceInfoResponse> =
            response.json().await.expect("Failed to parse sources");
        if let Some(source) = sources.first() {
            status = source.status;
            if status == expected {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    status
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source_resilience.toml")),
    seed = seeds::connector_stream
)]
async fn given_transient_search_errors_when_connector_polls_should_retry_and_produce(
    harness: &TestHarness,
    fixture: OpenSearchSourceTransientErrorFixture,
) {
    let client = harness.root_client().await.unwrap();
    let received = poll_json_messages(&client, "resilience_retry_consumer", 1).await;

    assert_eq!(
        received.len(),
        1,
        "expected one message after transient search errors were retried"
    );
    assert_eq!(
        received[0].get("id").and_then(|value| value.as_i64()),
        Some(42)
    );
    assert!(
        fixture.search_request_count().await >= 3,
        "search should be retried after transient 503 responses"
    );

    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let status =
        wait_for_source_status(&Client::new(), &api_address, ConnectorStatus::Running).await;
    assert_eq!(status, ConnectorStatus::Running);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source_resilience.toml")),
    seed = seeds::connector_stream
)]
async fn given_persistent_search_errors_when_connector_polls_should_remain_running_without_messages(
    harness: &TestHarness,
    fixture: OpenSearchSourceCircuitBreakerFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    sleep(Duration::from_millis(POLL_INTERVAL_MS * 20)).await;

    let status = wait_for_source_status(&http_client, &api_address, ConnectorStatus::Running).await;
    assert_eq!(
        status,
        ConnectorStatus::Running,
        "persistent search failures should not move source to Error"
    );

    let received = poll_json_messages(&client, "resilience_cb_consumer", 1).await;
    assert!(
        received.is_empty(),
        "persistent failures should not produce messages, got {}",
        received.len()
    );

    assert!(
        fixture.search_request_count().await >= 3,
        "connector should retry transient HTTP failures before surfacing poll errors"
    );
}
