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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_ROW_COUNT};
use crate::connectors::fixtures::FlussSourceFixture;
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;

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

    let payloads: Vec<String> = (0..TEST_ROW_COUNT)
        .map(|index| format!("fluss-payload-{index}"))
        .collect();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let received = poll_records(&client).await;

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

async fn poll_records(client: &iggy::prelude::IggyClient) -> Vec<FlussRecord> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "fluss_test_consumer".try_into().unwrap();

    let mut received: Vec<FlussRecord> = Vec::new();
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
            for message in polled.messages {
                if let Ok(record) = serde_json::from_slice(&message.payload) {
                    received.push(record);
                }
            }
            if received.len() >= TEST_ROW_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}
