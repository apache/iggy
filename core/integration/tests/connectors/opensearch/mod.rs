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

mod opensearch_source;
mod opensearch_source_resilience;
mod opensearch_source_types;

use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::{
    CommitPolicy, Consumer, Identifier, IggyMessage, MessageClient, PollOptions, PollingStrategy,
};
use integration::harness::{TestHarness, seeds};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

const TEST_MESSAGE_COUNT: usize = 3;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const NEXT_POLL_COUNT: u32 = 10;
const FULL_READ_POLL_COUNT: u32 = 100;
const CHECKPOINT_FILE: &str = "source_opensearch.state";
const CHECKPOINT_FAILURE_LOG: &str = "Failed to save state for source connector";

async fn wait_for_failed_checkpoint(harness: &TestHarness) -> bool {
    let runtime = harness.connectors_runtime().expect("connector runtime");
    for _ in 0..POLL_ATTEMPTS {
        if runtime.collect_logs().0.contains(CHECKPOINT_FAILURE_LOG) {
            return true;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    false
}

/// Reads new messages with a committed consumer until `min_messages` have arrived.
async fn poll_next(
    client: &impl MessageClient,
    consumer: &str,
    min_messages: usize,
) -> Vec<IggyMessage> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = consumer.try_into().unwrap();
    let mut received = Vec::new();

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                DEFAULT_PARTITION_ID,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                PollOptions {
                    count: NEXT_POLL_COUNT,
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await
        {
            received.extend(polled.messages);
            if received.len() >= min_messages {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    received
}

/// Reads everything on the topic from offset zero, so duplicates stay visible.
async fn read_from_start(
    client: &impl MessageClient,
    consumer: &str,
    min_messages: usize,
) -> Vec<IggyMessage> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = consumer.try_into().unwrap();
    let mut received = Vec::new();

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                DEFAULT_PARTITION_ID,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::offset(0),
                PollOptions {
                    count: FULL_READ_POLL_COUNT,
                    ..PollOptions::default()
                },
            )
            .await
        {
            received = polled.messages;
            if received.len() >= min_messages {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    received
}

fn json_payloads(messages: &[IggyMessage]) -> Vec<Value> {
    messages
        .iter()
        .filter_map(|message| serde_json::from_slice(&message.payload).ok())
        .collect()
}

fn sorted_document_ids(payloads: &[Value]) -> Vec<i64> {
    let mut ids: Vec<i64> = payloads
        .iter()
        .filter_map(|payload| payload.get("id").and_then(Value::as_i64))
        .collect();
    ids.sort_unstable();
    ids
}

fn expected_ids(first: usize, count: usize) -> Vec<i64> {
    (first..first + count).map(|id| id as i64).collect()
}
