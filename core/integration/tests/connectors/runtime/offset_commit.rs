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

//! Offset commit mode tests for sink connectors.
//!
//! The runtime commits consumer offsets in one of two places, selected by the
//! `offset_commit` key on a sink config:
//!   * `after_polling` (default) - the SDK auto-commits when a message is
//!     polled, before the sink has seen it. A sink that rejects the batch still
//!     leaves the offset advanced, so those messages are never redelivered.
//!   * `after_consuming` - auto-commit is disabled and the runtime stores the
//!     offset only once the sink accepts the batch.
//!
//! Each test drives `test_sink`, a fixture plugin whose `fail_after_batches`
//! config decides whether it accepts or rejects batches, then reads the stored
//! consumer group offset back from the server.

use iggy::prelude::{
    Consumer, ConsumerOffsetClient, Identifier, IggyMessage, MessageClient, Partitioning,
};
use iggy_connector_sdk::api::{ConnectorStatus, SinkInfoResponse};
use integration::harness::seeds;
use integration::harness::{TestHarness, seeds::names};
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

const MESSAGE_COUNT: usize = 10;
const SINK_KEY: &str = "offset_commit_sink";
const OFFSET_POLL_ATTEMPTS: u32 = 50;
const OFFSET_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// How long to wait before asserting an offset stayed absent. Long enough that
/// a commit the runtime was going to make would already have landed.
const NO_COMMIT_OBSERVATION_WINDOW: Duration = Duration::from_secs(3);

async fn send_test_messages(harness: &TestHarness) {
    send_test_messages_to(harness, names::TOPIC).await;
}

async fn send_test_messages_to(harness: &TestHarness, topic: &str) {
    let client = harness.root_client().await.expect("failed to build client");
    let stream_id: Identifier = names::STREAM.try_into().unwrap();
    let topic_id: Identifier = topic.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..MESSAGE_COUNT)
        .map(|index| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(format!(r#"{{"index":{index}}}"#).into())
                .build()
                .expect("failed to build message")
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
        .expect("failed to send messages");
}

async fn stored_offset(harness: &TestHarness, consumer_group: &str) -> Option<u64> {
    stored_offset_for(harness, consumer_group, names::TOPIC).await
}

async fn stored_offset_for(
    harness: &TestHarness,
    consumer_group: &str,
    topic: &str,
) -> Option<u64> {
    let client = harness.root_client().await.expect("failed to build client");
    let stream_id: Identifier = names::STREAM.try_into().unwrap();
    let topic_id: Identifier = topic.try_into().unwrap();
    let group_id: Identifier = consumer_group.try_into().unwrap();

    client
        .get_consumer_offset(&Consumer::group(group_id), &stream_id, &topic_id, None)
        .await
        .expect("failed to query consumer offset")
        .map(|info| info.stored_offset)
}

/// Waits for the sink to report `status`, returning the last status seen so a
/// failure reports what the connector actually settled on.
async fn wait_for_sink_status(harness: &TestHarness, status: ConnectorStatus) -> ConnectorStatus {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();
    let mut last = ConnectorStatus::Running;

    for _ in 0..OFFSET_POLL_ATTEMPTS {
        let sinks: Vec<SinkInfoResponse> = http_client
            .get(format!("{api_address}/sinks"))
            .send()
            .await
            .expect("failed to query /sinks")
            .json()
            .await
            .expect("failed to parse sinks");
        last = sinks
            .iter()
            .find(|sink| sink.key == SINK_KEY)
            .expect("sink should be reported")
            .status;
        if last == status {
            return last;
        }
        sleep(OFFSET_POLL_INTERVAL).await;
    }
    last
}

/// Waits for the consumer group offset to reach `expected`, returning the last
/// value seen so a failure reports what the offset actually was.
async fn wait_for_stored_offset(
    harness: &TestHarness,
    consumer_group: &str,
    expected: u64,
) -> Option<u64> {
    let mut last = None;
    for _ in 0..OFFSET_POLL_ATTEMPTS {
        last = stored_offset(harness, consumer_group).await;
        if last == Some(expected) {
            return last;
        }
        sleep(OFFSET_POLL_INTERVAL).await;
    }
    last
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/offset_commit_after_consuming.toml"
    )),
    seed = seeds::connector_stream
)]
async fn given_after_consuming_when_sink_accepts_batch_should_advance_offset(
    harness: &TestHarness,
) {
    send_test_messages(harness).await;

    let last_offset = (MESSAGE_COUNT - 1) as u64;
    let offset =
        wait_for_stored_offset(harness, "offset_commit_after_consuming", last_offset).await;

    assert_eq!(
        offset,
        Some(last_offset),
        "with offset_commit = after_consuming the runtime should store the last consumed offset \
         once the sink accepts the batch"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/offset_commit_after_consuming_failing.toml"
    )),
    seed = seeds::connector_stream
)]
async fn given_after_consuming_when_sink_rejects_batch_should_not_advance_offset(
    harness: &TestHarness,
) {
    send_test_messages(harness).await;

    let status = wait_for_sink_status(harness, ConnectorStatus::Error).await;
    assert_eq!(
        status,
        ConnectorStatus::Error,
        "a rejected batch must stop the sink rather than hand the next batch to the same \
         failing target"
    );

    sleep(NO_COMMIT_OBSERVATION_WINDOW).await;
    let offset = stored_offset(harness, "offset_commit_after_consuming_failing").await;

    assert_eq!(
        offset, None,
        "with offset_commit = after_consuming a rejected batch must leave the offset unstored so \
         the messages are redelivered, but the server reported {offset:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/offset_commit_after_polling_failing.toml"
    )),
    seed = seeds::connector_stream
)]
async fn given_after_polling_when_sink_rejects_batch_should_still_advance_offset(
    harness: &TestHarness,
) {
    send_test_messages(harness).await;

    let last_offset = (MESSAGE_COUNT - 1) as u64;
    let offset =
        wait_for_stored_offset(harness, "offset_commit_after_polling_failing", last_offset).await;

    assert_eq!(
        offset,
        Some(last_offset),
        "the default after_polling mode commits at poll time, so a rejected batch still advances \
         the offset and those messages are lost - this is the at-most-once behaviour that \
         after_consuming exists to avoid"
    );

    let status = wait_for_sink_status(harness, ConnectorStatus::Error).await;
    assert_eq!(
        status,
        ConnectorStatus::Error,
        "a rejection stops the sink in both modes - continuing would commit every later batch at \
         poll time and drain the topic into a target that is already refusing writes"
    );
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/runtime/offset_commit_multi_topic.toml"
    )),
    seed = seeds::connector_multi_topic_stream
)]
async fn given_multi_topic_sink_when_one_topic_rejects_should_stop_every_topic(
    harness: &TestHarness,
) {
    send_test_messages_to(harness, names::TOPIC).await;

    let status = wait_for_sink_status(harness, ConnectorStatus::Error).await;
    assert_eq!(
        status,
        ConnectorStatus::Error,
        "the rejecting topic should drive the connector to Error"
    );

    // The sink is configured to reject only TOPIC, so TOPIC_2 would be accepted
    // and its offset committed if its task were still alive. Sending after the
    // halt makes a committed offset here mean exactly one thing: the sibling
    // outlived the failure. Both topics share one plugin instance and so one
    // target, and the instance is the failure domain, not the task.
    send_test_messages_to(harness, names::TOPIC_2).await;
    sleep(NO_COMMIT_OBSERVATION_WINDOW).await;

    let offset = stored_offset_for(harness, "offset_commit_multi_topic", names::TOPIC_2).await;
    assert_eq!(
        offset, None,
        "the sibling topic's task must stop with the instance, but it consumed a batch the sink \
         would have accepted and committed {offset:?}"
    );
}
