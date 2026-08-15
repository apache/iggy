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

use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, HeaderKey, HeaderValue, IggyClient, IggyMessage,
    Partitioning,
};
use iggy_common::{
    Consumer, Identifier, MessageClient, PollingStrategy, StreamClient, TopicClient,
};
use integration::harness::{
    ServerHandle, TestBinary, TestBinaryError, TestContext, TestFixture, TestHarness,
    TestServerConfig, seeds,
};
use integration::iggy_harness;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const UPSTREAM_STREAM: &str = "upstream_stream";
const UPSTREAM_TOPIC: &str = "upstream_topic";
const TEST_MESSAGE_COUNT: usize = 5;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const POLL_BATCH: u32 = 100;
const HEADER_PRODUCER_KEY: &str = "producer";
const HEADER_PRODUCER_VALUE: &str = "integration-test";
const HEADER_SEQ_KEY: &str = "seq";

/// Boots a second `iggy-server` that acts as the upstream cluster the
/// `iggy_source` connector replicates from. The connector config's
/// `connection_string` is injected through the runtime env override.
pub struct IggySourceUpstreamFixture {
    upstream: ServerHandle,
}

#[async_trait]
impl TestFixture for IggySourceUpstreamFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let mut context = TestContext::new(None, true)?;
        context.ensure_created()?;
        let mut upstream = ServerHandle::with_config(
            TestServerConfig::builder()
                .quic_enabled(false)
                .websocket_enabled(false)
                .http_enabled(false)
                .build(),
            Arc::new(context),
        );
        upstream.start()?;
        Ok(Self { upstream })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let tcp_addr = self
            .upstream
            .tcp_addr()
            .expect("upstream server TCP address");
        HashMap::from([
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_CONNECTION_STRING".to_string(),
                format!("iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{tcp_addr}"),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_UPSTREAM_STREAM".to_string(),
                UPSTREAM_STREAM.to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_UPSTREAM_TOPIC".to_string(),
                UPSTREAM_TOPIC.to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_POLL_INTERVAL".to_string(),
                "100ms".to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_INITIAL_OFFSET".to_string(),
                "earliest".to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_STREAMS_0_STREAM".to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_STREAMS_0_TOPIC".to_string(),
                seeds::names::TOPIC.to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_STREAMS_0_SCHEMA".to_string(),
                "raw".to_string(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_IGGY_PATH".to_string(),
                "../../target/debug/libiggy_connector_iggy_source".to_string(),
            ),
        ])
    }
}

impl IggySourceUpstreamFixture {
    pub async fn client(&self) -> Result<IggyClient, TestBinaryError> {
        self.upstream
            .tcp_client()?
            .with_root_login()
            .connect()
            .await
    }

    /// The connector auto-creates the upstream stream/topic in `open()`; wait
    /// until both exist before producing so no message races topic creation.
    pub async fn ensure_upstream_topic(&self, client: &IggyClient) {
        let stream_id = Identifier::named(UPSTREAM_STREAM).expect("valid stream name");
        let topic_id = Identifier::named(UPSTREAM_TOPIC).expect("valid topic name");
        for _ in 0..POLL_ATTEMPTS {
            if client
                .get_stream(&stream_id)
                .await
                .is_ok_and(|stream| stream.is_some())
                && client
                    .get_topic(&stream_id, &topic_id)
                    .await
                    .is_ok_and(|topic| topic.is_some())
            {
                return;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        panic!("Upstream stream/topic were not created within the poll window");
    }

    pub async fn produce_messages(&self, client: &IggyClient, payloads: &[String]) {
        let mut messages = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let mut headers = BTreeMap::new();
                headers.insert(
                    HeaderKey::try_from(HEADER_PRODUCER_KEY).expect("valid header key"),
                    HeaderValue::try_from(HEADER_PRODUCER_VALUE).expect("valid header value"),
                );
                headers.insert(
                    HeaderKey::try_from(HEADER_SEQ_KEY).expect("valid header key"),
                    (i as u64).into(),
                );
                IggyMessage::builder()
                    .id((i as u128) + 1)
                    .payload(Bytes::from(payload.clone()))
                    .user_headers(headers)
                    .build()
                    .expect("Failed to build upstream message")
            })
            .collect::<Vec<_>>();
        client
            .send_messages(
                &Identifier::named(UPSTREAM_STREAM).expect("valid stream name"),
                &Identifier::named(UPSTREAM_TOPIC).expect("valid topic name"),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("Failed to send messages upstream");
    }
}

/// Drains the downstream test topic with a fresh consumer, returning every
/// message exactly once. Waits until at least `min_expected` messages arrive
/// (the connector syncs asynchronously), then ends the drain after two
/// consecutive empty polls, which also covers the connector's in-flight
/// batches.
async fn drain_downstream_topic(
    harness: &TestHarness,
    consumer_name: &str,
    min_expected: usize,
) -> Vec<IggyMessage> {
    let client = harness.root_client().await.expect("root client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("valid stream name");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("valid topic name");
    let consumer = Consumer::new(Identifier::named(consumer_name).expect("valid consumer name"));

    let mut received: Vec<IggyMessage> = Vec::new();
    let mut empty_polls = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                POLL_BATCH,
                true,
            )
            .await
            .expect("Failed to poll downstream topic");
        if polled.messages.is_empty() {
            // Two consecutive empty polls only end the drain once the
            // expected messages have been observed; before that, an empty
            // topic just means the connector has not synced yet.
            if received.len() >= min_expected {
                empty_polls += 1;
                if empty_polls >= 2 {
                    break;
                }
            }
        } else {
            empty_polls = 0;
            received.extend(polled.messages);
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}

fn assert_headers(message: &IggyMessage, expected_seq: u64) {
    let headers = message
        .user_headers_map()
        .expect("Failed to parse user headers")
        .expect("User headers missing");
    assert_eq!(
        headers
            .get(&HeaderKey::try_from(HEADER_PRODUCER_KEY).expect("valid header key"))
            .map(HeaderValue::as_str)
            .and_then(Result::ok),
        Some(HEADER_PRODUCER_VALUE),
        "Producer header mismatch"
    );
    assert_eq!(
        headers
            .get(&HeaderKey::try_from(HEADER_SEQ_KEY).expect("valid header key"))
            .map(HeaderValue::as_uint64)
            .and_then(Result::ok),
        Some(expected_seq),
        "Seq header mismatch"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn iggy_source_replicates_messages_with_headers(
    harness: &TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;

    let payloads: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| format!("upstream-message-{i}"))
        .collect();
    fixture.produce_messages(&upstream_client, &payloads).await;

    let received =
        drain_downstream_topic(harness, "iggy_source_headers_consumer", TEST_MESSAGE_COUNT).await;
    assert_eq!(
        received.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} synced messages, got {}",
        received.len()
    );

    for (i, message) in received.iter().enumerate() {
        assert_eq!(
            String::from_utf8_lossy(&message.payload),
            payloads[i],
            "Payload mismatch at index {i}"
        );
        assert_headers(message, i as u64);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_persists_across_connector_restart(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;

    let first_batch: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| format!("first-batch-{i}"))
        .collect();
    fixture
        .produce_messages(&upstream_client, &first_batch)
        .await;

    let received_before =
        drain_downstream_topic(harness, "iggy_source_restart_consumer", TEST_MESSAGE_COUNT).await;
    assert_eq!(
        received_before.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} messages before restart, got {}",
        received_before.len()
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");

    let second_batch: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| format!("second-batch-{i}"))
        .collect();
    fixture
        .produce_messages(&upstream_client, &second_batch)
        .await;

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    // A fresh consumer drains the whole downstream topic exactly once. If the
    // connector replayed the first batch after restart (stale state) or
    // skipped the second batch (offset jumped too far), the sequence below
    // would differ.
    let received_after = drain_downstream_topic(
        harness,
        "iggy_source_restart_consumer_after",
        TEST_MESSAGE_COUNT * 2,
    )
    .await;
    let expected: Vec<String> = first_batch
        .iter()
        .chain(second_batch.iter())
        .cloned()
        .collect();
    assert_eq!(
        received_after.len(),
        expected.len(),
        "Expected exactly {} messages after restart (no duplicates, no loss), got {}",
        expected.len(),
        received_after.len()
    );
    for (i, message) in received_after.iter().enumerate() {
        assert_eq!(
            String::from_utf8_lossy(&message.payload),
            expected[i],
            "Sequence mismatch at index {i}"
        );
    }
}
