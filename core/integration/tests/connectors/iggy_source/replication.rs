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
    Consumer, Identifier, MessageClient, PartitionClient, PollingStrategy, StreamClient,
    TopicClient,
};
use iggy_connector_sdk::api::ConnectorRuntimeStats;
use integration::harness::{
    ServerHandle, TestBinary, TestBinaryError, TestContext, TestFixture, TestHarness,
    TestServerConfig, seeds,
};
use integration::iggy_harness;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "iggy";
const UPSTREAM_STREAM: &str = "upstream_stream";
const UPSTREAM_TOPIC: &str = "upstream_topic";
const MULTI_PARTITION_COUNT: u32 = 3;
const TEST_MESSAGE_COUNT: usize = 5;
const POLL_ATTEMPTS: usize = 200;
const POLL_INTERVAL_MS: u64 = 50;
const POLL_BATCH: u32 = 100;
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const HEADER_PRODUCER_KEY: &str = "producer";
const HEADER_PRODUCER_VALUE: &str = "integration-test";
const HEADER_SEQ_KEY: &str = "seq";
const MALFORMED_MESSAGE_POLICY_ENV: &str =
    "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_MALFORMED_MESSAGE_POLICY";
const POLL_INTERVAL_ENV: &str = "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_POLL_INTERVAL";
const RETRY_INTERVAL_ENV: &str = "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_RETRY_INTERVAL";
const MAX_RETRY_INTERVAL_ENV: &str = "IGGY_CONNECTORS_SOURCE_IGGY_PLUGIN_CONFIG_MAX_RETRY_INTERVAL";

/// Boots a second `iggy-server` that acts as the upstream cluster the
/// `iggy_source` connector replicates from. The connector config's
/// `connection_string` is injected through the runtime env override.
pub struct IggySourceUpstreamFixture {
    upstream: ServerHandle,
}

#[derive(Debug, Deserialize)]
struct PersistedIggySourceState {
    offsets: HashMap<u32, u64>,
    messages_synced: u64,
    errors_count: u64,
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
        self.produce_messages_to_partition(client, 0, 0, payloads)
            .await;
    }

    pub async fn produce_messages_to_partition(
        &self,
        client: &IggyClient,
        partition_id: u32,
        sequence_start: u64,
        payloads: &[String],
    ) {
        let mut messages = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let sequence = sequence_start + i as u64;
                let mut headers = BTreeMap::new();
                headers.insert(
                    HeaderKey::try_from(HEADER_PRODUCER_KEY).expect("valid header key"),
                    HeaderValue::try_from(HEADER_PRODUCER_VALUE).expect("valid header value"),
                );
                headers.insert(
                    HeaderKey::try_from(HEADER_SEQ_KEY).expect("valid header key"),
                    sequence.into(),
                );
                IggyMessage::builder()
                    .id(u128::from(sequence) + 1)
                    .payload(Bytes::from(payload.clone()))
                    .user_headers(headers)
                    .build()
                    .expect("Failed to build upstream message")
            })
            .collect::<Vec<_>>();
        self.produce_iggy_messages(client, partition_id, &mut messages)
            .await;
    }

    async fn produce_iggy_messages(
        &self,
        client: &IggyClient,
        partition_id: u32,
        messages: &mut [IggyMessage],
    ) {
        client
            .send_messages(
                &Identifier::named(UPSTREAM_STREAM).expect("valid stream name"),
                &Identifier::named(UPSTREAM_TOPIC).expect("valid topic name"),
                &Partitioning::partition_id(partition_id),
                messages,
            )
            .await
            .expect("Failed to send messages upstream");
    }
}

fn message_with_unknown_header_kind(payload: &str) -> IggyMessage {
    let mut user_headers = vec![0xFF];
    user_headers.extend_from_slice(&3u32.to_le_bytes());
    user_headers.extend_from_slice(b"key");
    user_headers.push(0xFF);
    user_headers.extend_from_slice(&5u32.to_le_bytes());
    user_headers.extend_from_slice(b"value");

    let mut message = IggyMessage::builder()
        .payload(Bytes::copy_from_slice(payload.as_bytes()))
        .build()
        .expect("test message should be valid");
    message.header.user_headers_length =
        u32::try_from(user_headers.len()).expect("test user headers should fit in u32");
    message.user_headers = Some(user_headers.into());
    message
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

async fn source_errors(http: &Client, api_url: &str) -> u64 {
    let stats = http
        .get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("runtime stats should be available")
        .json::<ConnectorRuntimeStats>()
        .await
        .expect("runtime stats should be valid");
    stats
        .connectors
        .iter()
        .find(|connector| connector.key == SOURCE_KEY)
        .expect("Iggy source stats should be present")
        .errors
}

async fn wait_for_source_error_after(http: &Client, api_url: &str, previous_errors: u64) {
    timeout(WAIT_TIMEOUT, async {
        loop {
            if let Ok(response) = http
                .get(format!("{api_url}/stats"))
                .header("api-key", API_KEY)
                .send()
                .await
                && let Ok(stats) = response.json::<ConnectorRuntimeStats>().await
                && let Some(source) = stats
                    .connectors
                    .iter()
                    .find(|connector| connector.key == SOURCE_KEY)
                && source.errors > previous_errors
            {
                break;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("Iggy source did not report a downstream send failure");
}

async fn wait_for_source_state(
    harness: &TestHarness,
    expected_offsets: &HashMap<u32, u64>,
    expected_messages_synced: u64,
    minimum_errors: u64,
) -> PersistedIggySourceState {
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join("source_iggy.state");

    timeout(WAIT_TIMEOUT, async {
        loop {
            if let Ok(bytes) = tokio::fs::read(&state_path).await
                && let Ok(state) = rmp_serde::from_slice::<PersistedIggySourceState>(&bytes)
                && state.offsets == *expected_offsets
                && state.messages_synced == expected_messages_synced
                && state.errors_count >= minimum_errors
            {
                break state;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Iggy source state did not reach offsets {expected_offsets:?}, synced count \
             {expected_messages_synced}, and at least {minimum_errors} error(s)"
        )
    })
}

async fn restart_with_upstream_partition_count(
    harness: &mut TestHarness,
    upstream_client: &IggyClient,
    expected_partitions: u32,
) {
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");

    let stream_id = Identifier::named(UPSTREAM_STREAM).expect("valid stream name");
    let topic_id = Identifier::named(UPSTREAM_TOPIC).expect("valid topic name");
    let topic = upstream_client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("Failed to fetch upstream topic")
        .expect("Upstream topic should exist");
    let additional_partitions = expected_partitions.saturating_sub(topic.partitions_count);
    if additional_partitions > 0 {
        upstream_client
            .create_partitions(&stream_id, &topic_id, additional_partitions)
            .await
            .expect("Failed to create upstream partitions");
    }

    let topic = upstream_client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("Failed to fetch upstream topic")
        .expect("Upstream topic should exist");
    assert_eq!(topic.partitions_count, expected_partitions);

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");
}

#[iggy_harness(
    cluster_nodes = 1,
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
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn block_policy_commits_prefix_and_drop_headers_resumes_partition(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;

    let first_payload = vec!["before-malformed-headers".to_string()];
    fixture
        .produce_messages(&upstream_client, &first_payload)
        .await;
    let first_received = drain_downstream_topic(harness, "iggy_source_before_malformed", 1).await;
    assert_eq!(first_received.len(), 1, "Expected the first offset to sync");

    let mut blocked_batch = vec![
        IggyMessage::builder()
            .payload(Bytes::from_static(b"valid-prefix"))
            .build()
            .expect("test message should be valid"),
        message_with_unknown_header_kind("malformed-headers"),
        IggyMessage::builder()
            .payload(Bytes::from_static(b"after-malformed-headers"))
            .build()
            .expect("test message should be valid"),
    ];
    fixture
        .produce_iggy_messages(&upstream_client, 0, &mut blocked_batch)
        .await;

    let blocked_offsets = HashMap::from([(0, 1)]);
    wait_for_source_state(harness, &blocked_offsets, 2, 1).await;

    let received_while_blocked =
        drain_downstream_topic(harness, "iggy_source_while_blocked", 2).await;
    let blocked_payloads = received_while_blocked
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        blocked_payloads,
        ["before-malformed-headers", "valid-prefix"],
        "Only the valid prefix should be committed while the malformed offset is blocked"
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .add_env(MALFORMED_MESSAGE_POLICY_ENV, "drop_headers");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime with drop_headers policy");

    let received = drain_downstream_topic(harness, "iggy_source_after_malformed", 4).await;
    let payloads = received
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        payloads,
        [
            "before-malformed-headers",
            "valid-prefix",
            "malformed-headers",
            "after-malformed-headers",
        ],
        "The malformed offset and its tail should resume without replaying the committed prefix"
    );

    let malformed = received
        .iter()
        .find(|message| message.payload.as_ref() == b"malformed-headers")
        .expect("malformed message should be forwarded");
    assert!(
        malformed
            .user_headers_map()
            .expect("downstream user headers should be valid")
            .is_none(),
        "drop_headers should remove only the unparsable user headers"
    );

    let resumed_offsets = HashMap::from([(0, 3)]);
    wait_for_source_state(harness, &resumed_offsets, 4, 2).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn malformed_user_headers_do_not_back_off_healthy_partitions(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;
    let connectors_runtime = harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime");
    connectors_runtime.add_env(RETRY_INTERVAL_ENV, "30s");
    connectors_runtime.add_env(MAX_RETRY_INTERVAL_ENV, "30s");
    restart_with_upstream_partition_count(harness, &upstream_client, MULTI_PARTITION_COUNT).await;

    fixture
        .produce_messages_to_partition(&upstream_client, 0, 0, &["healthy-partition-0".to_string()])
        .await;
    let mut malformed = [message_with_unknown_header_kind("blocked-partition-1")];
    fixture
        .produce_iggy_messages(&upstream_client, 1, &mut malformed)
        .await;
    fixture
        .produce_messages_to_partition(&upstream_client, 2, 0, &["healthy-partition-2".to_string()])
        .await;

    let expected_offsets = HashMap::from([(0, 0), (2, 0)]);
    wait_for_source_state(harness, &expected_offsets, 2, 1).await;

    fixture
        .produce_messages_to_partition(
            &upstream_client,
            2,
            1,
            &["healthy-after-conversion-error".to_string()],
        )
        .await;

    let expected_offsets = HashMap::from([(0, 0), (2, 1)]);
    wait_for_source_state(harness, &expected_offsets, 3, 2).await;

    let received = drain_downstream_topic(harness, "iggy_source_partition_isolation", 3).await;
    let mut payloads = received
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect::<Vec<_>>();
    payloads.sort();
    assert_eq!(
        payloads,
        [
            "healthy-after-conversion-error",
            "healthy-partition-0",
            "healthy-partition-2",
        ]
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn non_recoverable_poll_error_does_not_skip_remaining_partitions(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;
    let connectors_runtime = harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime");
    connectors_runtime.add_env(POLL_INTERVAL_ENV, "5s");
    connectors_runtime.add_env(RETRY_INTERVAL_ENV, "30s");
    connectors_runtime.add_env(MAX_RETRY_INTERVAL_ENV, "30s");
    restart_with_upstream_partition_count(harness, &upstream_client, MULTI_PARTITION_COUNT).await;

    fixture
        .produce_messages_to_partition(&upstream_client, 0, 0, &["before-topic-delete".to_string()])
        .await;
    let initial_offsets = HashMap::from([(0, 0)]);
    let initial_state = wait_for_source_state(harness, &initial_offsets, 1, 0).await;
    assert_eq!(initial_state.errors_count, 0);

    upstream_client
        .delete_topic(
            &Identifier::named(UPSTREAM_STREAM).expect("valid stream name"),
            &Identifier::named(UPSTREAM_TOPIC).expect("valid topic name"),
        )
        .await
        .expect("Failed to delete upstream topic");

    let failed_state = wait_for_source_state(harness, &initial_offsets, 1, 1).await;
    assert_eq!(
        failed_state.errors_count,
        u64::from(MULTI_PARTITION_COUNT),
        "Every discovered partition should be attempted before connector-wide backoff"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn multiple_upstream_partitions_resume_from_independent_offsets(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;
    restart_with_upstream_partition_count(harness, &upstream_client, MULTI_PARTITION_COUNT).await;

    let partition_payloads = [
        vec!["partition-0-message-0".to_string()],
        vec![
            "partition-1-message-0".to_string(),
            "partition-1-message-1".to_string(),
        ],
        vec![
            "partition-2-message-0".to_string(),
            "partition-2-message-1".to_string(),
            "partition-2-message-2".to_string(),
        ],
    ];
    for (partition_id, payloads) in partition_payloads.iter().enumerate() {
        fixture
            .produce_messages_to_partition(
                &upstream_client,
                partition_id as u32,
                (partition_id as u64) * 100,
                payloads,
            )
            .await;
    }

    let initial_message_count = partition_payloads.iter().map(Vec::len).sum::<usize>();
    let initial_received = drain_downstream_topic(
        harness,
        "iggy_source_multiple_partitions_before_restart",
        initial_message_count,
    )
    .await;
    let mut initial_actual = initial_received
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect::<Vec<_>>();
    let mut initial_expected = partition_payloads
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    initial_actual.sort();
    initial_expected.sort();
    assert_eq!(initial_actual, initial_expected);

    let initial_offsets = HashMap::from([(0, 0), (1, 1), (2, 2)]);
    wait_for_source_state(harness, &initial_offsets, initial_message_count as u64, 0).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let resumed_payloads = [
        "partition-0-after-restart".to_string(),
        "partition-1-after-restart".to_string(),
        "partition-2-after-restart".to_string(),
    ];
    for (partition_id, payload) in resumed_payloads.iter().enumerate() {
        fixture
            .produce_messages_to_partition(
                &upstream_client,
                partition_id as u32,
                1_000 + partition_id as u64,
                std::slice::from_ref(payload),
            )
            .await;
    }
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let total_message_count = initial_message_count + resumed_payloads.len();
    let received_after_restart = drain_downstream_topic(
        harness,
        "iggy_source_multiple_partitions_after_restart",
        total_message_count,
    )
    .await;
    let mut actual_after_restart = received_after_restart
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect::<Vec<_>>();
    let mut expected_after_restart = initial_expected;
    expected_after_restart.extend(resumed_payloads);
    actual_after_restart.sort();
    expected_after_restart.sort();
    assert_eq!(actual_after_restart, expected_after_restart);

    let resumed_offsets = HashMap::from([(0, 1), (1, 2), (2, 3)]);
    wait_for_source_state(harness, &resumed_offsets, total_message_count as u64, 0).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/iggy_source/source.toml")),
    seed = seeds::connector_stream
)]
async fn downstream_outage_replays_nacked_batch_after_recovery(
    harness: &mut TestHarness,
    fixture: IggySourceUpstreamFixture,
) {
    let upstream_client = fixture.client().await.expect("upstream client");
    fixture.ensure_upstream_topic(&upstream_client).await;

    let downstream_address = harness
        .server()
        .tcp_addr()
        .expect("downstream server TCP address");
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .add_env(
            "IGGY_CONNECTORS_IGGY_ADDRESS",
            format!("{downstream_address}?reconnection_retries=1&reconnection_interval=100ms"),
        );
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime with finite reconnection retries");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    let errors_before_outage = source_errors(&http, &api_url).await;

    harness
        .server_mut()
        .stop()
        .expect("Failed to stop downstream server");

    let payloads: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| format!("outage-message-{i}"))
        .collect();
    fixture.produce_messages(&upstream_client, &payloads).await;
    wait_for_source_error_after(&http, &api_url, errors_before_outage).await;

    harness
        .server_mut()
        .start()
        .expect("Failed to restart downstream server");

    let received =
        drain_downstream_topic(harness, "iggy_source_outage_consumer", TEST_MESSAGE_COUNT).await;
    assert_eq!(
        received.len(),
        TEST_MESSAGE_COUNT,
        "Expected the NACKed batch to be replayed after downstream recovery"
    );
    for (index, message) in received.iter().enumerate() {
        assert_eq!(
            String::from_utf8_lossy(&message.payload),
            payloads[index],
            "Payload mismatch at index {index}"
        );
    }
}

#[iggy_harness(
    cluster_nodes = 1,
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
