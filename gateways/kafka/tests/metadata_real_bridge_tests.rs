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

//! Wire-level `Metadata` tests against a real `iggy-server` process, through
//! [`metadata::handle`] with a connected [`GatewayState`]. See
//! `create_topics_real_bridge_tests.rs` for why requests/responses are hand-built here rather
//! than through `kafka_protocol`'s own `Encodable`/`Decodable` (this crate builds
//! `broker`-feature-only: request `Decodable` and response `Encodable`, not the reverse).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serial_test::serial;

use iggy_gateway_kafka::bridge::{IggyBridge, TopicMapping, TopicOverride};
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::metadata;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::TestServer;

const REQUEST_VERSION: i16 = 9;
const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Builds a v9 flexible `Metadata` request. `topics: None` means "all topics" (the null-array
/// sentinel); `Some(names)` requests exactly those topics.
fn build_request(topics: Option<&[&str]>) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    match topics {
        None => enc.write_varint(0), // null compact array: all topics
        Some(names) => {
            enc.write_varint((names.len() + 1) as u64);
            for name in names {
                enc.write_compact_nullable_string(Some(name));
                enc.write_empty_tagged_fields(); // per-topic tagged fields
            }
        }
    }
    enc.write_bool(false); // allow_auto_topic_creation
    enc.write_bool(false); // include_cluster_authorized_operations
    enc.write_bool(false); // include_topic_authorized_operations
    enc.write_empty_tagged_fields(); // top-level tagged fields
    enc.freeze()
}

/// One decoded `MetadataResponseTopic` entry: `(name, error_code, partitions_count)`.
/// One decoded `MetadataResponsePartition`: `(leader_id, replica_nodes, isr_nodes)`. Asserted on
/// directly, not just counted - a regression emitting `leader_id = -1` or empty `replica_nodes`
/// would otherwise pass a suite that only checks partition *count*, while making the topic look
/// leaderless to a real client (that client picks its produce/fetch broker from exactly these
/// fields).
type PartitionEntry = (i32, Vec<i32>, Vec<i32>);
type TopicEntry = (Option<String>, i16, Vec<PartitionEntry>);

/// Decodes a v9 flexible `Metadata` response into every topic entry, in wire order.
///
/// Assumes every partition entry has zero offline replicas - true for every response
/// `metadata::handle`'s real path can produce (nothing here ever reports an offline broker).
fn decode_topics(body: Bytes) -> Vec<TopicEntry> {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");

    let brokers_plus_one = d.read_varint().expect("brokers array count");
    for _ in 1..brokers_plus_one {
        let _node_id = d.read_i32().expect("node_id");
        let _host = d.read_compact_nullable_string().expect("host");
        let _port = d.read_i32().expect("port");
        let _rack = d.read_compact_nullable_string().expect("rack");
        let _tagged = d.read_varint().expect("broker tagged fields");
    }

    let _cluster_id = d.read_compact_nullable_string().expect("cluster_id");
    let _controller_id = d.read_i32().expect("controller_id");

    let topics_plus_one = d.read_varint().expect("topics array count");
    let mut topics = Vec::new();
    for _ in 1..topics_plus_one {
        let error_code = d.read_i16().expect("topic error_code");
        let name = d.read_compact_nullable_string().expect("topic name");
        let _is_internal = d.read_bool().expect("is_internal");

        let partitions_plus_one = d.read_varint().expect("partitions array count");
        let mut partitions = Vec::new();
        for _ in 1..partitions_plus_one {
            let _error_code = d.read_i16().expect("partition error_code");
            let _partition_index = d.read_i32().expect("partition_index");
            let leader_id = d.read_i32().expect("leader_id");
            let _leader_epoch = d.read_i32().expect("leader_epoch");
            let replica_plus_one = d.read_varint().expect("replica_nodes count");
            let mut replica_nodes = Vec::new();
            for _ in 1..replica_plus_one {
                replica_nodes.push(d.read_i32().expect("replica node id"));
            }
            let isr_plus_one = d.read_varint().expect("isr_nodes count");
            let mut isr_nodes = Vec::new();
            for _ in 1..isr_plus_one {
                isr_nodes.push(d.read_i32().expect("isr node id"));
            }
            let offline_plus_one = d.read_varint().expect("offline_replicas count");
            for _ in 1..offline_plus_one {
                let _offline = d.read_i32().expect("offline replica node id");
            }
            let _tagged = d.read_varint().expect("partition tagged fields");
            partitions.push((leader_id, replica_nodes, isr_nodes));
        }

        let _topic_authorized_operations = d.read_i32().expect("topic_authorized_operations");
        let _tagged = d.read_varint().expect("topic tagged fields");

        topics.push((name, error_code, partitions));
    }

    topics
}

/// `count` partitions, each with this gateway's single broker (node id 1) as leader, sole
/// replica, and sole ISR member - the only shape `metadata::handle`'s real path ever produces.
fn expected_partitions(count: u32) -> Vec<PartitionEntry> {
    (0..count).map(|_| (1, vec![1], vec![1])).collect()
}

async fn send(state: &GatewayState, topics: Option<&[&str]>) -> Vec<TopicEntry> {
    let body = build_request(topics);
    let outcome = metadata::handle(state, REQUEST_VERSION, body).await;
    let resp_body = outcome.expect_response("Metadata request always answers");
    decode_topics(resp_body)
}

async fn connected_state(server: &TestServer) -> (GatewayState, IggyBridge) {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    let seed = IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect to a ready server");
    let state = GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
    );
    (state, seed)
}

#[tokio::test]
#[serial]
async fn a_named_lookup_of_an_existing_topic_succeeds() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 3)
        .await
        .expect("seed the topic");

    let topics = send(&state, Some(&["orders"])).await;
    assert_eq!(topics.len(), 1);
    assert_eq!(
        topics[0],
        (
            Some("orders".to_string()),
            ERROR_NONE,
            expected_partitions(3)
        )
    );
}

#[tokio::test]
#[serial]
async fn a_named_lookup_of_a_nonexistent_topic_reports_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, _seed) = connected_state(&server).await;

    let topics = send(&state, Some(&["orders"])).await;
    assert_eq!(topics.len(), 1);
    assert_eq!(
        topics[0],
        (
            Some("orders".to_string()),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            Vec::new()
        )
    );
}

#[tokio::test]
#[serial]
async fn a_null_topics_array_lists_every_real_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 3)
        .await
        .expect("seed orders");
    seed.ensure_stream_and_topic("payments", 1)
        .await
        .expect("seed payments");

    let mut topics = send(&state, None).await;
    topics.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        topics,
        vec![
            (
                Some("orders".to_string()),
                ERROR_NONE,
                expected_partitions(3)
            ),
            (
                Some("payments".to_string()),
                ERROR_NONE,
                expected_partitions(1)
            ),
        ]
    );
}

#[tokio::test]
#[serial]
async fn a_null_topics_array_against_an_empty_bridge_lists_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, _seed) = connected_state(&server).await;

    let topics = send(&state, None).await;
    assert!(topics.is_empty());
}

#[tokio::test]
#[serial]
async fn a_named_lookup_reports_the_kafka_side_name_through_a_topic_mapping_override() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let mut config = server.test_config();
    let mut overrides = HashMap::new();
    overrides.insert(
        "orders".to_string(),
        TopicOverride {
            stream: "billing".to_string(),
            topic: "orders_v2".to_string(),
        },
    );
    let default_stream = config.topic_mapping.default_stream().to_string();
    config.topic_mapping =
        TopicMapping::new(default_stream, overrides).expect("valid mapping for this test");
    let seed = IggyBridge::connect(config.clone())
        .await
        .expect("seed bridge should connect to a ready server");
    seed.ensure_stream_and_topic("orders", 2)
        .await
        .expect("seed the mapped topic");

    let bridge = IggyBridge::connect(config)
        .await
        .expect("bridge should connect to a ready server");
    let state = GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
    );

    let topics = send(&state, Some(&["orders"])).await;
    assert_eq!(topics.len(), 1);
    // The Kafka-side name asked about ("orders"), not the Iggy-side name under the override
    // ("orders_v2") - a Kafka client never heard of "orders_v2" and would be confused by it.
    assert_eq!(
        topics[0],
        (
            Some("orders".to_string()),
            ERROR_NONE,
            expected_partitions(2)
        )
    );
}

/// Regression test: `bounds_guard`'s pre-decode projection cannot see a real topic's partition
/// count, only known after the bridge round trip - a real, oversized-relative-to-`max_frame_size`
/// response must still close the connection rather than build and send it.
#[tokio::test]
#[serial]
async fn a_response_projected_over_max_frame_size_closes_instead_of_answering() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 50)
        .await
        .expect("seed a topic with enough partitions to trip a tiny cap");

    // 50 partitions * 64 bytes/partition (this crate's own conservative per-partition estimate)
    // = 3200 bytes, comfortably over a 512-byte max_frame_size.
    let tiny_state = GatewayState::new(state.broker, state.bridge, 512);
    let body = build_request(Some(&["orders"]));
    let outcome = metadata::handle(&tiny_state, REQUEST_VERSION, body).await;
    assert!(outcome.is_close(), "expected Close, got {outcome:?}");
}

/// Regression test: unlike the named-lookup path above, an all-topics response over budget must
/// be truncated, not closed - its size is the cluster's own catalog, not anything the requesting
/// client chose or can shrink, so closing would make every all-topics call (the bootstrap/refresh
/// shape both librdkafka and the Java client use) fail identically and permanently.
#[tokio::test]
#[serial]
async fn an_all_topics_response_over_max_frame_size_truncates_instead_of_closing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("small", 1)
        .await
        .expect("seed a topic that alone fits any reasonable budget");
    seed.ensure_stream_and_topic("big", 50)
        .await
        .expect("seed a topic that alone exceeds the tiny budget below");

    // 50 partitions * 64 bytes/partition (this crate's own conservative per-partition estimate)
    // = 3200 bytes, comfortably over a 512-byte max_frame_size - so the catalog as a whole cannot
    // fit, but neither topic's own partition count is malformed or attacker-shaped.
    let tiny_state = GatewayState::new(state.broker, state.bridge, 512);
    let topics = send(&tiny_state, None).await;
    assert!(
        topics.len() < 2,
        "expected truncation to drop at least one topic, got {topics:?}"
    );
    let total_partitions: usize = topics
        .iter()
        .map(|(_, _, partitions)| partitions.len())
        .sum();
    assert!(
        total_partitions * 64 <= 512,
        "kept topics must themselves fit the budget, got {total_partitions} partitions"
    );
}

/// Regression test: a topic named more than once in one request must resolve to exactly one
/// response entry, not one per repeat - real Kafka answers a `Metadata` request naming the same
/// topic twice with one entry, and this bridge re-expanding to match the request is what let a
/// handful of repeats of one large topic name amplify a response sized off the repeat count
/// instead of the distinct count.
#[tokio::test]
#[serial]
async fn a_topic_named_twice_in_one_request_resolves_to_one_entry() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, seed) = connected_state(&server).await;
    seed.ensure_stream_and_topic("orders", 2)
        .await
        .expect("seed the topic");

    let topics = send(&state, Some(&["orders", "orders"])).await;
    assert_eq!(topics.len(), 1);
    assert_eq!(
        topics[0],
        (
            Some("orders".to_string()),
            ERROR_NONE,
            expected_partitions(2)
        )
    );
}

/// Regression test: a named-lookup request addressing more than the old per-name bridge-backed
/// topic cap (100) must resolve every name individually, not be rejected wholesale. A hard cap
/// here permanently broke a long-lived Java producer once its `ProducerMetadata`'s cumulative
/// tracked-topic set (resent in full on every refresh) crossed it - every later request answered
/// every topic `INVALID_REQUEST`, with no way for the producer to shrink its own tracked set and
/// recover. `IggyBridge::get_kafka_topics` batches by resolved stream instead of by name, so a
/// name count this large costs the same one-`get_topics`-call-per-stream (here: one, the default
/// stream) it would for any smaller batch.
#[tokio::test]
#[serial]
async fn a_named_lookup_of_more_than_the_old_topic_cap_resolves_every_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (state, _seed) = connected_state(&server).await;

    let names: Vec<String> = (0..101).map(|i| format!("topic-{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let topics = send(&state, Some(&name_refs)).await;
    assert_eq!(topics.len(), 101);
    // None of these topics exist - each is resolved individually and unknown, not blanket
    // rejected as a batch.
    for (_, error_code, _) in &topics {
        assert_eq!(*error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}
